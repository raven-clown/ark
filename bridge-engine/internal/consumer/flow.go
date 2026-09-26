package consumer

import (
	"cmp"
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"time"

	"github.com/expr-lang/expr"
	"github.com/expr-lang/expr/vm"
	"github.com/segmentio/kafka-go"

	"github.com/raven-clown/ark/bridge-engine/internal/callback"
	"github.com/raven-clown/ark/bridge-engine/internal/config"
	"github.com/raven-clown/ark/bridge-engine/internal/datarules"
	"github.com/raven-clown/ark/bridge-engine/internal/events"
	"github.com/raven-clown/ark/bridge-engine/internal/kafkaadmin"
	"github.com/raven-clown/ark/bridge-engine/internal/metrics"
	"github.com/raven-clown/ark/bridge-engine/internal/tap"
)

// flowRuntime is a flow's compiled steps: a caller per call step, the
// programs of every condition and a checker per data check.
type flowRuntime struct {
	flow       *config.Flow
	callers    map[string]*caller
	callerList []*caller
	branches   map[string][]*vm.Program
	checkers   map[string]*datarules.Checker
}

// flowEnv is what a condition can read: the message as it is at that step
// (data), as it was consumed (original), the last call's answer (response),
// why it failed when on a failure path (reason), its key and headers.
var flowEnv = map[string]any{
	"data":     map[string]any{},
	"original": map[string]any{},
	"response": map[string]any{},
	"reason":   "",
	"key":      "",
	"headers":  map[string]string{},
}

func newFlowRuntime(ctx context.Context, deps Deps, p config.Pipeline) (*flowRuntime, error) {
	rt := &flowRuntime{flow: p.Flow, callers: map[string]*caller{}, branches: map[string][]*vm.Program{}, checkers: map[string]*datarules.Checker{}}
	for _, s := range p.Flow.Steps {
		switch s.Type {
		case config.StepCall:
			c := newCaller(s.ID, *s.Target, *s.Retry, *s.CircuitBreaker)
			rt.callers[s.ID] = c
			rt.callerList = append(rt.callerList, c)
		case config.StepCondition:
			for i, b := range s.Branches {
				prog, err := expr.Compile(b.When, expr.Env(flowEnv), expr.AsBool())
				if err != nil {
					return nil, fmt.Errorf("flow step %q branches[%d]: %w", s.ID, i, err)
				}
				rt.branches[s.ID] = append(rt.branches[s.ID], prog)
			}
		case config.StepCheck:
			c, err := datarules.New(*s.Rules)
			if err != nil {
				return nil, fmt.Errorf("flow step %q rules: %w", s.ID, err)
			}
			rt.checkers[s.ID] = c
		case config.StepReject, config.StepDeadLetter:
			if s.Topic != "" {
				if err := kafkaadmin.EnsureTopic(ctx, deps.Brokers, s.Topic, 1, deps.ReplicationFactor); err != nil {
					return nil, fmt.Errorf("flow step %q: ensuring topic %s exists: %w", s.ID, s.Topic, err)
				}
			}
		}
	}
	return rt, nil
}

// flowMsg is the message as it travels a path; each path gets its own copy.
type flowMsg struct {
	key      []byte
	value    []byte
	headers  map[string]string
	original map[string]any
	response map[string]any
	reason   string
}

// flowOutcome sums what happened to one message across every path, so it
// is counted once however many ways it went.
type flowOutcome struct {
	delivered    bool
	rejected     bool
	deadLettered bool
}

func (r *Runner) runFlow(ctx context.Context, msg kafka.Message, headers map[string]string, log *slog.Logger) error {
	original, _ := parseObject(msg.Value)
	m := flowMsg{key: msg.Key, value: msg.Value, headers: headers, original: original}
	var out flowOutcome
	if err := r.runSteps(ctx, r.shared.flow.flow.Start, msg.Partition, m, &out, log); err != nil {
		return err
	}
	switch {
	case out.deadLettered:
		r.counters.deadLettered.Add(1)
		metrics.DeadLettered.WithLabelValues(r.pipeline.Name, r.workerLabel, r.pipeline.Tenant).Inc()
	case out.rejected:
		r.counters.rejected.Add(1)
		metrics.Rejected.WithLabelValues(r.pipeline.Name, r.workerLabel, r.pipeline.Tenant).Inc()
	case out.delivered:
		r.counters.processed.Add(1)
		metrics.Processed.WithLabelValues(r.pipeline.Name, r.workerLabel, r.pipeline.Tenant).Inc()
	}
	return nil
}

func (r *Runner) runSteps(ctx context.Context, ids []string, partition int, m flowMsg, out *flowOutcome, log *slog.Logger) error {
	for _, id := range ids {
		if err := r.runStep(ctx, id, partition, m.clone(), out, log); err != nil {
			return err
		}
	}
	return nil
}

func (r *Runner) runStep(ctx context.Context, id string, partition int, m flowMsg, out *flowOutcome, log *slog.Logger) error {
	rt := r.shared.flow
	s, _ := rt.flow.Step(id)
	log = log.With("step", id)
	correlationID := m.headers[callback.CorrelationIDHeader]

	switch s.Type {
	case config.StepCall:
		res, err := r.call(ctx, rt.callers[id], partition, correlationID, m.key, m.value, log)
		if err != nil {
			return err
		}
		switch {
		case res.rejected:
			m.reason, m.response = res.reason, response(res.resp)
			if len(s.OnReject) == 0 {
				return r.flowReject(ctx, config.Step{ID: id}, m, out, log)
			}
			return r.runSteps(ctx, s.OnReject, partition, m, out, log)
		case res.failed:
			m.reason = res.reason
			if len(s.OnFailure) == 0 {
				if r.shared.dlq == nil {
					return fmt.Errorf("%s (on_exhausted: block keeps retrying it)", res.reason)
				}
				return r.flowDeadLetter(ctx, config.Step{ID: id}, m, out, log)
			}
			return r.runSteps(ctx, s.OnFailure, partition, m, out, log)
		}
		m.response = response(res.resp)
		m.value = res.resp.Body
		return r.runSteps(ctx, s.Next, partition, m, out, log)

	case config.StepCondition:
		env := m.env()
		matched := false
		for i, prog := range rt.branches[id] {
			result, err := expr.Run(prog, env)
			if err != nil {
				log.Error("flow condition evaluation failed", "branch", i, "error", err)
				continue
			}
			if ok, _ := result.(bool); !ok {
				continue
			}
			matched = true
			branch := m
			if branch.reason == "" {
				branch.reason = fmt.Sprintf("condition %q matched %q", id, cmp.Or(s.Branches[i].Name, s.Branches[i].When))
			}
			if err := r.runSteps(ctx, s.Branches[i].Next, partition, branch, out, log); err != nil {
				return err
			}
			if s.Match != "all" {
				break
			}
		}
		if !matched {
			return r.runSteps(ctx, s.Otherwise, partition, m, out, log)
		}
		return nil

	case config.StepCheck:
		c := rt.checkers[id]
		vs := c.Check(m.key, m.headers, m.value)
		if len(vs) == 0 {
			return r.runSteps(ctx, s.Next, partition, m, out, log)
		}
		reason := datarules.Summary(vs)
		for _, v := range vs {
			metrics.DataRuleViolations.WithLabelValues(r.pipeline.Name, v.Rule, r.pipeline.Tenant).Inc()
		}
		events.Record(r.pipeline.Name, events.DataRuleViolation, reason, map[string]string{"correlation_id": correlationID, "step": id})
		if s.Rules.OnViolation == "tag" {
			m.headers[ViolationsHeader] = reason
			return r.runSteps(ctx, s.Next, partition, m, out, log)
		}
		m.reason = reason
		if len(s.OnFail) == 0 {
			return r.flowReject(ctx, config.Step{ID: id}, m, out, log)
		}
		return r.runSteps(ctx, s.OnFail, partition, m, out, log)

	case config.StepTopic:
		if err := r.sendWithRetry(ctx, r.shared.overrideProducer(s.Topic), m.key, m.value, m.headers, log); err != nil {
			return fmt.Errorf("step %s producing to %s: %w", id, s.Topic, err)
		}
		r.tapOut(tap.ToDestination, s.Topic, id, "", statusOf(m.response), m.key, m.value, m.headers)
		out.delivered = true
		return r.runSteps(ctx, s.Next, partition, m, out, log)

	case config.StepWebhook:
		if err := r.webhookWithRetry(ctx, s.URL, correlationID, m.value, log); err != nil {
			if ctx.Err() != nil {
				return ctx.Err()
			}
			m.reason = err.Error()
			if len(s.OnFailure) == 0 {
				return r.flowDeadLetter(ctx, config.Step{ID: id}, m, out, log)
			}
			return r.runSteps(ctx, s.OnFailure, partition, m, out, log)
		}
		if r.watching() {
			rec := r.tapRecord(tap.StageOut, correlationID, m.key, m.headers)
			rec.To, rec.Target, rec.Rule = tap.ToWebhook, s.URL, id
			rec.SetValue(m.value)
			tap.Default.Publish(rec)
		}
		out.delivered = true
		return r.runSteps(ctx, s.Next, partition, m, out, log)

	case config.StepReject:
		if err := r.flowReject(ctx, s, m, out, log); err != nil {
			return err
		}
		return r.runSteps(ctx, s.Next, partition, m, out, log)

	case config.StepDeadLetter:
		if err := r.flowDeadLetter(ctx, s, m, out, log); err != nil {
			return err
		}
		return r.runSteps(ctx, s.Next, partition, m, out, log)

	case config.StepDrop:
		log.Info("message dropped")
		r.tapOut(tap.ToDropped, "", id, "dropped by flow step "+id, 0, m.key, m.value, m.headers)
		return nil
	}
	return fmt.Errorf("flow step %q: unhandled type %q", id, s.Type)
}

// flowReject sends m to the step's topic, or the pipeline's reject topic,
// or its dead-letter topic when it has no reject topic.
func (r *Runner) flowReject(ctx context.Context, s config.Step, m flowMsg, out *flowOutcome, log *slog.Logger) error {
	topic := s.Topic
	if topic == "" {
		topic = r.pipeline.RejectTopic
	}
	if topic == "" {
		return r.flowDeadLetter(ctx, s, m, out, log)
	}
	if err := r.flowRoute(ctx, s, topic, tap.ToReject, events.MessageRejected, m, log); err != nil {
		return err
	}
	out.rejected = true
	return nil
}

func (r *Runner) flowDeadLetter(ctx context.Context, s config.Step, m flowMsg, out *flowOutcome, log *slog.Logger) error {
	topic := s.Topic
	if topic == "" {
		topic = r.pipeline.DeadLetterTopic
	}
	if err := r.flowRoute(ctx, s, topic, tap.ToDLQ, events.MessageDeadLettered, m, log); err != nil {
		return err
	}
	out.deadLettered = true
	return nil
}

func (r *Runner) flowRoute(ctx context.Context, s config.Step, topic, to string, kind events.Kind, m flowMsg, log *slog.Logger) error {
	reason := s.Reason
	if reason == "" {
		reason = m.reason
	}
	if reason == "" {
		reason = "sent here by flow step " + s.ID
	}
	headers := make(map[string]string, len(m.headers)+3)
	for k, v := range m.headers {
		headers[k] = v
	}
	headers[ReasonHeader] = reason
	headers[PipelineHeader] = r.pipeline.Name
	headers[FailedAtHeader] = time.Now().UTC().Format(time.RFC3339)
	target := r.shared.overrideProducer(topic)
	if topic == r.pipeline.DeadLetterTopic && r.shared.dlq != nil {
		target = r.shared.dlq
	} else if topic == r.pipeline.RejectTopic && r.shared.reject != nil {
		target = r.shared.reject
	}
	if err := r.sendWithRetry(ctx, target, m.key, m.value, headers, log); err != nil {
		return fmt.Errorf("step %s producing to %s: %w", s.ID, topic, err)
	}
	r.tapOut(to, topic, s.ID, reason, statusOf(m.response), m.key, m.value, headers)
	events.Record(r.pipeline.Name, kind, reason, map[string]string{"key": string(m.key), "correlation_id": headers[callback.CorrelationIDHeader], "step": s.ID})
	log.Info("message sent by flow", "to", to, "topic", topic, "reason", reason)
	return nil
}

func (m flowMsg) clone() flowMsg {
	h := make(map[string]string, len(m.headers))
	for k, v := range m.headers {
		h[k] = v
	}
	m.headers = h
	return m
}

func (m flowMsg) env() map[string]any {
	data, ok := parseObject(m.value)
	if !ok {
		data = map[string]any{}
	}
	original := m.original
	if original == nil {
		original = map[string]any{}
	}
	resp := m.response
	if resp == nil {
		resp = map[string]any{}
	}
	return map[string]any{"data": data, "original": original, "response": resp, "reason": m.reason, "key": string(m.key), "headers": m.headers}
}

func response(resp *callback.Response) map[string]any {
	if resp == nil {
		return nil
	}
	body, _ := parseObject(resp.Body)
	return map[string]any{"status": resp.StatusCode, "body": body}
}

func statusOf(resp map[string]any) int {
	if s, ok := resp["status"].(int); ok {
		return s
	}
	return 0
}

func parseObject(v []byte) (map[string]any, bool) {
	var m map[string]any
	if len(v) == 0 || json.Unmarshal(v, &m) != nil {
		return nil, false
	}
	return m, true
}
