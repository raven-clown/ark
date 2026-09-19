import React from 'react';
import Header from '../Header';
import Table from '../../components/Table';
import Root from '../../components/Root';
import { toast } from 'react-toastify';
import 'react-toastify/dist/ReactToastify.css';
import { withRouter } from '../../utils/withRouter';
import {
  uriClusterOpsTransactions,
  uriClusterOpsAbortTransaction,
  uriClusterOpsElectLeader,
  uriClusterOpsLogDirs,
  uriClusterOpsQuorum
} from '../../utils/endpoints';

class ClusterOps extends Root {
  state = {
    transactions: [],
    logDirs: [],
    quorum: null,
    loading: true,
    abortForm: { topic: '', partition: '', producerId: '', producerEpoch: '', coordinatorEpoch: '' },
    leaderForm: { topic: '', partition: '' }
  };

  componentDidMount() {
    this.loadAll();
  }

  clusterId() {
    return this.props.params.clusterId;
  }

  async loadAll() {
    this.setState({ loading: true });
    const cluster = this.clusterId();
    try {
      const [txRes, dirsRes, quorumRes] = await Promise.allSettled([
        this.getApi(uriClusterOpsTransactions(cluster)),
        this.getApi(uriClusterOpsLogDirs(cluster)),
        this.getApi(uriClusterOpsQuorum(cluster))
      ]);
      this.setState({
        transactions: txRes.status === 'fulfilled' ? txRes.value.data || [] : [],
        logDirs: dirsRes.status === 'fulfilled' ? dirsRes.value.data || [] : [],
        quorum: quorumRes.status === 'fulfilled' ? quorumRes.value.data : null,
        loading: false
      });
    } catch (err) {
      this.setState({ loading: false });
    }
  }

  handleAbortChange = e => {
    const { name, value } = e.target;
    this.setState({ abortForm: { ...this.state.abortForm, [name]: value } });
  };

  abortTransaction = async e => {
    e.preventDefault();
    const { abortForm } = this.state;
    try {
      await this.postApi(uriClusterOpsAbortTransaction(this.clusterId()), {
        topic: abortForm.topic,
        partition: Number(abortForm.partition),
        producerId: Number(abortForm.producerId),
        producerEpoch: Number(abortForm.producerEpoch),
        coordinatorEpoch: Number(abortForm.coordinatorEpoch)
      });
      toast.success('Transaction aborted');
      this.setState({
        abortForm: { topic: '', partition: '', producerId: '', producerEpoch: '', coordinatorEpoch: '' }
      });
      this.loadAll();
    } catch (err) {
      // toasted by the api layer
    }
  };

  handleLeaderChange = e => {
    const { name, value } = e.target;
    this.setState({ leaderForm: { ...this.state.leaderForm, [name]: value } });
  };

  electLeader = async e => {
    e.preventDefault();
    const { leaderForm } = this.state;
    try {
      await this.postApi(uriClusterOpsElectLeader(this.clusterId(), leaderForm.topic, Number(leaderForm.partition)));
      toast.success('Preferred leader election triggered');
    } catch (err) {
      // toasted by the api layer
    }
  };

  render() {
    const { transactions, logDirs, quorum, loading, abortForm, leaderForm } = this.state;

    const txRows = transactions.map((t, i) => ({
      id: `${t.transactionalId}-${i}`,
      transactionalId: t.transactionalId,
      producerId: t.producerId,
      state: t.state
    }));

    const dirRows = logDirs.map((d, i) => ({
      id: `${d.brokerId}-${d.path}-${i}`,
      brokerId: d.brokerId,
      path: d.path,
      totalBytes: d.totalBytes ?? '-',
      usableBytes: d.usableBytes ?? '-',
      cordoned: d.cordoned ? 'yes' : 'no'
    }));

    return (
      <div>
        <Header title="Cluster Operations" />

        <h3>Transactions</h3>
        <Table
          loading={loading}
          columns={[
            { id: 'transactionalId', accessor: 'transactionalId', colName: 'Transactional ID', sortable: true },
            { id: 'producerId', accessor: 'producerId', colName: 'Producer ID' },
            { id: 'state', accessor: 'state', colName: 'State' }
          ]}
          actions={[]}
          data={txRows}
          updateData={() => {}}
          noContent="No transactions reported by this cluster."
        />

        <form className="khq-data-filter khq-nav p-3 mt-3 mb-3" onSubmit={this.abortTransaction}>
          <p className="mb-2">
            Abort a hung transaction. Get the producer ID, epoch and coordinator epoch from the
            transaction coordinator broker&apos;s logs or <code>kafka-transactions.sh</code> first,
            this form does not look them up for you.
          </p>
          <div className="row g-2 align-items-end">
            <div className="col-auto">
              <label className="form-label">Topic</label>
              <input className="form-control" name="topic" value={abortForm.topic} onChange={this.handleAbortChange} />
            </div>
            <div className="col-auto">
              <label className="form-label">Partition</label>
              <input className="form-control" name="partition" value={abortForm.partition} onChange={this.handleAbortChange} />
            </div>
            <div className="col-auto">
              <label className="form-label">Producer ID</label>
              <input className="form-control" name="producerId" value={abortForm.producerId} onChange={this.handleAbortChange} />
            </div>
            <div className="col-auto">
              <label className="form-label">Producer epoch</label>
              <input className="form-control" name="producerEpoch" value={abortForm.producerEpoch} onChange={this.handleAbortChange} />
            </div>
            <div className="col-auto">
              <label className="form-label">Coordinator epoch</label>
              <input className="form-control" name="coordinatorEpoch" value={abortForm.coordinatorEpoch} onChange={this.handleAbortChange} />
            </div>
            <div className="col-auto">
              <button type="submit" className="btn btn-danger">
                Abort transaction
              </button>
            </div>
          </div>
        </form>

        <h3>Preferred leader election</h3>
        <form className="khq-data-filter khq-nav p-3 mb-3" onSubmit={this.electLeader}>
          <div className="row g-2 align-items-end">
            <div className="col-auto">
              <label className="form-label">Topic</label>
              <input className="form-control" name="topic" value={leaderForm.topic} onChange={this.handleLeaderChange} />
            </div>
            <div className="col-auto">
              <label className="form-label">Partition</label>
              <input className="form-control" name="partition" value={leaderForm.partition} onChange={this.handleLeaderChange} />
            </div>
            <div className="col-auto">
              <button type="submit" className="btn btn-primary">
                Trigger election
              </button>
            </div>
          </div>
        </form>

        <h3>Log directories</h3>
        <Table
          loading={loading}
          columns={[
            { id: 'brokerId', accessor: 'brokerId', colName: 'Broker', sortable: true },
            { id: 'path', accessor: 'path', colName: 'Path' },
            { id: 'totalBytes', accessor: 'totalBytes', colName: 'Total bytes' },
            { id: 'usableBytes', accessor: 'usableBytes', colName: 'Usable bytes' },
            { id: 'cordoned', accessor: 'cordoned', colName: 'Cordoned' }
          ]}
          actions={[]}
          data={dirRows}
          updateData={() => {}}
          noContent="No log directory information available."
        />

        <h3>Metadata quorum (KRaft)</h3>
        <div className="khq-data-filter khq-nav p-3 mb-3">
          {quorum ? (
            <>
              <p className="mb-1">
                Leader: <b>{quorum.leaderId}</b> (epoch {quorum.leaderEpoch}), high watermark{' '}
                {quorum.highWatermark}
              </p>
              <p className="mb-1">
                Voters: {quorum.voters.map(v => `${v.replicaId} (offset ${v.logEndOffset})`).join(', ')}
              </p>
              <p className="mb-0">
                Observers:{' '}
                {quorum.observers.length > 0
                  ? quorum.observers.map(v => `${v.replicaId} (offset ${v.logEndOffset})`).join(', ')
                  : 'none'}
              </p>
            </>
          ) : (
            <p className="mb-0">Not available, this cluster may not be running in KRaft mode.</p>
          )}
        </div>
      </div>
    );
  }
}

export default withRouter(ClusterOps);
