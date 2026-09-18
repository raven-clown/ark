package org.akhq.employee.cluster;

import com.fasterxml.jackson.databind.ObjectMapper;
import jakarta.inject.Singleton;

import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

/**
 * Stores cluster connection definitions an admin has authored and generates the equivalent
 * akhq.connections YAML. This does not make the cluster reachable by itself - see the class
 * comment on ClusterConnectionDefinition and the "Dynamic cluster management" section of
 * docs/enterprise/architecture-blueprint.md for why the KafkaModule wiring is a separate,
 * deliberately deferred change.
 */
@Singleton
public class ClusterConnectionService {
    private static final Pattern SAFE_NAME = Pattern.compile("^[a-zA-Z0-9_-]+$");

    private final ClusterConnectionDefinitionRepository repository;
    private final ObjectMapper mapper = new ObjectMapper();

    public ClusterConnectionService(ClusterConnectionDefinitionRepository repository) {
        this.repository = repository;
    }

    public List<ClusterConnectionView> list() {
        return repository.findAllByOrderByNameAsc().stream()
            .map(this::toView)
            .collect(Collectors.toList());
    }

    public ClusterConnectionView get(String name) {
        return toView(requireDefinition(name));
    }

    public ClusterConnectionView create(ClusterConnectionRequest request, String createdBy) {
        if (!SAFE_NAME.matcher(request.name).matches()) {
            throw new IllegalArgumentException(
                "Cluster name must contain only letters, digits, '-' and '_': " + request.name);
        }
        if (repository.findByName(request.name).isPresent()) {
            throw new IllegalArgumentException("A cluster connection named " + request.name + " already exists");
        }

        ClusterConnectionDefinition definition = new ClusterConnectionDefinition();
        definition.setName(request.name);
        definition.setCreatedBy(createdBy);
        applyRequest(definition, request);

        return toView(repository.save(definition));
    }

    public ClusterConnectionView update(String name, ClusterConnectionRequest request) {
        ClusterConnectionDefinition definition = requireDefinition(name);
        applyRequest(definition, request);
        return toView(repository.update(definition));
    }

    public void delete(String name) {
        requireDefinition(name);
        repository.deleteByName(name);
    }

    private void applyRequest(ClusterConnectionDefinition definition, ClusterConnectionRequest request) {
        definition.setBootstrapServers(request.bootstrapServers);
        definition.setSchemaRegistryUrl(request.schemaRegistryUrl);
        definition.setSchemaRegistryType(
            request.schemaRegistryType == null || request.schemaRegistryType.isBlank()
                ? "CONFLUENT" : request.schemaRegistryType);
        definition.setConnectsJson(writeJson(request.connects));
        definition.setKsqldbsJson(writeJson(request.ksqldbs));
    }

    private ClusterConnectionDefinition requireDefinition(String name) {
        return repository.findByName(name)
            .orElseThrow(() -> new NoSuchElementException("Cluster connection not found: " + name));
    }

    private ClusterConnectionView toView(ClusterConnectionDefinition definition) {
        List<NamedUrl> connects = readJson(definition.getConnectsJson());
        List<NamedUrl> ksqldbs = readJson(definition.getKsqldbsJson());
        return new ClusterConnectionView(
            definition.getId(), definition.getName(), definition.getBootstrapServers(),
            definition.getSchemaRegistryUrl(), definition.getSchemaRegistryType(),
            connects, ksqldbs, definition.getCreatedBy(), definition.getCreatedAt(),
            toYamlSnippet(definition, connects, ksqldbs)
        );
    }

    private String toYamlSnippet(ClusterConnectionDefinition definition, List<NamedUrl> connects, List<NamedUrl> ksqldbs) {
        StringBuilder yaml = new StringBuilder();
        yaml.append("akhq:\n  connections:\n    ").append(definition.getName()).append(":\n");
        yaml.append("      properties:\n        bootstrap.servers: \"")
            .append(escape(definition.getBootstrapServers())).append("\"\n");

        if (definition.getSchemaRegistryUrl() != null && !definition.getSchemaRegistryUrl().isBlank()) {
            yaml.append("      schema-registry:\n        url: \"")
                .append(escape(definition.getSchemaRegistryUrl())).append("\"\n        type: ")
                .append(definition.getSchemaRegistryType()).append("\n");
        }

        if (!connects.isEmpty()) {
            yaml.append("      connect:\n");
            for (NamedUrl entry : connects) {
                yaml.append("        - name: \"").append(escape(entry.name())).append("\"\n");
                yaml.append("          url: \"").append(escape(entry.url())).append("\"\n");
            }
        }

        if (!ksqldbs.isEmpty()) {
            yaml.append("      ksqldb:\n");
            for (NamedUrl entry : ksqldbs) {
                yaml.append("        - name: \"").append(escape(entry.name())).append("\"\n");
                yaml.append("          url: \"").append(escape(entry.url())).append("\"\n");
            }
        }

        return yaml.toString();
    }

    private static String escape(String value) {
        if (value == null) {
            return "";
        }
        return value.replace("\\", "\\\\").replace("\"", "\\\"");
    }

    @SuppressWarnings("unchecked")
    private List<NamedUrl> readJson(String json) {
        try {
            List<Map<String, String>> raw = mapper.readValue(json, List.class);
            return raw.stream()
                .map(m -> new NamedUrl(m.get("name"), m.get("url")))
                .collect(Collectors.toList());
        } catch (Exception e) {
            return List.of();
        }
    }

    private String writeJson(List<NamedUrl> entries) {
        try {
            return mapper.writeValueAsString(entries == null ? List.of() : entries);
        } catch (Exception e) {
            return "[]";
        }
    }
}
