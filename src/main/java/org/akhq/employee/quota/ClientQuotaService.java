package org.akhq.employee.quota;

import jakarta.inject.Singleton;
import org.akhq.modules.KafkaModule;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.common.quota.ClientQuotaAlteration;
import org.apache.kafka.common.quota.ClientQuotaEntity;
import org.apache.kafka.common.quota.ClientQuotaFilter;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

@Singleton
public class ClientQuotaService {
    private static final String PRODUCER_BYTE_RATE = "producer_byte_rate";
    private static final String CONSUMER_BYTE_RATE = "consumer_byte_rate";
    private static final String REQUEST_PERCENTAGE = "request_percentage";

    private final KafkaModule kafkaModule;

    public ClientQuotaService(KafkaModule kafkaModule) {
        this.kafkaModule = kafkaModule;
    }

    public List<ClientQuotaEntry> list(String clusterId) throws Exception {
        AdminClient admin = kafkaModule.getAdminClient(clusterId);
        Map<ClientQuotaEntity, Map<String, Double>> all = admin
            .describeClientQuotas(ClientQuotaFilter.all())
            .entities().get();

        List<ClientQuotaEntry> entries = new ArrayList<>();
        for (Map.Entry<ClientQuotaEntity, Map<String, Double>> row : all.entrySet()) {
            for (Map.Entry<String, String> identity : row.getKey().entries().entrySet()) {
                Map<String, Double> values = row.getValue();
                entries.add(new ClientQuotaEntry(
                    identity.getKey(),
                    identity.getValue(),
                    values.get(PRODUCER_BYTE_RATE),
                    values.get(CONSUMER_BYTE_RATE),
                    values.get(REQUEST_PERCENTAGE)
                ));
            }
        }
        return entries;
    }

    public void set(String clusterId, ClientQuotaRequest request) throws Exception {
        AdminClient admin = kafkaModule.getAdminClient(clusterId);
        ClientQuotaEntity entity = toEntity(request.entityType, request.entityName);

        List<ClientQuotaAlteration.Op> ops = new ArrayList<>();
        ops.add(new ClientQuotaAlteration.Op(PRODUCER_BYTE_RATE, request.producerByteRate));
        ops.add(new ClientQuotaAlteration.Op(CONSUMER_BYTE_RATE, request.consumerByteRate));
        ops.add(new ClientQuotaAlteration.Op(REQUEST_PERCENTAGE, request.requestPercentage));

        admin.alterClientQuotas(List.of(new ClientQuotaAlteration(entity, ops))).all().get();
    }

    public void delete(String clusterId, ClientQuotaEntityType entityType, String entityName) throws Exception {
        AdminClient admin = kafkaModule.getAdminClient(clusterId);
        ClientQuotaEntity entity = toEntity(entityType, entityName);

        List<ClientQuotaAlteration.Op> ops = List.of(
            new ClientQuotaAlteration.Op(PRODUCER_BYTE_RATE, null),
            new ClientQuotaAlteration.Op(CONSUMER_BYTE_RATE, null),
            new ClientQuotaAlteration.Op(REQUEST_PERCENTAGE, null)
        );

        admin.alterClientQuotas(List.of(new ClientQuotaAlteration(entity, ops))).all().get();
    }

    private ClientQuotaEntity toEntity(ClientQuotaEntityType entityType, String entityName) {
        String key = switch (entityType) {
            case USER -> ClientQuotaEntity.USER;
            case CLIENT_ID -> ClientQuotaEntity.CLIENT_ID;
            case IP -> ClientQuotaEntity.IP;
        };
        return new ClientQuotaEntity(Map.of(key, entityName));
    }
}
