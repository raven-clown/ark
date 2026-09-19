package org.akhq.employee.scram;

import jakarta.inject.Singleton;
import org.akhq.modules.KafkaModule;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.ScramCredentialInfo;
import org.apache.kafka.clients.admin.ScramMechanism;
import org.apache.kafka.clients.admin.UserScramCredentialDeletion;
import org.apache.kafka.clients.admin.UserScramCredentialUpsertion;
import org.apache.kafka.clients.admin.UserScramCredentialsDescription;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

@Singleton
public class ScramCredentialService {
    private final KafkaModule kafkaModule;

    public ScramCredentialService(KafkaModule kafkaModule) {
        this.kafkaModule = kafkaModule;
    }

    public List<ScramCredentialEntry> list(String clusterId) throws Exception {
        AdminClient admin = kafkaModule.getAdminClient(clusterId);
        Map<String, UserScramCredentialsDescription> all = admin.describeUserScramCredentials().all().get();

        List<ScramCredentialEntry> entries = new ArrayList<>();
        for (UserScramCredentialsDescription description : all.values()) {
            for (ScramCredentialInfo info : description.credentialInfos()) {
                entries.add(new ScramCredentialEntry(
                    description.name(), info.mechanism().mechanismName(), info.iterations()
                ));
            }
        }
        return entries;
    }

    public void set(String clusterId, ScramCredentialRequest request) throws Exception {
        AdminClient admin = kafkaModule.getAdminClient(clusterId);
        ScramMechanism mechanism = ScramMechanism.valueOf(request.mechanism.name());
        ScramCredentialInfo info = new ScramCredentialInfo(mechanism, request.iterations);
        UserScramCredentialUpsertion upsertion =
            new UserScramCredentialUpsertion(request.username, info, request.password);
        admin.alterUserScramCredentials(List.of(upsertion)).all().get();
    }

    public void delete(String clusterId, String username, ScramMechanismChoice mechanism) throws Exception {
        AdminClient admin = kafkaModule.getAdminClient(clusterId);
        ScramMechanism kafkaMechanism = ScramMechanism.valueOf(mechanism.name());
        UserScramCredentialDeletion deletion = new UserScramCredentialDeletion(username, kafkaMechanism);
        admin.alterUserScramCredentials(List.of(deletion)).all().get();
    }
}
