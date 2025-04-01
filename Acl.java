import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AclBinding;
import org.apache.kafka.common.acl.AclOperation;
import org.apache.kafka.common.acl.AclPermissionType;
import org.apache.kafka.common.resource.ResourcePattern;
import org.apache.kafka.common.resource.ResourceType;
import java.util.*;

public class KafkaAclManager {

    private final AdminClient adminClient;

    public KafkaAclManager(String bootstrapServers) {
        Properties props = new Properties();
        props.put("bootstrap.servers", bootstrapServers);
        props.put("security.protocol", "PLAINTEXT");
        this.adminClient = AdminClient.create(props);
    }

    // Grant producer access
    public void grantProducerAccess(String userGroup, String topic) {
        AclBinding aclBinding = new AclBinding(
            new ResourcePattern(ResourceType.TOPIC, topic, ResourcePattern.WildcardType.LITERAL),
            new kafka.common.acl.AccessControlEntry("User:" + userGroup, "*", AclOperation.WRITE, AclPermissionType.ALLOW)
        );
        adminClient.createAcls(Collections.singletonList(aclBinding));
        System.out.println("✅ Producer access granted for user group: " + userGroup);
    }

    // Grant consumer access
    public void grantConsumerAccess(String userGroup, String topic) {
        AclBinding aclBinding = new AclBinding(
            new ResourcePattern(ResourceType.TOPIC, topic, ResourcePattern.WildcardType.LITERAL),
            new kafka.common.acl.AccessControlEntry("User:" + userGroup, "*", AclOperation.READ, AclPermissionType.ALLOW)
        );
        adminClient.createAcls(Collections.singletonList(aclBinding));
        System.out.println("✅ Consumer access granted for user group: " + userGroup);
    }

    // Revoke access
    public void revokeAccess(String userGroup, String topic) {
        AclBinding aclBinding = new AclBinding(
            new ResourcePattern(ResourceType.TOPIC, topic, ResourcePattern.WildcardType.LITERAL),
            new kafka.common.acl.AccessControlEntry("User:" + userGroup, "*", AclOperation.ALL, AclPermissionType.DENY)
        );
        adminClient.deleteAcls(Collections.singletonList(aclBinding));
        System.out.println("❌ Access revoked for user group: " + userGroup);
    }
}
