import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AclBinding;
import org.apache.kafka.common.acl.AclOperation;
import org.apache.kafka.common.acl.AclPermissionType;
import org.apache.kafka.common.resource.ResourcePattern;
import org.apache.kafka.common.resource.ResourceType;
import java.util.*;

public class KafkaRBACManager {

    private final AdminClient adminClient;
    private final Map<String, List<AclOperation>> rolePermissions;

    public KafkaRBACManager(String bootstrapServers) {
        Properties props = new Properties();
        props.put("bootstrap.servers", bootstrapServers);
        this.adminClient = AdminClient.create(props);

        // Define role-based permissions
        rolePermissions = new HashMap<>();
        rolePermissions.put("admin", Arrays.asList(AclOperation.ALL));
        rolePermissions.put("producer", Arrays.asList(AclOperation.WRITE));
        rolePermissions.put("consumer", Arrays.asList(AclOperation.READ));
    }

    public void assignRoleToUser(String userGroup, String role, String topic) {
        if (!rolePermissions.containsKey(role)) {
            System.out.println("❌ Invalid role: " + role);
            return;
        }

        List<AclBinding> aclBindings = new ArrayList<>();
        for (AclOperation operation : rolePermissions.get(role)) {
            AclBinding aclBinding = new AclBinding(
                new ResourcePattern(ResourceType.TOPIC, topic, ResourcePattern.WildcardType.LITERAL),
                new kafka.common.acl.AccessControlEntry("User:" + userGroup, "*", operation, AclPermissionType.ALLOW)
            );
            aclBindings.add(aclBinding);
        }

        adminClient.createAcls(aclBindings);
        System.out.println("✅ Role '" + role + "' assigned to user group: " + userGroup);
    }
}
