import org.springframework.web.client.RestTemplate;

@Service
public class SchemaRegistryService {

    private final String schemaRegistryUrl = "http://localhost:8081";
    private final KafkaRBACManager rbacManager;

    public SchemaRegistryService(KafkaRBACManager rbacManager) {
        this.rbacManager = rbacManager;
    }

    public String registerSchema(String schemaName, String schemaJson, String userGroup) {
        if (!rbacManager.validateSchemaAccess(userGroup, "CREATE")) {
            return "❌ Access Denied: You don't have CREATE permissions.";
        }

        RestTemplate restTemplate = new RestTemplate();
        String url = schemaRegistryUrl + "/subjects/" + schemaName + "-value/versions";
        Map<String, String> request = new HashMap<>();
        request.put("schema", schemaJson);
        
        restTemplate.postForEntity(url, request, String.class);
        return "✅ Schema '" + schemaName + "' registered successfully!";
    }

    public String deleteSchema(String schemaName, String userGroup) {
        if (!rbacManager.validateSchemaAccess(userGroup, "DELETE")) {
            return "❌ Access Denied: You don't have DELETE permissions.";
        }

        RestTemplate restTemplate = new RestTemplate();
        String url = schemaRegistryUrl + "/subjects/" + schemaName;
        
        restTemplate.delete(url);
        return "✅ Schema '" + schemaName + "' deleted successfully!";
    }

    public String getSchemaVersions(String schemaName, String userGroup) {
        if (!rbacManager.validateSchemaAccess(userGroup, "READ")) {
            return "❌ Access Denied: You don't have READ permissions.";
        }

        RestTemplate restTemplate = new RestTemplate();
        String url = schemaRegistryUrl + "/subjects/" + schemaName + "/versions";
        
        return restTemplate.getForObject(url, String.class);
    }
}
