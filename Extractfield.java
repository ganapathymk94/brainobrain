@Entity
@Table(name = "schema_field_metadata")
public class SchemaFieldMetadata {
    @Id
    @GeneratedValue
    private UUID id;

    private String topicName;
    private String fieldPath;
    private String fieldName;
    private String dataType;
    private String docString;
    private Boolean isNullable;
    private String status; // ACTIVE or INACTIVE
    private Integer version;
    private LocalDateTime lastUpdated;

    // Getters and setters
}


public interface SchemaFieldRepository extends JpaRepository<SchemaFieldMetadata, UUID> {
    List<SchemaFieldMetadata> findByTopicNameAndStatus(String topicName, String status);
}


public class AvroFieldExtractor {

    public static List<SchemaFieldMetadata> extractFields(String schemaJson, String topicName, int version) throws Exception {
        Schema schema = new Schema.Parser().parse(schemaJson);
        List<SchemaFieldMetadata> result = new ArrayList<>();
        traverse(schema, "", topicName, version, result);
        return result;
    }

    private static void traverse(Schema schema, String prefix, String topicName, int version, List<SchemaFieldMetadata> result) {
        if (schema.getType() == Schema.Type.RECORD) {
            for (Schema.Field field : schema.getFields()) {
                String fieldName = field.name();
                String fullPath = prefix.isEmpty() ? fieldName : prefix + "." + fieldName;
                String doc = field.doc() != null ? field.doc() : "";
                Schema fieldSchema = unwrapUnion(field.schema());
                boolean isNullable = isNullable(field.schema());

                if (fieldSchema.getType() == Schema.Type.RECORD || fieldSchema.getType() == Schema.Type.ARRAY) {
                    traverse(fieldSchema, fullPath, topicName, version, result);
                } else {
                    SchemaFieldMetadata meta = new SchemaFieldMetadata();
                    meta.setFieldName(fieldName);
                    meta.setFieldPath(fullPath);
                    meta.setDocString(doc);
                    meta.setTopicName(topicName);
                    meta.setVersion(version);
                    meta.setDataType(fieldSchema.getType().getName());
                    meta.setIsNullable(isNullable);
                    meta.setStatus("ACTIVE");
                    meta.setLastUpdated(LocalDateTime.now());
                    result.add(meta);
                }
            }
        } else if (schema.getType() == Schema.Type.ARRAY) {
            traverse(schema.getElementType(), prefix + "[]", topicName, version, result);
        }
    }

    private static Schema unwrapUnion(Schema schema) {
        if (schema.getType() == Schema.Type.UNION) {
            return schema.getTypes().stream()
                .filter(s -> s.getType() != Schema.Type.NULL)
                .findFirst()
                .orElse(schema);
        }
        return schema;
    }

    private static boolean isNullable(Schema schema) {
        return schema.getType() == Schema.Type.UNION &&
               schema.getTypes().stream().anyMatch(s -> s.getType() == Schema.Type.NULL);
    }
}


@Service
public class SchemaSyncJob {

    @Autowired
    private SchemaFieldRepository repository;

    @Scheduled(cron = "0 0 2 * * ?") // Runs daily at 2 AM
    public void syncSchemas() {
        List<TopicSchema> latestSchemas = fetchLatestSchemasFromDB(); // Your SQL Server source

        for (TopicSchema topicSchema : latestSchemas) {
            List<SchemaFieldMetadata> newFields = AvroFieldExtractor.extractFields(
                topicSchema.getSchemaJson(),
                topicSchema.getTopicName(),
                topicSchema.getVersion()
            );

            List<SchemaFieldMetadata> existingFields = repository.findByTopicNameAndStatus(topicSchema.getTopicName(), "ACTIVE");

            Map<String, SchemaFieldMetadata> existingMap = existingFields.stream()
                .collect(Collectors.toMap(SchemaFieldMetadata::getFieldPath, f -> f));

            Set<String> newPaths = newFields.stream().map(SchemaFieldMetadata::getFieldPath).collect(Collectors.toSet());
            Set<String> existingPaths = existingMap.keySet();

            // Add new fields
            for (SchemaFieldMetadata newField : newFields) {
                if (!existingPaths.contains(newField.getFieldPath())) {
                    repository.save(newField);
                }
            }

            // Mark removed fields as INACTIVE
            for (String oldPath : existingPaths) {
                if (!newPaths.contains(oldPath)) {
                    SchemaFieldMetadata removed = existingMap.get(oldPath);
                    removed.setStatus("INACTIVE");
                    removed.setLastUpdated(LocalDateTime.now());
                    repository.save(removed);
                }
            }
        }
    }
}


public class TopicSchema {
    private String topicName;
    private int version;
    private String schemaJson;

    // Getters and setters
}


WITH RankedSchemas AS (
    SELECT *,
           ROW_NUMBER() OVER (PARTITION BY topic_name ORDER BY version DESC) AS rn
    FROM SchemaVersions
)
SELECT topic_name, version, schema_json
FROM RankedSchemas
WHERE rn = 1

@Repository
public class SchemaVersionsRepositoryImpl implements SchemaVersionsRepository {

    @PersistenceContext
    private EntityManager entityManager;

    @Override
    public List<TopicSchema> fetchLatestSchemas() {
        String sql = """
            WITH RankedSchemas AS (
                SELECT topic_name, version, schema_json,
                       ROW_NUMBER() OVER (PARTITION BY topic_name ORDER BY version DESC) AS rn
                FROM SchemaVersions
            )
            SELECT topic_name, version, schema_json
            FROM RankedSchemas
            WHERE rn = 1
        """;

        List<Object[]> rows = entityManager.createNativeQuery(sql).getResultList();

        List<TopicSchema> result = new ArrayList<>();
        for (Object[] row : rows) {
            String topicName = (String) row[0];
            int version = ((Number) row[1]).intValue();
            String schemaJson = (String) row[2];
            result.add(new TopicSchema(topicName, version, schemaJson));
        }

        return result;
    }
}
