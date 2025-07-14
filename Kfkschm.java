@RestController
@RequestMapping("/api/schema")
public class SchemaController {

    private final SchemaRegistryService registry;
    private final SchemaValidatorService validator;
    private final BitbucketService bitbucket;

    public SchemaController(SchemaRegistryService registry, SchemaValidatorService validator, BitbucketService bitbucket) {
        this.registry = registry;
        this.validator = validator;
        this.bitbucket = bitbucket;
    }

    @GetMapping("/latest/{subject}")
    public String getLatestSchema(@PathVariable String subject) throws Exception {
        return registry.fetchLatestSchema(subject);
    }

    @PostMapping("/validate")
    public Map<String, Object> validateSchema(@RequestBody Map<String, String> payload) throws Exception {
        String subject = payload.get("subject");
        String newSchema = payload.get("schema");
        return validator.validate(subject, newSchema);
    }

    @PostMapping("/push")
    public ResponseEntity<String> pushSchema(@RequestBody Map<String, String> payload) {
        String subject = payload.get("subject");
        String schema = payload.get("schema");
        boolean pushed = bitbucket.push(subject, schema);
        return ResponseEntity.ok(pushed ? "✅ Pushed to Bitbucket" : "❌ Push failed");
    }
}

@Service
public class SchemaValidatorService {

    @Autowired SchemaRegistryService registry;
    @Autowired ObjectMapper mapper;

    public Map<String, Object> validate(String subject, String proposedSchemaRaw) throws Exception {
        JsonNode latest = mapper.readTree(registry.fetchLatestSchema(subject));
        JsonNode proposed = mapper.readTree(proposedSchemaRaw);

        Map<String, JsonNode> oldFields = extract(latest);
        Map<String, JsonNode> newFields = extract(proposed);

        List<Map<String, String>> issues = new ArrayList<>();
        boolean compatible = true;

        for (String name : newFields.keySet()) {
            JsonNode newField = newFields.get(name);
            String type = newField.get("type").toString();
            boolean optional = type.contains("null");
            boolean hasDefault = newField.has("default");

            if (!oldFields.containsKey(name)) {
                if (!optional && !hasDefault) {
                    compatible = false;
                    issues.add(Map.of("field", name, "issue", "New field without default or nullability"));
                }
            } else {
                String oldType = oldFields.get(name).get("type").toString();
                if (!oldType.equals(type)) {
                    compatible = false;
                    issues.add(Map.of("field", name, "issue", "Type changed from " + oldType + " to " + type));
                }
            }
        }

        for (String name : oldFields.keySet()) {
            if (!newFields.containsKey(name)) {
                compatible = false;
                issues.add(Map.of("field", name, "issue", "Field removed"));
            }
        }

        return Map.of("compatible", compatible, "issues", issues);
    }

    private Map<String, JsonNode> extract(JsonNode schema) {
        Map<String, JsonNode> fieldMap = new HashMap<>();
        for (JsonNode f : schema.get("fields")) {
            fieldMap.put(f.get("name").asText(), f);
        }
        return fieldMap;
    }
}


@Service
public class SchemaRegistryService {

    private static final String REGISTRY_URL = "http://localhost:8081"; // Adjust
    private final RestTemplate rest = new RestTemplate();

    public String fetchLatestSchema(String subject) {
        Map<?, ?> latest = rest.getForObject(REGISTRY_URL + "/subjects/" + subject + "/versions/latest", Map.class);
        return String.valueOf(latest.get("schema"));
    }
}




import React, { useState } from "react";
import {
  TextField, Button, Typography, MenuItem,
  Table, TableBody, TableRow, TableCell, Paper
} from "@mui/material";
import axios from "axios";
export default function SchemaValidatorPage({ topic }) {
  const [latest, setLatest] = useState("");
  const [proposedSchemaText, setProposedSchemaText] = useState("");
  const [parsedSchema, setParsedSchema] = useState(null);
  const [issues, setIssues] = useState([]);
  const [compatible, setCompatible] = useState(null);

  useEffect(() => {
    if (topic) {
      axios.get(`/api/schema/latest/${topic}`)
        .then(res => setLatest(res.data))
        .catch(err => setLatest("❌ Error fetching schema"));
    }
  }, [topic]);

  const handleSchemaTextChange = (text) => {
    setProposedSchemaText(text);
    try {
      const parsed = JSON.parse(text);
      setParsedSchema(parsed);
    } catch {
      setParsedSchema(null);
    }
  };

  const handleValidate = async () => {
    if (!parsedSchema) {
      alert("❌ Invalid schema");
      return;
    }
    const res = await axios.post(`/api/schema/validate`, {
      subject: topic,
      schema: parsedSchema
    });
    setCompatible(res.data.compatible);
    setIssues(res.data.issues);
  };

  return (
    <Paper sx={{ p: 4, maxWidth: 900, margin: "auto", mt: 4 }}>
      <Typography variant="h5" gutterBottom>Schema Validation for: {topic}</Typography>

      {latest && (
        <>
          <Typography variant="subtitle1">📦 Latest Registered Schema:</Typography>
          <TextField value={latest} multiline rows={6} fullWidth sx={{ mb: 3 }} InputProps={{ readOnly: true }} />
        </>
      )}

      <Typography variant="subtitle1">✏️ Proposed Schema (JSON):</Typography>
      <TextField
        value={proposedSchemaText}
        onChange={(e) => handleSchemaTextChange(e.target.value)}
        multiline rows={6}
        fullWidth sx={{ mb: 3 }}
        placeholder='{"type":"record","name":"User","fields":[{"name":"id","type":"string"}]}'
      />

      <Button variant="contained" onClick={handleValidate}>Validate</Button>

      {compatible !== null && (
        <>
          <Typography sx={{ mt: 3 }}>
            Compatibility: <strong>{compatible ? "✅ Compatible" : "❌ Not Compatible"}</strong>
          </Typography>

          {!compatible && (
            <Table sx={{ mt: 2 }}>
              <TableBody>
                {issues.map((issue, idx) => (
                  <TableRow key={idx}>
                    <TableCell>{issue.field}</TableCell>
                    <TableCell>{issue.issue}</TableCell>
                  </TableRow>
                ))}
              </TableBody>
            </Table>
          )}

          {compatible && (
            <Typography sx={{ mt: 3 }} color="success.main">
              ✅ Schema is compatible and ready for deployment.
            </Typography>
          )}
        </>
      )}
    </Paper>
  );
}
          
