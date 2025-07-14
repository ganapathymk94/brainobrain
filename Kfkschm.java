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




import { useState } from "react";
import { TextField, Button, Typography, MenuItem, Table, TableBody, TableRow, TableCell, Paper } from "@mui/material";
import axios from "axios";

const topics = ["order-event", "user-event"]; // You can fetch this via API too

export default function SchemaValidatorPage() {
  const [selected, setSelected] = useState("");
  const [latest, setLatest] = useState("");
  const [newSchema, setNewSchema] = useState("");
  const [issues, setIssues] = useState([]);
  const [compatible, setCompatible] = useState(null);

  const fetchSchema = async () => {
    const res = await axios.get(`/api/schema/latest/${selected}`);
    setLatest(res.data);
  };

  const handleValidate = async () => {
    const res = await axios.post(`/api/schema/validate`, { subject: selected, schema: newSchema });
    setCompatible(res.data.compatible);
    setIssues(res.data.issues);
  };

  const handlePush = async () => {
    await axios.post(`/api/schema/push`, { subject: selected, schema: newSchema });
    alert("✅ Pushed to Bitbucket!");
  };

  return (
    <Paper sx={{ p: 4 }}>
      <Typography variant="h5">Schema Validation Portal</Typography>

      <TextField
        select fullWidth label="Choose Topic" value={selected}
        onChange={e => { setSelected(e.target.value); fetchSchema(); }}
        sx={{ my: 2 }}
      >
        {topics.map(t => <MenuItem key={t} value={t}>{t}</MenuItem>)}
      </TextField>

      {latest && (
        <>
          <Typography variant="subtitle1">Latest Registered Schema:</Typography>
          <TextField value={latest} multiline rows={6} fullWidth sx={{ mb: 2 }} />
        </>
      )}

      <Typography variant="subtitle1">Proposed Schema:</Typography>
      <TextField
        value={newSchema} multiline rows={6} fullWidth sx={{ mb: 2 }}
        onChange={e => setNewSchema(e.target.value)}
      />

      <Button variant="contained" onClick={handleValidate}>Validate</Button>

      {compatible !== null && (
        <>
          <Typography sx={{ mt: 2 }}>
            Compatibility: {compatible ? "✅ Compatible" : "❌ Not Compatible"}
          </Typography>

          {!compatible && (
            <Table sx={{ mt: 2 }}>
              <TableBody>
                {issues.map((i, idx) => (
                  <TableRow key={idx}>
                    <TableCell>{i.field}</TableCell>
                    <TableCell>{i.issue}</TableCell>
                  </TableRow>
                ))}
              </TableBody>
            </Table>
          )}

          {compatible && (
            <Button variant="outlined" color="success" sx={{ mt: 2 }} onClick={handlePush}>
              Push to Bitbucket
            </Button>
          )}
        </>
      )}
    </Paper>
  );
}
