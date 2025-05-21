import React, { useState, useEffect } from "react";
import axios from "axios";
import avro from "avsc";
import { Button, TextField, Typography, Box, Paper, Alert } from "@mui/material";

const SchemaValidator = () => {
  const [schema, setSchema] = useState(null);
  const [jsonInput, setJsonInput] = useState("");
  const [validationErrors, setValidationErrors] = useState([]);

  // Fetch latest schema from Schema Registry API
  useEffect(() => {
    axios.get("http://localhost:8081/subjects/orders-value/versions/latest")
      .then(response => {
        const parsedSchema = JSON.parse(response.data.schema);
        setSchema(avro.Type.forSchema(parsedSchema)); // Convert schema to Avro type
      })
      .catch(error => console.error("Error fetching schema:", error));
  }, []);

  // Recursive validation for nested structures
  const validateJson = (jsonObj, schemaType, path = "") => {
    const errors = [];

    if (schemaType.type === "record") {
      schemaType.fields.forEach(field => {
        const fieldPath = path ? `${path}.${field.name}` : field.name;
        const jsonValue = jsonObj[field.name];

        if (jsonValue === undefined) {
          errors.push(`❌ Missing field: '${fieldPath}'`);
        } else {
          errors.push(...validateJson(jsonValue, field.type, fieldPath));
        }
      });
    } else if (schemaType.type === "array") {
      if (!Array.isArray(jsonObj)) {
        errors.push(`⚠️ Type mismatch: '${path}' should be an array`);
      } else {
        jsonObj.forEach((element, index) => {
          errors.push(...validateJson(element, schemaType.items, `${path}[${index}]`));
        });
      }
    } else if (schemaType.type === "map") {
      if (typeof jsonObj !== "object" || Array.isArray(jsonObj)) {
        errors.push(`⚠️ Type mismatch: '${path}' should be an object (map)`);
      } else {
        Object.entries(jsonObj).forEach(([key, value]) => {
          errors.push(...validateJson(value, schemaType.values, `${path}.${key}`));
        });
      }
    } else {
      const actualType = Array.isArray(jsonObj) ? "array" : typeof jsonObj;
      if (schemaType !== actualType) {
        errors.push(`⚠️ Type mismatch for '${path}': Expected '${schemaType}', got '${actualType}'`);
      }
    }

    return errors;
  };

  // Validate JSON input on button click
  const handleValidation = () => {
    if (!schema || !jsonInput) return;

    try {
      const jsonParsed = JSON.parse(jsonInput);
      const errors = validateJson(jsonParsed, schema);

      setValidationErrors(errors.length > 0 ? errors : ["✅ JSON is valid against schema!"]);
    } catch (e) {
      setValidationErrors(["❌ Invalid JSON format"]);
    }
  };

  return (
    <Paper sx={{ padding: 3, width: 600, margin: "auto", mt: 5 }}>
      <Typography variant="h6">Schema Validator</Typography>
      <Typography variant="subtitle2">Latest Schema Version: {schema?.name}</Typography>

      <TextField
        multiline
        rows={6}
        fullWidth
        label="Paste JSON Data Here"
        variant="outlined"
        value={jsonInput}
        onChange={(e) => setJsonInput(e.target.value)}
        sx={{ mt: 2 }}
      />

      <Button variant="contained" color="primary" onClick={handleValidation} sx={{ mt: 2 }}>
        Validate JSON
      </Button>

      {validationErrors.length > 0 && (
        <Box sx={{ mt: 2 }}>
          {validationErrors.map((error, index) => (
            <Alert key={index} severity={error.includes("❌") ? "error" : "warning"}>
              {error}
            </Alert>
          ))}
        </Box>
      )}
    </Paper>
  );
};

export default SchemaValidator;
