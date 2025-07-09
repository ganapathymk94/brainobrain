package com.example.schemasync.entity;

import javax.persistence.*;
import java.time.LocalDateTime;

@Entity
@Table(name = "SchemaHistory")
public class SchemaHistory {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Integer id;

    private String subjectName;
    private Integer version;

    @Column(columnDefinition = "NVARCHAR(MAX)")
    private String schemaText;

    private Integer schemaId;

    @Column(columnDefinition = "NVARCHAR(MAX)")
    private String diffSummary;

    private LocalDateTime registeredAt = LocalDateTime.now();

    // Getters and Setters
    public Integer getId() { return id; }
    public void setId(Integer id) { this.id = id; }

    public String getSubjectName() { return subjectName; }
    public void setSubjectName(String subjectName) { this.subjectName = subjectName; }

    public Integer getVersion() { return version; }
    public void setVersion(Integer version) { this.version = version; }

    public String getSchemaText() { return schemaText; }
    public void setSchemaText(String schemaText) { this.schemaText = schemaText; }

    public Integer getSchemaId() { return schemaId; }
    public void setSchemaId(Integer schemaId) { this.schemaId = schemaId; }

    public String getDiffSummary() { return diffSummary; }
    public void setDiffSummary(String diffSummary) { this.diffSummary = diffSummary; }

    public LocalDateTime getRegisteredAt() { return registeredAt; }
    public void setRegisteredAt(LocalDateTime registeredAt) { this.registeredAt = registeredAt; }
}



package com.example.schemasync.repository;

import com.example.schemasync.entity.SchemaHistory;
import org.springframework.data.jpa.repository.JpaRepository;
import java.util.List;

public interface SchemaHistoryRepository extends JpaRepository<SchemaHistory, Integer> {
    List<SchemaHistory> findBySubjectNameOrderByVersionDesc(String subjectName);
}


package com.example.schemasync.util;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.util.*;

public class SchemaDiffUtil {

    private static final ObjectMapper mapper = new ObjectMapper();

    public static Set<String> extractFieldNames(String schemaText) throws Exception {
        JsonNode root = mapper.readTree(schemaText);
        JsonNode fields = root.get("fields");
        Set<String> fieldNames = new HashSet<>();
        if (fields != null && fields.isArray()) {
            for (JsonNode field : fields) {
                fieldNames.add(field.get("name").asText());
            }
        }
        return fieldNames;
    }

    public static String getFieldDiff(Set<String> oldFields, Set<String> newFields) {
        Set<String> added = new HashSet<>(newFields);
        added.removeAll(oldFields);

        Set<String> removed = new HashSet<>(oldFields);
        removed.removeAll(newFields);

        return "Added: " + added + ", Removed: " + removed;
    }
}


package com.example.schemasync.service;

import com.example.schemasync.entity.SchemaHistory;
import com.example.schemasync.repository.SchemaHistoryRepository;
import com.example.schemasync.util.SchemaDiffUtil;
import io.confluent.kafka.schemaregistry.client.CachedSchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.rest.entities.SchemaMetadata;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;

import java.util.*;

@Service
public class SchemaSyncService {

    @Autowired
    private SchemaHistoryRepository repository;

    @Value("${schema.registry.urls}")
    private String registryUrls;

    private SchemaRegistryClient getAvailableClient() {
        List<String> urls = Arrays.asList(registryUrls.split(","));
        for (String url : urls) {
            try {
                SchemaRegistryClient client = new CachedSchemaRegistryClient(url.trim(), 100);
                client.getAllSubjects(); // test connection
                return client;
            } catch (Exception e) {
                System.err.println("Failed to connect to Schema Registry at: " + url);
            }
        }
        throw new RuntimeException("All Schema Registry URLs are unreachable.");
    }

    @Scheduled(cron = "0 0 2 * * ?") // Daily at 2 AM
    public void syncSchemas() {
        SchemaRegistryClient client = getAvailableClient();
        try {
            List<String> subjects = client.getAllSubjects();
            for (String subject : subjects) {
                List<Integer> versions = client.getAllVersions(subject);
                List<SchemaHistory> existingVersions = repository.findBySubjectNameOrderByVersionDesc(subject);
                Set<String> previousFields = new HashSet<>();

                if (!existingVersions.isEmpty()) {
                    previousFields = SchemaDiffUtil.extractFieldNames(existingVersions.get(0).getSchemaText());
                }

                for (Integer version : versions) {
                    boolean exists = existingVersions.stream()
                        .anyMatch(s -> s.getVersion().equals(version));
                    if (!exists) {
                        SchemaMetadata metadata = client.getSchemaMetadata(subject, version);
                        Set<String> currentFields = SchemaDiffUtil.extractFieldNames(metadata.getSchema());
                        String diffSummary = SchemaDiffUtil.getFieldDiff(previousFields, currentFields);

                        SchemaHistory history = new SchemaHistory();
                        history.setSubjectName(subject);
                        history.setVersion(version);
                        history.setSchemaText(metadata.getSchema());
                        history.setSchemaId(metadata.getId());
                        history.setDiffSummary(diffSummary);
                        repository.save(history);

                        previousFields = currentFields;
                    }
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}

