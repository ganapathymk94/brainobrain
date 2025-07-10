@RestController
@RequestMapping("/api/audits")
public class AuditController {

  @Autowired
  private KafkaSyncAuditRepository auditRepository;

  @GetMapping("/")
  public List<KafkaSyncAudit> getAllAudits() {
    return auditRepository.findAll();
  }
}

package com.example.schemasync.repository;

import com.example.schemasync.entity.KafkaTopicMetadata;
import org.springframework.data.jpa.repository.JpaRepository;
import java.util.List;

public interface KafkaTopicMetadataRepository extends JpaRepository<KafkaTopicMetadata, Integer> {
    List<KafkaTopicMetadata> findByTopicNameOrderByCollectedAtDesc(String topicName);
    KafkaTopicMetadata findTopByOrderByCollectedAtDesc();
}

package com.example.schemasync.service;

import com.example.schemasync.entity.SchemaHistory;
import com.example.schemasync.entity.KafkaSyncAudit;
import com.example.schemasync.repository.SchemaHistoryRepository;
import com.example.schemasync.repository.KafkaSyncAuditRepository;
import com.example.schemasync.util.SchemaDiffUtil;
import io.confluent.kafka.schemaregistry.client.*;
import io.confluent.kafka.schemaregistry.client.rest.entities.SchemaMetadata;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;

import java.util.*;

@Service
public class SchemaSyncService {

    @Autowired
    private SchemaHistoryRepository historyRepo;

    @Autowired
    private KafkaSyncAuditRepository auditRepo;

    @Value("${schema.registry.urls}")
    private String registryUrls;

    private SchemaRegistryClient getAvailableClient() {
        List<String> urls = Arrays.asList(registryUrls.split(","));
        for (String url : urls) {
            try {
                SchemaRegistryClient client = new CachedSchemaRegistryClient(url.trim(), 100);
                client.getAllSubjects();
                return client;
            } catch (Exception e) {
                System.err.println("Registry unreachable at " + url);
            }
        }
        throw new RuntimeException("All Schema Registry URLs failed.");
    }

    @Scheduled(cron = "0 0 2 * * ?") // Daily at 2 AM
    public void syncSchemas() {
        SchemaRegistryClient client = getAvailableClient();
        int subjectsScanned = 0, totalChecked = 0, totalAdded = 0;
        StringBuilder remarks = new StringBuilder();

        try {
            List<String> subjects = client.getAllSubjects();
            subjectsScanned = subjects.size();

            for (String subject : subjects) {
                List<Integer> versions = client.getAllVersions(subject);
                totalChecked += versions.size();
                List<SchemaHistory> existing = historyRepo.findBySubjectNameOrderByVersionDesc(subject);
                Set<String> previousFields = existing.isEmpty() ? new HashSet<>() :
                    SchemaDiffUtil.extractFieldNames(existing.get(0).getSchemaText());

                for (Integer version : versions) {
                    boolean exists = existing.stream().anyMatch(s -> s.getVersion().equals(version));
                    if (!exists) {
                        SchemaMetadata meta = client.getSchemaMetadata(subject, version);
                        Set<String> currentFields = SchemaDiffUtil.extractFieldNames(meta.getSchema());
                        String diffSummary = SchemaDiffUtil.getFieldDiff(previousFields, currentFields);

                        SchemaHistory history = new SchemaHistory();
                        history.setSubjectName(subject);
                        history.setVersion(version);
                        history.setSchemaText(meta.getSchema());
                        history.setSchemaId(meta.getId());
                        history.setDiffSummary(diffSummary);
                        historyRepo.save(history);

                        totalAdded++;
                        previousFields = currentFields;
                    }
                }
            }
            remarks.append("Schema sync successful.");
        } catch (Exception e) {
            remarks.append("Schema sync failed: ").append(e.getMessage());
            e.printStackTrace();
        }

        KafkaSyncAudit audit = new KafkaSyncAudit();
        audit.setJobType("schema-sync");
        audit.setSyncTimestamp(LocalDateTime.now());
        audit.setSubjectsScanned(subjectsScanned);
        audit.setTotalVersionsChecked(totalChecked);
        audit.setTotalVersionsAdded(totalAdded);
        audit.setRemarks(remarks.toString());
        auditRepo.save(audit);
    }
}



package com.example.schemasync.service;

import com.example.schemasync.entity.KafkaTopicMetadata;
import com.example.schemasync.entity.KafkaSyncAudit;
import com.example.schemasync.repository.KafkaTopicMetadataRepository;
import com.example.schemasync.repository.KafkaSyncAuditRepository;
import org.apache.kafka.clients.admin.*;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.*;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;

import java.time.LocalDateTime;
import java.util.*;

@Service
public class KafkaMetadataService {

    @Autowired
    private KafkaTopicMetadataRepository metadataRepo;

    @Autowired
    private KafkaSyncAuditRepository auditRepo;

    private final Properties kafkaProps;

    public KafkaMetadataService() {
        this.kafkaProps = new Properties();
        kafkaProps.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        kafkaProps.put("bootstrap.servers", "localhost:9092");
    }

    @Scheduled(cron = "0 0 0 * * ?")   // At midnight
    @Scheduled(cron = "0 0 23 * * ?") // At 11 PM
    public void collectMetadata() {
        int topicsProcessed = 0;
        StringBuilder remarks = new StringBuilder();

        try (AdminClient admin = AdminClient.create(kafkaProps);
             KafkaConsumer<String, String> consumer = new KafkaConsumer<>(kafkaProps, new StringDeserializer(), new StringDeserializer())) {

            Set<String> topicNames = admin.listTopics().names().get();
            Map<String, TopicDescription> descriptions = admin.describeTopics(topicNames).all().get();

            for (String topic : topicNames) {
                TopicDescription td = descriptions.get(topic);
                int replicationFactor = td.partitions().get(0).replicas().size();
                int isr = td.partitions().get(0).isr().size();
                String messageType = topic.contains("-avro") ? "AVRO" : "JSON";

                // Estimate message count
                long messageCount = 0;
                List<PartitionInfo> partitions = consumer.partitionsFor(topic);
                for (PartitionInfo p : partitions) {
                    TopicPartition tp = new TopicPartition(topic, p.partition());
                    consumer.assign(Collections.singletonList(tp));
                    consumer.seekToBeginning(Collections.singletonList(tp));
                    long startOffset = consumer.position(tp);
                    consumer.seekToEnd(Collections.singletonList(tp));
                    long endOffset = consumer.position(tp);
                    messageCount += (endOffset - startOffset);
                }

                // Find consumers
                List<String> consumers = new ArrayList<>();
                for (ConsumerGroupListing cg : admin.listConsumerGroups().all().get()) {
                    ConsumerGroupDescription cgDesc = admin.describeConsumerGroups(Collections.singletonList(cg.groupId())).all().get().get(cg.groupId());
                    for (MemberDescription member : cgDesc.members()) {
                        member.assignment().topicPartitions().forEach(tp -> {
                            if (tp.topic().equals(topic)) consumers.add(cg.groupId());
                        });
                    }
                }

                KafkaTopicMetadata meta = new KafkaTopicMetadata();
                meta.setTopicName(topic);
                meta.setMessageType(messageType);
                meta.setMessagesToday((int) messageCount);
                meta.setReplicationFactor(replicationFactor);
                meta.setInsyncReplicas(isr);
                meta.setProducerIds("TBD");
                meta.setConsumerGroups(String.join(",", new HashSet<>(consumers)));

                metadataRepo.save(meta);
                topicsProcessed++;
            }

            remarks.append("Topic metadata collection successful.");
        } catch (Exception e) {
            remarks.append("Failed to collect metadata: ").append(e.getMessage());
            e.printStackTrace();
        }

        KafkaSyncAudit audit = new KafkaSyncAudit();
        audit.setJobType("topic-metadata-sync");
        audit.setSyncTimestamp(LocalDateTime.now());
        audit.setSubjectsScanned(topicsProcessed);
        audit.setTotalVersionsChecked(0);
        audit.setTotalVersionsAdded(0);
        audit.setRemarks(remarks.toString());
        auditRepo.save(audit);
    }
}


package com.example.schemasync.repository;

import com.example.schemasync.entity.KafkaTopicMetadata;
import org.springframework.data.jpa.repository.JpaRepository;
import java.util.List;

public interface KafkaTopicMetadataRepository extends JpaRepository<KafkaTopicMetadata, Integer> {
    List<KafkaTopicMetadata> findByTopicNameOrderByCollectedAtDesc(String topicName);
    KafkaTopicMetadata findTopByOrderByCollectedAtDesc();
}


package com.example.schemasync.entity;

import javax.persistence.*;
import java.time.LocalDateTime;

@Entity
@Table(name = "KafkaTopicMetadata")
public class KafkaTopicMetadata {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Integer id;

    private String topicName;
    private String messageType;
    private Integer messagesToday;
    private Integer replicationFactor;
    private Integer insyncReplicas;

    @Column(columnDefinition = "NVARCHAR(MAX)")
    private String producerIds;

    @Column(columnDefinition = "NVARCHAR(MAX)")
    private String consumerGroups;

    private LocalDateTime collectedAt = LocalDateTime.now();

    // Getters and Setters...
}



