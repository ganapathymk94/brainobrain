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

package com.example.schemasync.service;

import com.example.schemasync.entity.KafkaTopicMetadata;
import com.example.schemasync.entity.KafkaSyncAudit;
import com.example.schemasync.repository.KafkaTopicMetadataRepository;
import com.example.schemasync.repository.KafkaSyncAuditRepository;
import com.example.schemasync.util.KafkaMetadataDiffUtil;
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

    private final Properties props;

    public KafkaMetadataService() {
        this.props = new Properties();
        props.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put("bootstrap.servers", "localhost:9092");
    }

    @Scheduled(cron = "0 0 0 * * ?")   // midnight
    @Scheduled(cron = "0 0 23 * * ?") // 11 PM
    public void collectTopicMetadata() {
        int topicsProcessed = 0;
        StringBuilder remarks = new StringBuilder();

        try (AdminClient admin = AdminClient.create(props);
             KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props, new StringDeserializer(), new StringDeserializer())) {

            Set<String> topicNames = admin.listTopics().names().get();
            Map<String, TopicDescription> descriptions = admin.describeTopics(topicNames).all().get();

            for (String topic : topicNames) {
                TopicDescription td = descriptions.get(topic);
                int partitions = td.partitions().size();
                int replicationFactor = td.partitions().get(0).replicas().size();
                int isr = td.partitions().get(0).isr().size();

                // Message count estimation
                long msgCount = 0;
                for (PartitionInfo p : consumer.partitionsFor(topic)) {
                    TopicPartition tp = new TopicPartition(topic, p.partition());
                    consumer.assign(Collections.singleton(tp));
                    consumer.seekToBeginning(Collections.singleton(tp));
                    long startOffset = consumer.position(tp);
                    consumer.seekToEnd(Collections.singleton(tp));
                    long endOffset = consumer.position(tp);
                    msgCount += (endOffset - startOffset);
                }

                // Consumer group tracking
                List<String> consumers = new ArrayList<>();
                for (ConsumerGroupListing cg : admin.listConsumerGroups().all().get()) {
                    ConsumerGroupDescription desc = admin.describeConsumerGroups(Collections.singletonList(cg.groupId()))
                                                         .all().get().get(cg.groupId());
                    for (MemberDescription member : desc.members()) {
                        member.assignment().topicPartitions().forEach(tp -> {
                            if (tp.topic().equals(topic)) consumers.add(cg.groupId());
                        });
                    }
                }

                // Topic config
                ConfigResource resource = new ConfigResource(ConfigResource.Type.TOPIC, topic);
                Config config = admin.describeConfigs(Collections.singletonList(resource))
                                     .all().get().get(resource);

                KafkaTopicMetadata metadata = new KafkaTopicMetadata();
                metadata.setTopicName(topic);
                metadata.setPartitions(partitions);
                metadata.setCompressionType(valueOrDefault(config, "compression.type"));
                metadata.setCleanupPolicy(valueOrDefault(config, "cleanup.policy"));
                metadata.setMessageFormatVersion(valueOrDefault(config, "message.format.version"));
                metadata.setRetentionMs(longOrNull(config, "retention.ms"));
                metadata.setRetentionBytes(longOrNull(config, "retention.bytes"));
                metadata.setMaxMessageBytes(intOrNull(config, "max.message.bytes"));
                metadata.setMinInsyncReplicas(intOrNull(config, "min.insync.replicas"));

                metadata.setReplicationFactor(replicationFactor);
                metadata.setInsyncReplicas(isr);
                metadata.setMessagesToday((int) msgCount);
                metadata.setProducerIds("TBD");
                metadata.setConsumerGroups(String.join(",", new HashSet<>(consumers)));

                KafkaTopicMetadata previous = metadataRepo.findTopByTopicNameOrderByCollectedAtDesc(topic);

                if (KafkaMetadataDiffUtil.isMetadataChanged(previous, metadata)) {
                    metadataRepo.save(metadata);
                    topicsProcessed++;
                }
            }

            remarks.append("Metadata sync completed with ").append(topicsProcessed).append(" changes.");
        } catch (Exception e) {
            remarks.append("Metadata sync failed: ").append(e.getMessage());
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

    private String valueOrDefault(Config config, String key) {
        ConfigEntry entry = config.get(key);
        return entry != null ? entry.value() : "unknown";
    }

    private Long longOrNull(Config config, String key) {
        try {
            ConfigEntry entry = config.get(key);
            return entry != null ? Long.parseLong(entry.value()) : null;
        } catch (Exception e) {
            return null;
        }
    }

    private Integer intOrNull(Config config, String key) {
        try {
            ConfigEntry entry = config.get(key);
            return entry != null ? Integer.parseInt(entry.value()) : null;
        } catch (Exception e) {
            return null;
        }
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
    private Integer partitions;
    private String compressionType;
    private Integer minInsyncReplicas;
    private String cleanupPolicy;
    private Long retentionMs;
    private String messageFormatVersion;
    private Integer maxMessageBytes;
    private Long retentionBytes;

    private Integer messagesToday;
    private Integer replicationFactor;
    private Integer insyncReplicas;

    @Column(columnDefinition = "NVARCHAR(MAX)")
    private String producerIds;

    @Column(columnDefinition = "NVARCHAR(MAX)")
    private String consumerGroups;

    private LocalDateTime collectedAt = LocalDateTime.now();

    // Getters and setters...
}


package com.example.schemasync.util;

import com.example.schemasync.entity.KafkaTopicMetadata;

import java.util.Objects;

public class KafkaMetadataDiffUtil {

    public static boolean isMetadataChanged(KafkaTopicMetadata old, KafkaTopicMetadata current) {
        if (old == null) return true;
        return !Objects.equals(old.getPartitions(), current.getPartitions()) ||
               !Objects.equals(old.getCompressionType(), current.getCompressionType()) ||
               !Objects.equals(old.getMinInsyncReplicas(), current.getMinInsyncReplicas()) ||
               !Objects.equals(old.getCleanupPolicy(), current.getCleanupPolicy()) ||
               !Objects.equals(old.getRetentionMs(), current.getRetentionMs()) ||
               !Objects.equals(old.getMessageFormatVersion(), current.getMessageFormatVersion()) ||
               !Objects.equals(old.getMaxMessageBytes(), current.getMaxMessageBytes()) ||
               !Objects.equals(old.getRetentionBytes(), current.getRetentionBytes()) ||
               !Objects.equals(old.getReplicationFactor(), current.getReplicationFactor()) ||
               !Objects.equals(old.getInsyncReplicas(), current.getInsyncReplicas()) ||
               !Objects.equals(old.getMessagesToday(), current.getMessagesToday()) ||
               !Objects.equals(old.getConsumerGroups(), current.getConsumerGroups());
    }
}


private String valueOrDefault(Config config, String key) {
        ConfigEntry entry = config.get(key);
        return entry != null ? entry.value() : "unknown";
    }

    private Long longOrNull(Config config, String key) {
        try {
            ConfigEntry entry = config.get(key);
            return entry != null ? Long.parseLong(entry.value()) : null;
        } catch (Exception e) {
            return null;
        }
    }

    private Integer intOrNull(Config config, String key) {
        try {
            ConfigEntry entry = config.get(key);
            return entry != null ? Integer.parseInt(entry.value()) : null;
        } catch (Exception e) {
            return null;
        }
    }
