import org.apache.kafka.clients.admin.*;
import org.apache.kafka.common.TopicPartition;
import org.springframework.stereotype.Service;

import java.util.*;
import java.util.concurrent.ExecutionException;

@Service
public class ConsumerLagService {
    private final String BOOTSTRAP_SERVERS = "localhost:9092";
    private final Map<String, List<Long>> lagHistory = new HashMap<>();

    public Map<String, Map<String, Object>> getConsumerLagForTopic(String topic) {
        Map<String, Map<String, Object>> lagData = new HashMap<>();

        try (AdminClient adminClient = AdminClient.create(Map.of(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS))) {
            List<String> consumerGroups = adminClient.listConsumerGroups()
                .valid()
                .get()
                .stream()
                .map(ConsumerGroupListing::groupId)
                .toList();

            Map<TopicPartition, Long> endOffsets = new HashMap<>();

            adminClient.describeTopics(Collections.singleton(topic))
                .values().get(topic).get().partitions()
                .forEach(p -> {
                    TopicPartition tp = new TopicPartition(topic, p.partition());
                    try {
                        endOffsets.put(tp, adminClient.listOffsets(Map.of(tp, OffsetSpec.latest())).all().get().get(tp).offset());
                    } catch (InterruptedException | ExecutionException e) {
                        e.printStackTrace();
                    }
                });

            for (String consumerGroup : consumerGroups) {
                Map<TopicPartition, Long> currentOffsets = new HashMap<>();
                List<String> activeConsumers = new ArrayList<>();

                ConsumerGroupDescription groupDescription = adminClient.describeConsumerGroups(Collections.singletonList(consumerGroup))
                    .describedGroups().get(consumerGroup).get();

                groupDescription.members().forEach(member -> activeConsumers.add(member.consumerId() + " (" + member.host() + ")"));

                adminClient.listConsumerGroupOffsets(consumerGroup)
                    .partitionsToOffsetAndMetadata().get()
                    .forEach((tp, offsetMeta) -> {
                        if (tp.topic().equals(topic)) {
                            currentOffsets.put(tp, offsetMeta.offset());
                        }
                    });

                for (TopicPartition tp : currentOffsets.keySet()) {
                    long lag = endOffsets.get(tp) - currentOffsets.get(tp);

                    // Store historical lag data for predictions
                    lagHistory.computeIfAbsent(tp.toString(), k -> new ArrayList<>()).add(lag);
                    long predictedLag = predictLag(tp.toString());

                    lagData.computeIfAbsent(consumerGroup, k -> new HashMap<>()).put(tp.toString(), Map.of(
                        "activeConsumers", activeConsumers,
                        "currentOffset", currentOffsets.get(tp),
                        "endOffset", endOffsets.get(tp),
                        "lag", lag,
                        "predictedLag", predictedLag
                    ));
                }
            }
        } catch (InterruptedException | ExecutionException e) {
            e.printStackTrace();
        }

        return lagData;
    }

    private long predictLag(String partition) {
        List<Long> history = lagHistory.getOrDefault(partition, new ArrayList<>());
        if (history.size() < 5) return 0; // Not enough data to predict

        long movingAverage = history.subList(Math.max(history.size() - 5, 0), history.size()).stream()
                .mapToLong(Long::longValue).sum() / 5;
        
        return movingAverage > 1000 ? movingAverage : 0; // Warning if lag is growing
    }
}
