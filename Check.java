import org.apache.kafka.clients.admin.*;
import org.apache.kafka.common.TopicPartitionInfo;
import org.apache.kafka.common.Node;

import java.util.*;
import java.util.concurrent.ExecutionException;

public class KafkaStrayPartitionChecker {

    private static final String BOOTSTRAP_SERVERS = "localhost:9092";
    private static final long LAG_THRESHOLD = 5000; // Adjust based on cluster needs

    public static void main(String[] args) throws ExecutionException, InterruptedException {
        findStrayPartitionsForAllTopics();
    }

    public static void findStrayPartitionsForAllTopics() throws ExecutionException, InterruptedException {
        try (AdminClient adminClient = AdminClient.create(Map.of(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS))) {

            // Fetch all topics
            List<String> topics = new ArrayList<>(adminClient.listTopics().names().get());

            System.out.println("🔎 Checking stray partitions across all topics...");

            for (String topic : topics) {
                System.out.println("\n🧐 Inspecting topic: " + topic);

                DescribeTopicsResult describeTopicsResult = adminClient.describeTopics(Collections.singletonList(topic));
                TopicDescription topicDescription = describeTopicsResult.all().get().get(topic);

                for (TopicPartitionInfo partition : topicDescription.partitions()) {
                    List<Node> replicas = partition.replicas();
                    List<Node> isr = partition.isr();
                    Node leader = partition.leader();

                    // Fetch lag per replica
                    Map<Node, Long> replicaLag = getReplicaLag(adminClient, topic, partition.partition());

                    boolean strayPartition = false;
                    for (Node replica : replicas) {
                        long lag = replicaLag.getOrDefault(replica, 0L);
                        if (lag > LAG_THRESHOLD) {
                            strayPartition = true;
                            System.out.println("⚠️ Stray Partition Found: Partition " + partition.partition());
                            System.out.println("   Leader: " + leader.id());
                            System.out.println("   Replica Lag: " + lag);
                        }
                    }

                    if (!strayPartition) {
                        System.out.println("✅ Partition " + partition.partition() + " is healthy.");
                    }
                }
            }

            System.out.println("\n✅ Stray partition check completed.");
        }
    }

    private static Map<Node, Long> getReplicaLag(AdminClient adminClient, String topic, int partition) throws ExecutionException, InterruptedException {
        Map<Node, Long> lagMap = new HashMap<>();

        TopicPartition tp = new TopicPartition(topic, partition);
        Map<TopicPartition, OffsetSpec> request = Collections.singletonMap(tp, OffsetSpec.latest());
        Map<TopicPartition, ListOffsetsResult.ListOffsetsResultInfo> offsets = adminClient.listOffsets(request).all().get();

        long latestOffset = offsets.get(tp).offset();

        DescribeTopicsResult describeTopicsResult = adminClient.describeTopics(Collections.singletonList(topic));
        TopicDescription topicDescription = describeTopicsResult.all().get().get(topic);

        for (Node replica : topicDescription.partitions().get(partition).replicas()) {
            long replicaOffset = adminClient.listOffsets(Collections.singletonMap(new TopicPartition(topic, partition), OffsetSpec.latest())).all().get().get(tp).offset();
            lagMap.put(replica, latestOffset - replicaOffset);
        }

        return lagMap;
    }
}
