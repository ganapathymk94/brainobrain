public static void findStrayPartitionsForAllTopics() throws ExecutionException, InterruptedException {
    try (AdminClient adminClient = AdminClient.create(Map.of(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS))) {

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
                    
                    // Check if this is an observer (not in ISR)
                    boolean isObserver = !isr.contains(replica);

                    if (isObserver && lag > LAG_THRESHOLD) {
                        strayPartition = true;
                        System.out.println("⚠️ Stray Partition (Observer Lag): Partition " + partition.partition());
                        System.out.println("   Observer Replica ID: " + replica.id());
                        System.out.println("   Lag: " + lag);
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
