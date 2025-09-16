consumer.subscribe(Arrays.asList("abc"), new ConsumerRebalanceListener() {
    @Override
    public void onPartitionsRevoked(Collection<TopicPartition> partitions) {
        System.out.println("Rebalance started. Partitions revoked: " + partitions);
    }

    @Override
    public void onPartitionsAssigned(Collection<TopicPartition> partitions) {
        System.out.println("Rebalance completed. Partitions assigned: " + partitions);
        // Safe to start polling here
    }
});
