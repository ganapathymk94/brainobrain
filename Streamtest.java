StreamsBuilder builder = new StreamsBuilder();

// Subscribe to all 250 topics (wildcard)
KStream<String, String> inputStream = builder.stream(Pattern.compile(".*"));

// Enrich with sourceTopic (namespace)
KStream<String, String> enrichedStream = inputStream.transformValues(() -> new ValueTransformerWithKey<String, String, String>() {
    private ProcessorContext context;
    @Override
    public void init(ProcessorContext context) { this.context = context; }
    @Override
    public String transform(String readOnlyKey, String value) {
        try {
            ObjectMapper mapper = new ObjectMapper();
            JsonNode json = mapper.readTree(value);
            String traceId = json.get("traceId").asText();
            String payloadHash = DigestUtils.sha256Hex(value);
            String sourceTopic = context.topic(); // capture origin topic

            return String.format(
                "{\"traceId\":\"%s\",\"payloadHash\":\"%s\",\"sourceTopic\":\"%s\",\"timestamp\":%d,\"status\":\"SUCCESS\"}",
                traceId, payloadHash, sourceTopic, System.currentTimeMillis()
            );
        } catch (Exception e) {
            return "{\"status\":\"FAILED\"}";
        }
    }
});

// Repartition into fixed partition count (e.g., 24)
KStream<String, String> repartitioned = enrichedStream
    .selectKey((key, value) -> {
        try {
            JsonNode json = new ObjectMapper().readTree(value);
            return json.get("traceId").asText(); // key by traceId
        } catch (Exception e) {
            return "error";
        }
    })
    .repartition(Repartitioned.with(Serdes.String(), Serdes.String()).withNumberOfPartitions(24));

// Materialize state store
KTable<String, String> reconciliationTable = repartitioned
    .toTable(Materialized.<String, String, KeyValueStore<Bytes, byte[]>>as("reconciliation-store")
        .withKeySerde(Serdes.String())
        .withValueSerde(Serdes.String()));

// Forward to audit topic
reconciliationTable.toStream().to("reconciliation.audit",
    Produced.with(Serdes.String(), Serdes.String()));
