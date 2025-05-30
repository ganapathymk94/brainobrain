import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.*;

import java.util.List;

@RestController
@RequestMapping("/api/kafka-search")
public class KafkaSearchController {

    @Autowired
    private KafkaSearchService kafkaSearchService;

    @GetMapping("/search")
    public List<String> searchMessages(
            @RequestParam String keyword,
            @RequestParam(defaultValue = "0") int offset,
            @RequestParam(defaultValue = "10") int limit) {
        return kafkaSearchService.searchMessages(keyword, offset, limit);
    }
}

_---------

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.springframework.stereotype.Service;

import java.time.Duration;
import java.util.*;

@Service
public class KafkaSearchService {
    private final String BOOTSTRAP_SERVERS = "localhost:9092";
    private final String TOPIC = "messages-topic";
    private final String GROUP_ID = "search-group";

    public List<String> searchMessages(String keyword, int offset, int limit) {
        Properties props = new Properties();
        props.put("bootstrap.servers", BOOTSTRAP_SERVERS);
        props.put("group.id", GROUP_ID);
        props.put("auto.offset.reset", "earliest");
        props.put("enable.auto.commit", "false");
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");

        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);
        TopicPartition partition = new TopicPartition(TOPIC, 0);
        consumer.assign(Collections.singletonList(partition));

        consumer.seek(partition, offset); // Start from specified offset
        ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(1000));

        List<String> matchingMessages = new ArrayList<>();
        int count = 0;

        for (ConsumerRecord<String, String> record : records) {
            if (boyerMooreSearch(record.value(), keyword)) {
                matchingMessages.add(record.value());
                count++;
            }
            if (count >= limit) break; // Stop when limit is reached
        }

        consumer.close();
        return matchingMessages;
    }

    // Boyer-Moore Algorithm for Fast String Matching
    private boolean boyerMooreSearch(String text, String pattern) {
        int[] badCharShift = new int[256];
        Arrays.fill(badCharShift, pattern.length());

        for (int i = 0; i < pattern.length() - 1; i++) {
            badCharShift[pattern.charAt(i)] = pattern.length() - 1 - i;
        }

        int index = 0;
        while (index <= text.length() - pattern.length()) {
            int j = pattern.length() - 1;
            while (j >= 0 && text.charAt(index + j) == pattern.charAt(j)) {
                j--;
            }

            if (j < 0) return true;
            index += badCharShift[text.charAt(index + pattern.length() - 1)];
        }

        return false;
    }
}

--------

    import React, { useState } from "react";
import axios from "axios";
import { Typography, Paper, Box, TextField, Button, List, ListItem } from "@mui/material";

const KafkaSearchDashboard = () => {
  const [keyword, setKeyword] = useState("");
  const [results, setResults] = useState([]);
  const [offset, setOffset] = useState(0);
  const limit = 10; // Number of messages to fetch per request

  const searchMessages = () => {
    if (!keyword) return;

    axios.get(`/api/kafka-search/search?keyword=${keyword}&offset=${offset}&limit=${limit}`)
      .then(response => {
        setResults(prev => [...prev, ...response.data]); // Append new results
        setOffset(prev => prev + limit); // Increment offset for next batch
      })
      .catch(error => console.error("Error searching Kafka messages:", error));
  };

  return (
    <Paper sx={{ padding: 3, width: 600, margin: "auto", mt: 5 }}>
      <Typography variant="h6">Kafka Message Search</Typography>

      <TextField
        fullWidth
        label="Enter Keyword"
        variant="outlined"
        value={keyword}
        onChange={(e) => setKeyword(e.target.value)}
        sx={{ mt: 2 }}
      />

      <Button variant="contained" color="primary" onClick={searchMessages} sx={{ mt: 2 }}>
        Search Kafka Messages
      </Button>

      {results.length > 0 && (
        <Box sx={{ mt: 3 }}>
          <List>
            {results.map((msg, index) => <ListItem key={index}>{msg}</ListItem>)}
          </List>
        </Box>
      )}

      {results.length > 0 && (
        <Button variant="outlined" color="secondary" onClick={searchMessages} sx={{ mt: 2 }}>
          Load More Messages
        </Button>
      )}
    </Paper>
  );
};

export default KafkaSearchDashboard;
