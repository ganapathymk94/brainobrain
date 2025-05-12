@Query(value = "SELECT mc.env_name, mc.inserted_time, mc.topic_name, " +
               "mc.total_messages - COALESCE((SELECT MAX(prev.total_messages) " +
               "FROM message_count prev WHERE prev.env_name = mc.env_name " +
               "AND prev.topic_name = mc.topic_name AND prev.inserted_time < mc.inserted_time), 0) " +
               "AS hourly_message_count " +
               "FROM message_count mc ORDER BY mc.inserted_time ASC", nativeQuery = true)
List<Object[]> getHourlyMessageCountPerEnvTopic();

public Map<String, List<Map<String, Object>>> getHourlyMessageRate() {
    List<Object[]> results = messageCountRepository.getHourlyMessageCountPerCluster();
    Map<String, List<Map<String, Object>>> clusterHourlyRates = new HashMap<>();

    for (Object[] row : results) {
        String cluster = (String) row[0];
        LocalDateTime dateTime = (LocalDateTime) row[1];
        Long hourlyMessages = (Long) row[2];

        clusterHourlyRates.computeIfAbsent(cluster, k -> new ArrayList<>())
            .add(Map.of("dateTime", dateTime, "messageCount", hourlyMessages));
    }

    return clusterHourlyRates;
}


import React, { useEffect, useState } from "react";
import axios from "axios";
import { LineChart } from "@mui/x-charts";

const KafkaHourlyRateChart = ({ cluster }) => {
  const [data, setData] = useState([]);

  useEffect(() => {
    axios.get(`/api/kafka-stats/hourly-message-rate`)
      .then(response => {
        setData(response.data[cluster] || []);
      })
      .catch(error => console.error("Error fetching hourly rates:", error));
  }, [cluster]);

  const chartData = {
    labels: data.map(entry => new Date(entry.dateTime).toLocaleTimeString()),
    datasets: [
      {
        label: `Messages per Hour (Cluster ${cluster})`,
        data: data.map(entry => entry.messageCount),
        borderColor: "#42A5F5",
        backgroundColor: "rgba(66,165,245,0.2)",
      },
    ],
  };

  return (
    <LineChart
      xAxis={[{ scaleType: "band", data: chartData.labels }]}
      series={[{ data: chartData.datasets[0].data }]}
      width={800}
      height={400}
    />
  );
};

export default KafkaHourlyRateChart;

<KafkaHourlyRateChart cluster="A" />
