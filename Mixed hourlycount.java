SELECT mc.env_name, mc.inserted_time, mc.topic_name, 
CAST(mc.total_messages AS BIGINT) -
COALESCE((SELECT CAST(MAX(prev.total_messages) AS BIGINT) 
          FROM message_count prev 
          WHERE prev.env_name = mc.env_name 
          AND prev.topic_name = mc.topic_name 
          AND prev.inserted_time < mc.inserted_time), 0) 
AS hourly_message_count 
FROM message_count mc 
ORDER BY mc.inserted_time ASC;
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

import React from "react";
import { Line } from "react-chartjs-2";
import { Chart as ChartJS, CategoryScale, LinearScale, PointElement, LineElement, Tooltip, Legend } from "chart.js";

// Register required components for Chart.js
ChartJS.register(CategoryScale, LinearScale, PointElement, LineElement, Tooltip, Legend);

const KafkaHourlyRateChart = () => {
  const data = [
    { dateTime: "2025-05-12T00:00:00Z", messageCount: 1200 },
    { dateTime: "2025-05-12T01:00:00Z", messageCount: 1300 },
    { dateTime: "2025-05-12T02:00:00Z", messageCount: 1100 },
    { dateTime: "2025-05-12T03:00:00Z", messageCount: 1400 },
    { dateTime: "2025-05-12T04:00:00Z", messageCount: 1250 },
  ];

  const chartData = {
    labels: data.map(entry => new Date(entry.dateTime).toLocaleTimeString()),
    datasets: [
      {
        label: "Messages per Hour (Static Data)",
        data: data.map(entry => entry.messageCount),
        borderColor: "#42A5F5",
        backgroundColor: "rgba(66,165,245,0.2)",
        pointRadius: 5,
        tension: 0.4, // Smooth curve
      },
    ],
  };

  const options = {
    responsive: true,
    maintainAspectRatio: false, // Allow size control
    plugins: {
      legend: { position: "top" },
      tooltip: { enabled: true },
    },
    scales: {
      x: { title: { display: true, text: "Time" } },
      y: { title: { display: true, text: "Messages Count" } },
    },
  };

  return (
    <div style={{ width: "600px", height: "400px", margin: "auto" }}>
      <Line data={chartData} options={options} />
    </div>
  );
};

export default KafkaHourlyRateChart;
