import React, { useEffect, useState } from "react";
import axios from "axios";
import { Line } from "react-chartjs-2";
import { Chart as ChartJS, CategoryScale, LinearScale, PointElement, LineElement, Tooltip, Legend } from "chart.js";

ChartJS.register(CategoryScale, LinearScale, PointElement, LineElement, Tooltip, Legend);

const KafkaHourlyRateChart = () => {
  const [topics, setTopics] = useState([]); // Topics from API
  const [selectedTopic, setSelectedTopic] = useState(""); // Selected topic
  const [data, setData] = useState([]); // Hourly message count from API

  // Fetch topics on component mount
  useEffect(() => {
    axios.get("/api/kafka-stats/topics")
      .then(response => setTopics(response.data))
      .catch(error => console.error("Error fetching topics:", error));
  }, []);

  // Fetch hourly message count when topic changes
  useEffect(() => {
    if (selectedTopic) {
      axios.get(`/api/kafka-stats/hourly-message-rate?topic=${selectedTopic}`)
        .then(response => setData(response.data))
        .catch(error => console.error("Error fetching hourly rates:", error));
    }
  }, [selectedTopic]);

  const chartData = {
    labels: data.map(entry => new Date(entry.insertedTime).toLocaleTimeString()),
    datasets: [
      {
        label: `Messages per Hour (${selectedTopic})`,
        data: data.map(entry => entry.messageCount),
        borderColor: "#42A5F5",
        backgroundColor: "rgba(66,165,245,0.2)",
        pointRadius: 5,
        tension: 0.4,
      },
    ],
  };

  const options = {
    responsive: true,
    maintainAspectRatio: false,
    plugins: {
      legend: { position: "top" },
      tooltip: { enabled: true, mode: "index", intersect: false },
    },
    scales: {
      x: { title: { display: true, text: "Time" } },
      y: { title: { display: true, text: "Messages Count" } },
    },
  };

  return (
    <div style={{ width: "600px", height: "400px", margin: "auto" }}>
      <div style={{ marginBottom: "10px", textAlign: "center" }}>
        <label>Topic:</label>
        <select value={selectedTopic} onChange={(e) => setSelectedTopic(e.target.value)}>
          <option value="" disabled>Select a topic</option>
          {topics.map(topic => <option key={topic} value={topic}>{topic}</option>)}
        </select>
      </div>
      <Line data={chartData} options={options} />
    </div>
  );
};

export default KafkaHourlyRateChart;
