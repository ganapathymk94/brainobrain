import React, { useEffect, useState } from "react";
import axios from "axios";
import { Typography, Paper, Box, Alert, Table, TableBody, TableCell, TableContainer, TableHead, TableRow, TextField, Button } from "@mui/material";

const ConsumerLagDashboard = () => {
  const [topic, setTopic] = useState("");
  const [lagData, setLagData] = useState(null);

  const fetchLagData = () => {
    if (!topic) return;
    axios.get(`/api/kafka-lag/${topic}`)
      .then(response => setLagData(response.data))
      .catch(error => console.error("Error fetching consumer lag data:", error));
  };

  useEffect(() => {
    const interval = setInterval(fetchLagData, 5000);
    return () => clearInterval(interval);
  }, [topic]);

  return (
    <Paper sx={{ padding: 3, width: 800, margin: "auto", mt: 5 }}>
      <Typography variant="h6">Kafka Consumer Lag Dashboard</Typography>

      <TextField
        fullWidth
        label="Enter Kafka Topic"
        variant="outlined"
        value={topic}
        onChange={(e) => setTopic(e.target.value)}
        sx={{ mt: 2 }}
      />

      <Button variant="contained" color="primary" onClick={fetchLagData} sx={{ mt: 2 }}>
        Fetch Lag Data
      </Button>

      {!lagData ? <Alert severity="info">Enter a Kafka topic to fetch consumer lag data.</Alert> : (
        Object.entries(lagData).map(([consumerGroup, partitions]) => (
          <Box key={consumerGroup} sx={{ mt: 3 }}>
            <Typography variant="subtitle1">Consumer Group: {consumerGroup}</Typography>
            <TableContainer>
              <Table>
                <TableHead>
                  <TableRow>
                    <TableCell>Partition</TableCell>
                    <TableCell>Current Offset</TableCell>
                    <TableCell>Latest Offset</TableCell>
                    <TableCell>Lag</TableCell>
                    <TableCell>Predicted Lag</TableCell>
                  </TableRow>
                </TableHead>
                <TableBody>
                  {Object.entries(partitions).map(([partition, stats]) => (
                    <TableRow key={partition}>
                      <TableCell>{partition}</TableCell>
                      <TableCell>{stats.currentOffset}</TableCell>
                      <TableCell>{stats.endOffset}</TableCell>
                      <TableCell>
                        {stats.lag > 1000 ? <Alert severity="error">{stats.lag} (High Lag!)</Alert> : stats.lag}
                      </TableCell>
                      <TableCell>
                        {stats.predictedLag > 0 ? <Alert severity="warning">{stats.predictedLag} (Lag Increasing!)</Alert> : "Stable"}
                      </TableCell>
                    </TableRow>
                  ))}
                </TableBody>
              </Table>
            </TableContainer>
          </Box>
        ))
      )}
    </Paper>
  );
};

export default ConsumerLagDashboard;
