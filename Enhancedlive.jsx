import React, { useState, useEffect } from "react";
import { Container, Button, Table, TableHead, TableRow, TableCell, TableBody, Chip } from "@mui/material";

const LogDashboard = () => {
  const [hosts, setHosts] = useState([]);
  const [isRunning, setIsRunning] = useState(true);

  useEffect(() => {
    let interval;
    if (isRunning) {
      interval = setInterval(fetchHosts, 60000); // Refresh every 1 minute
    }
    return () => clearInterval(interval);
  }, [isRunning]);

  const fetchHosts = async () => {
    try {
      const response = await fetch("http://your-springboot-server:8080/api/host-status");
      const data = await response.json();
      setHosts(data);
    } catch (error) {
      console.error("Error fetching host statuses:", error);
    }
  };

  return (
    <Container>
      <Button variant="contained" color={isRunning ? "error" : "success"} onClick={() => setIsRunning(!isRunning)}>
        {isRunning ? "Pause Monitoring" : "Resume Monitoring"}
      </Button>

      <Table>
        <TableHead>
          <TableRow>
            <TableCell><b>Hostname</b></TableCell>
            <TableCell><b>Port</b></TableCell>
            <TableCell><b>Status</b></TableCell>
            <TableCell><b>Last Down</b></TableCell>
            <TableCell><b>Last Up</b></TableCell>
            <TableCell><b>Downtime</b></TableCell>
          </TableRow>
        </TableHead>
        <TableBody>
          {hosts.map((host, index) => (
            <TableRow key={index}>
              <TableCell>{host.Hostname}</TableCell>
              <TableCell>{host.Port}</TableCell>
              <TableCell>
                <Chip label={host.HostStatus} color={host.HostStatus === "Up" ? "success" : "error"} />
              </TableCell>
              <TableCell>{host.Down_at || "N/A"}</TableCell>
              <TableCell>{host.Up_at || "N/A"}</TableCell>
              <TableCell>{host.Total_downtime ? `${host.Total_downtime} sec` : "Calculating..."}</TableCell>
            </TableRow>
          ))}
        </TableBody>
      </Table>
    </Container>
  );
};

export default LogDashboard;
