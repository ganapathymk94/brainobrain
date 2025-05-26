import React, { useState, useEffect } from "react";

const LogDashboard = () => {
  const [logs, setLogs] = useState([]);
  const [upHosts, setUpHosts] = useState([]);
  const [isRunning, setIsRunning] = useState(true);

  useEffect(() => {
    let interval;
    if (isRunning) {
      interval = setInterval(fetchLogs, 60000); // Refresh every 1 minute
    }
    return () => clearInterval(interval);
  }, [isRunning]);

  const fetchLogs = async () => {
    try {
      const response = await fetch("http://your-springboot-server:8080/api/logs");
      const data = await response.json();

      setLogs((prevLogs) => [...data, ...prevLogs].slice(0, 50)); // Rolling log effect
      setUpHosts(data.filter(host => host.HostStatus === "Up"));
    } catch (error) {
      console.error("Error fetching logs:", error);
    }
  };

  return (
    <div>
      <button onClick={() => setIsRunning(!isRunning)}>
        {isRunning ? "Stop Log" : "Resume Log"}
      </button>

      <div style={{ display: "flex", justifyContent: "space-between" }}>
        {/* Rolling Log Display */}
        <div style={{ width: "70%", height: "400px", overflow: "auto", border: "1px solid black" }}>
          {logs.map((log, index) => (
            <div key={index}>
              [{log.Down_at}] {log.Hostname} ({log.HostType}) - <b>{log.HostStatus}</b>
            </div>
          ))}
        </div>

        {/* Side Frame for Hosts Coming Up */}
        <div style={{ width: "25%", border: "1px solid green", padding: "10px" }}>
          <h3>Recovered Hosts</h3>
          {upHosts.map((host, index) => (
            <div key={index}>
              {host.Hostname} - {host.Total_downtime} sec downtime
            </div>
          ))}
        </div>
      </div>
    </div>
  );
};

export default LogDashboard;
