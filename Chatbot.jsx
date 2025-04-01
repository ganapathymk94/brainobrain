import React, { useState } from "react";
import { Box, TextField, Button, Typography } from "@mui/material";

const Chatbot = () => {
  const [messages, setMessages] = useState([]);
  const [input, setInput] = useState("");
  const [state, setState] = useState(null); // Tracks chatbot conversation state
  const [topicName, setTopicName] = useState("");
  const [partitions, setPartitions] = useState("");
  const [userGroup, setUserGroup] = useState("");

  const handleSendMessage = async () => {
    let userMessage = { sender: "User", text: input };
    setMessages([...messages, userMessage]);

    if (state === "waiting_topic_name") {
      setTopicName(input);
      setState("waiting_partitions");
      setMessages([...messages, userMessage, { sender: "Bot", text: "How many partitions should this topic have?" }]);
    } else if (state === "waiting_partitions") {
      setPartitions(input);
      setState("waiting_user_group");
      setMessages([...messages, userMessage, { sender: "Bot", text: "Which user group should be granted access?" }]);
    } else if (state === "waiting_user_group") {
      setUserGroup(input);
      setState(null); // Reset the conversation flow
      const response = await fetch("/api/chatbot/create_topic", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ topic: topicName, partitions, userGroup }),
      });

      const botResponse = await response.json();
      setMessages([...messages, userMessage, { sender: "Bot", text: botResponse.message }]);
    } else {
      setState("waiting_topic_name");
      setMessages([...messages, userMessage, { sender: "Bot", text: "What is the topic name?" }]);
    }

    setInput("");
  };

  return (
    <Box sx={{ width: 400, padding: 2, border: "1px solid #ccc" }}>
      {messages.map((msg, index) => (
        <Typography key={index} sx={{ fontWeight: msg.sender === "Bot" ? "bold" : "normal" }}>
          {msg.sender}: {msg.text}
        </Typography>
      ))}
      <TextField value={input} onChange={(e) => setInput(e.target.value)} fullWidth />
      <Button onClick={handleSendMessage} variant="contained">Send</Button>
    </Box>
  );
};

export default Chatbot;

if (state === "waiting_schema_action") {
    if (input.toLowerCase().includes("register schema")) {
        setState("waiting_schema_name");
        setMessages([...messages, userMessage, { sender: "Bot", text: "Enter schema name:" }]);
    } else if (input.toLowerCase().includes("view schema versions")) {
        setState("waiting_schema_name");
        setMessages([...messages, userMessage, { sender: "Bot", text: "Enter schema name to view versions:" }]);
    } else {
        setMessages([...messages, userMessage, { sender: "Bot", text: "Invalid schema operation. Try 'register schema' or 'view schema versions'." }]);
    }
} else if (state === "waiting_schema_name") {
    setSchemaName(input);
    setState("waiting_user_group");
    setMessages([...messages, userMessage, { sender: "Bot", text: "Enter your user group:" }]);
} else if (state === "waiting_user_group") {
    setUserGroup(input);
    let endpoint = state === "waiting_schema_name" ? "/api/chatbot/schema_register" : "/api/chatbot/schema_versions";

    const response = await fetch(endpoint, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ schemaName, userGroup }),
    });

    const botResponse = await response.json();
    setMessages([...messages, userMessage, { sender: "Bot", text: botResponse.message }]);
    setState(null);
}

if (state === "waiting_user_group") {
    setUserGroup(input);
    setState("waiting_role");
    setMessages([...messages, userMessage, { sender: "Bot", text: "What role should be assigned? (admin/producer/consumer)" }]);
} else if (state === "waiting_role") {
    const response = await fetch("/api/chatbot/enable_rbac", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ topic: topicName, userGroup, role: input }),
    });

    const botResponse = await response.json();
    setMessages([...messages, userMessage, { sender: "Bot", text: botResponse.message }]);
    setState(null);
}
