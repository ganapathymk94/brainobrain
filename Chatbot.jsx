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
