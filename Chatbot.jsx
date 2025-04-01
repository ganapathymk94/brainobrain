import React, { useState } from "react";
import { Box, TextField, Button, Typography } from "@mui/material";

const Chatbot = () => {
  const [messages, setMessages] = useState([]);
  const [input, setInput] = useState("");

  const handleSendMessage = async () => {
    const userMessage = { sender: "User", text: input };
    const response = await fetch("/api/chatbot", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ query: input }),
    });

    const botResponse = await response.json();
    setMessages([...messages, userMessage, { sender: "Bot", text: botResponse.message }]);
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
