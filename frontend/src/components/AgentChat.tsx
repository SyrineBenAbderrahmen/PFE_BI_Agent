import { useState } from "react";
import { Box, Button, Paper, TextField, Typography, Alert } from "@mui/material";

export type ChatMsg = { role: "user" | "ai"; text: string };

export default function AgentChat({
  messages,
  warning,
  loading,
  onSend,
  onClear,
}: {
  messages: ChatMsg[];
  warning?: string | null;
  loading: boolean;
  onSend: (prompt: string) => void;
  onClear: () => void;
}) {
  const [prompt, setPrompt] = useState("");

  const send = () => {
    const p = prompt.trim();
    if (!p || loading) return;
    onSend(p);
    setPrompt("");
  };

  return (
    <Box sx={{ display: "flex", flexDirection: "column", height: "100%" }}>
      <Box sx={{ p: 2, borderBottom: "1px solid rgba(255,255,255,0.08)" }}>
        <Typography sx={{ fontWeight: 900, letterSpacing: 0.6 }}>AGENT</Typography>
      </Box>

      <Box sx={{ flex: 1, p: 2, overflow: "auto" }}>
        {warning ? (
          <Alert severity="warning" sx={{ mb: 2 }}>
            {warning}
          </Alert>
        ) : null}

        {messages.map((m, i) => (
          <Paper
            key={i}
            sx={{
              p: 1.2,
              mb: 1,
              maxWidth: "85%",
              ml: m.role === "user" ? "auto" : 0,
              bgcolor: m.role === "user" ? "rgba(59,130,246,0.25)" : "rgba(255,255,255,0.06)",
              border: "1px solid rgba(255,255,255,0.06)",
              color: "white",
              whiteSpace: "pre-wrap",
            }}
          >
            {m.text}
          </Paper>
        ))}
      </Box>

      <Box sx={{ p: 2, borderTop: "1px solid rgba(255,255,255,0.08)" }}>
        <TextField
          fullWidth
          multiline
          minRows={3}
          value={prompt}
          onChange={(e) => setPrompt(e.target.value)}
          placeholder="Enter your prompt here..."
        />
        <Box sx={{ display: "flex", justifyContent: "flex-end", gap: 1, mt: 1 }}>
          <Button variant="outlined" onClick={onClear} disabled={loading}>
            Clear
          </Button>
          <Button variant="contained" onClick={send} disabled={loading || !prompt.trim()}>
            {loading ? "Generating..." : "Generate"}
          </Button>
        </Box>
      </Box>
    </Box>
  );
}