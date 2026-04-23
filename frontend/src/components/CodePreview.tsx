import { useState } from "react";
import { Box, Button, Tab, Tabs, Typography } from "@mui/material";
import ContentCopyIcon from "@mui/icons-material/ContentCopy";

function CodeBox({ value }: { value: string }) {
  return (
    <Box
      sx={{
        flex: 1,
        p: 2,
        overflow: "auto",
        fontFamily: "Consolas, monospace",
        fontSize: 13,
        whiteSpace: "pre-wrap",
        wordBreak: "break-word",
      }}
    >
      {value || "—"}
    </Box>
  );
}

export default function CodePreview({
  mdx,
  xmla,
  jsonPretty,
}: {
  mdx: string;
  xmla: string;
  jsonPretty: string;
}) {
  const [tab, setTab] = useState<0 | 1 | 2>(0);

  const active = tab === 0 ? mdx : tab === 1 ? xmla : jsonPretty;

  const copy = async () => {
    await navigator.clipboard.writeText(active || "");
  };

  return (
    <Box sx={{ display: "flex", flexDirection: "column", height: "100%" }}>
      <Box
        sx={{
          p: 2,
          borderBottom: "1px solid rgba(255,255,255,0.08)",
          display: "flex",
          justifyContent: "space-between",
          alignItems: "center",
        }}
      >
        <Typography sx={{ fontWeight: 900, letterSpacing: 0.6 }}>CODE PREVIEW</Typography>
        <Button variant="outlined" onClick={copy} startIcon={<ContentCopyIcon />} disabled={!active}>
          Copy
        </Button>
      </Box>

      <Tabs value={tab} onChange={(_, v) => setTab(v)} sx={{ borderBottom: "1px solid rgba(255,255,255,0.08)" }}>
        <Tab label="MDX (raw)" />
        <Tab label="XMLA (raw)" />
        <Tab label="JSON" />
      </Tabs>

      <CodeBox value={active} />
    </Box>
  );
}