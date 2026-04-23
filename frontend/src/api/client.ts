export const API_BASE = import.meta.env.VITE_API_BASE || "http://localhost:8000";

export type DwItem = { id: string; label: string };

export type AgentPromptResponse = {
  status: "success" | "error";
  json_structure?: any;
  xmla_script?: string;
  suggested_mdx?: string;
  warning?: string | null;
  message?: string;
  raw_ai_output?: string;
};

export async function getDws(): Promise<DwItem[]> {
  const res = await fetch(`${API_BASE}/dws`);
  if (!res.ok) throw new Error("Failed to load /dws");
  return res.json();
}

export async function agentPrompt(dw: string, prompt: string): Promise<AgentPromptResponse> {
  const res = await fetch(`${API_BASE}/agent/prompt`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ dw, prompt }),
  });
  return res.json();
}

// IMPORTANT: RAW endpoints => no \n issues in JSON stringify
export async function agentMdxRaw(dw: string, prompt: string): Promise<string> {
  const res = await fetch(`${API_BASE}/agent/mdx-raw`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ dw, prompt }),
  });
  return res.text();
}

export async function agentXmlaRaw(dw: string, prompt: string): Promise<string> {
  const res = await fetch(`${API_BASE}/agent/xmla-raw`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ dw, prompt }),
  });
  return res.text();
}