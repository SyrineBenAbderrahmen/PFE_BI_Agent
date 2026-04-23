export const API_BASE = import.meta.env.VITE_API_BASE || "http://127.0.0.1:8000";

export type DwItem = {
  id: string;
  label: string;
  database?: string;
  schema?: string;
  ssas_database?: string;
  cube_name?: string;
};

export type GuidancePayload = {
  is_vague?: boolean;
  help_message?: string;
  suggested_measures?: string[];
  suggested_dimensions?: string[];
  guided_questions?: string[];
  assistant_stage?: string;
};

export type CubeModelPayload = {
  cube_name?: string;
  description?: string;
  facts?: Array<{ name?: string } | string>;
  dimensions?: Array<{ name?: string } | string>;
  measures?: Array<{ name?: string; id?: string } | string>;
};

export type AgentPromptResponse = {
  status: "success" | "error" | "needs_clarification" | "invalid" | "warning";
  message?: string;

  guidance?: GuidancePayload;

  json_structure?: any;
  suggested_mdx?: string | null;
  mdx?: string | null;
  xmla_script?: string | null;

  cube_name_used?: string | null;
  ssas_database_used?: string | null;

  intent?: string;
  cube_model?: CubeModelPayload | any;
  validation?: any;
  preview?: any;
};

export type CubeActionResponse = {
  status: "success" | "error" | "needs_clarification" | "invalid" | "warning";
  message?: string;
  intent?: string;
  cube_model?: CubeModelPayload | any;
  validation?: any;
  xmla_script?: string | null;
  preview?: any;
};

export type HistoryItem = {
  id: number;
  dw_id: string;
  cube_name?: string | null;
  prompt: string;
  intent?: string | null;
  status?: string | null;
  response_message?: string | null;
  xmla_script?: string | null;
  preview?: any;
  created_at: string;
};

export type HistoryResponse = {
  status: "success" | "error";
  items: HistoryItem[];
};

async function safeJson(res: Response) {
  const text = await res.text();
  try {
    return JSON.parse(text);
  } catch {
    return { raw: text };
  }
}

function extractErrorMessage(data: any, fallback: string) {
  return data?.detail || data?.message || data?.error || data?.raw || fallback;
}

export async function getDws(): Promise<DwItem[]> {
  const res = await fetch(`${API_BASE}/dws`);
  const data = await safeJson(res);

  if (!res.ok) {
    throw new Error(extractErrorMessage(data, `GET /dws failed (${res.status})`));
  }

  return data as DwItem[];
}

export async function extractSchema(dwId: string): Promise<any> {
  const res = await fetch(`${API_BASE}/dw/${dwId}/extract-schema`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({}),
  });

  const data = await safeJson(res);

  if (!res.ok) {
    throw new Error(
      extractErrorMessage(data, `POST /dw/${dwId}/extract-schema failed (${res.status})`)
    );
  }

  return data;
}

export async function getHistory(dwId: string): Promise<HistoryResponse> {
  const res = await fetch(`${API_BASE}/history/${dwId}`);
  const data = await safeJson(res);

  if (!res.ok) {
    throw new Error(extractErrorMessage(data, `GET /history/${dwId} failed (${res.status})`));
  }

  return data as HistoryResponse;
}

export async function agentPrompt(
  dwId: string,
  prompt: string
): Promise<AgentPromptResponse> {
  const res = await fetch(`${API_BASE}/agent/prompt`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ dw: dwId, prompt }),
  });

  const data = await safeJson(res);

  if (!res.ok) {
    return {
      status: "error",
      message: extractErrorMessage(data, `POST /agent/prompt failed (${res.status})`),
    };
  }

  return data as AgentPromptResponse;
}

export async function cubeAction(
  dwId: string,
  prompt: string
): Promise<CubeActionResponse> {
  const res = await fetch(`${API_BASE}/cube/action`, {
    method: "POST",
    headers: {
      "Content-Type": "application/json",
    },
    body: JSON.stringify({
      dw: dwId,
      prompt,
    }),
  });

  const data = await safeJson(res);

  if (!res.ok) {
    return {
      status: "error",
      message: extractErrorMessage(data, `POST /cube/action failed (${res.status})`),
    };
  }

  return data as CubeActionResponse;
}

export async function agentXmlaRaw(dwId: string, prompt: string): Promise<string> {
  const response = await fetch(`${API_BASE}/agent/xmla-raw`, {
    method: "POST",
    headers: {
      "Content-Type": "application/json",
    },
    body: JSON.stringify({
      dw: dwId,
      prompt,
    }),
  });

  if (!response.ok) {
    const data = await safeJson(response);
    throw new Error(
      extractErrorMessage(data, `POST /agent/xmla-raw failed (${response.status})`)
    );
  }

  return response.text();
}