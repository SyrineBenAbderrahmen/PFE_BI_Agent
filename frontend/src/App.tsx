import React, { useEffect, useMemo, useState } from "react";
import {
  AppBar,
  Toolbar,
  Typography,
  Box,
  Paper,
  Select,
  MenuItem,
  Button,
  TextField,
  Tabs,
  Tab,
  IconButton,
  Snackbar,
  Alert,
  CircularProgress,
  Divider,
  Chip,
  Grid,
  Stack,
} from "@mui/material";
import ContentCopyIcon from "@mui/icons-material/ContentCopy";
import SyncIcon from "@mui/icons-material/Sync";
import SendIcon from "@mui/icons-material/Send";
import AutoAwesomeIcon from "@mui/icons-material/AutoAwesome";
import DashboardCustomizeIcon from "@mui/icons-material/DashboardCustomize";
import RocketLaunchIcon from "@mui/icons-material/RocketLaunch";
import TipsAndUpdatesIcon from "@mui/icons-material/TipsAndUpdates";

import {
  agentPrompt,
  extractSchema,
  getDws,
  cubeAction,
  type DwItem,
  type HistoryItem,
  getHistory,
} from "./types/api";

type WorkspaceMode = "query" | "cube_design" | "deploy";

type ChatMsg = {
  id: string;
  role: "user" | "assistant" | "system";
  text: string;
  ts: number;
  status?: string;
  xmla_script?: string | null;
  mdx_script?: string | null;
  preview?: any;
  json_data?: any;
  linkedTo?: string;
  cube_name_used?: string | null;
  ssas_database_used?: string | null;
};

function copyToClipboard(text: string) {
  navigator.clipboard.writeText(text).catch(() => {});
}

function makeId() {
  if (typeof crypto !== "undefined" && "randomUUID" in crypto) {
    return crypto.randomUUID();
  }
  return `${Date.now()}-${Math.random().toString(36).slice(2)}`;
}

function TabPanel(props: {
  value: number;
  index: number;
  children: React.ReactNode;
}) {
  const { value, index, children } = props;
  if (value !== index) return null;
  return <Box sx={{ p: 2 }}>{children}</Box>;
}

function safeJsonParse(raw: string | null | undefined): any | null {
  if (!raw || typeof raw !== "string") return null;
  try {
    return JSON.parse(raw);
  } catch {
    return null;
  }
}

function buildClarificationJson(res: any, dwId: string, mode: WorkspaceMode) {
  const guidance = res?.guidance ?? null;
  const clarification = res?.clarification ?? null;

  const guidedQuestions =
    Array.isArray(clarification?.guided_questions)
      ? clarification.guided_questions
      : Array.isArray(guidance?.guided_questions)
        ? guidance.guided_questions
        : [];

  const suggestedMeasures =
    Array.isArray(clarification?.suggestions?.measures)
      ? clarification.suggestions.measures
      : Array.isArray(guidance?.suggested_measures)
        ? guidance.suggested_measures
        : [];

  const suggestedDimensions =
    Array.isArray(clarification?.suggestions?.dimensions)
      ? clarification.suggestions.dimensions
      : Array.isArray(guidance?.suggested_dimensions)
        ? guidance.suggested_dimensions
        : [];

  return {
    status: "needs_clarification",
    assistant_stage:
      res?.assistant_stage ??
      guidance?.assistant_stage ??
      "pre_generation_guidance",
    workspace_mode: mode,
    dw_id: dwId,
    message: res?.message ?? "Le prompt semble encore vague ou incomplet.",
    clarification: {
      intent_status:
        clarification?.intent_status ?? "prompt_vague",
      next_action:
        clarification?.next_action ?? "ask_user_for_precision",
      required_fields:
        Array.isArray(clarification?.required_fields)
          ? clarification.required_fields
          : ["measure", "analysis_axis", "time_filter_optional", "analysis_type"],
      guided_questions: guidedQuestions,
      suggestions: {
        measures: suggestedMeasures,
        dimensions: suggestedDimensions,
      },
    },
    generation: {
      mdx_generated: false,
      xmla_generated: false,
    },
  };
}

function buildSuccessJson(res: any, dwId: string, mode: WorkspaceMode) {
  return {
    status: res?.status ?? "success",
    workspace_mode: mode,
    dw_id: dwId,
    message: res?.message ?? "Réponse générée avec succès.",
    metadata: {
      cube_name_used:
        res?.cube_name_used ?? res?.cube_model?.cube_name ?? res?.preview?.cube_name ?? null,
      ssas_database_used: res?.ssas_database_used ?? null,
    },
    json_structure: res?.json_structure ?? null,
    cube_model: res?.cube_model ?? null,
    validation: res?.validation ?? null,
    preview: res?.preview ?? null,
    guidance: res?.guidance ?? null,
    generation: {
      mdx_generated: Boolean(res?.suggested_mdx || res?.mdx),
      xmla_generated: Boolean(res?.xmla_script),
    },
  };
}

function formatClarificationMessage(res: any): string {
  const message =
    res?.message ??
    "Le prompt est trop général. Précise la mesure à analyser, la dimension d’affichage et éventuellement une période.";

  const clarification = res?.clarification ?? {};
  const suggestions = clarification?.suggestions ?? {};

  const guidedQuestions = Array.isArray(clarification?.guided_questions)
    ? clarification.guided_questions
    : [];

  const suggestedMeasures = Array.isArray(suggestions?.measures)
    ? suggestions.measures
    : [];

  const suggestedDimensions = Array.isArray(suggestions?.dimensions)
    ? suggestions.dimensions
    : [];

  const parts: string[] = [message];

  if (guidedQuestions.length > 0) {
    parts.push("");
    parts.push("Questions guidées :");
    guidedQuestions.forEach((q: string) => parts.push(`- ${q}`));
  }

  if (suggestedMeasures.length > 0) {
    parts.push("");
    parts.push("Mesures suggérées :");
    suggestedMeasures.forEach((m: string) => parts.push(`- ${m}`));
  }

  if (suggestedDimensions.length > 0) {
    parts.push("");
    parts.push("Dimensions suggérées :");
    suggestedDimensions.forEach((d: string) => parts.push(`- ${d}`));
  }

  return parts.join("\n");
}

function buildAssistantSentence(res: any, mode: WorkspaceMode): string {
  const status = String(res?.status || "").toLowerCase();
  const hasMdx = Boolean(res?.suggested_mdx || res?.mdx);
  const hasXmla = Boolean(res?.xmla_script);

  if (status === "needs_clarification") {
    return "Merci de préciser ta demande.";
  }

  if (status === "warning") {
    if (hasXmla) return "Action effectuée avec avertissement. XMLA disponible.";
    if (hasMdx) return "Résultat généré avec avertissement. MDX disponible.";
    return "Action effectuée avec avertissement.";
  }

  if (status !== "success") {
    return res?.message ? `❌ ${res.message}` : "❌ Une erreur est survenue.";
  }

  if (mode === "cube_design") {
    if (hasXmla) return "Structure générée avec succès. XMLA disponible.";
    return "Structure générée avec succès.";
  }

  if (mode === "deploy") {
    if (hasXmla) return "Déploiement préparé avec succès. XMLA disponible.";
    return "Déploiement préparé avec succès.";
  }

  if (hasMdx) return "Requête générée avec succès. MDX disponible.";
  return "Réponse générée avec succès.";
}

function buildHistoryAssistantSentence(item: HistoryItem): string {
  const rawPreview = item?.preview ?? null;
  const preview =
    typeof rawPreview === "string" ? safeJsonParse(rawPreview) ?? rawPreview : rawPreview;

  const status = String(item?.status || "").toLowerCase();
  const hasMdx = Boolean(preview?.suggested_mdx || preview?.mdx || item?.generated_mdx);
  const hasXmla = Boolean(item?.xmla_script);

  if (status === "needs_clarification") {
    return formatClarificationMessage(
      preview ?? {
        message: item?.response_message ?? "Merci de préciser ta demande.",
        clarification: {
          guided_questions: [],
          suggestions: { measures: [], dimensions: [] },
        },
      }
    );
  }

  if (status === "warning") {
    if (hasXmla) return "Action effectuée avec avertissement. XMLA disponible.";
    if (hasMdx) return "Résultat généré avec avertissement. MDX disponible.";
    return "Action effectuée avec avertissement.";
  }

  if (status === "success") {
    const hasCubePreview = Boolean(preview?.cube_name || preview?.facts || preview?.dimensions);
    if (hasXmla && hasCubePreview) return "Structure générée avec succès. XMLA disponible.";
    if (hasMdx) return "Requête générée avec succès. MDX disponible.";
    return "Réponse générée avec succès.";
  }

  if (item?.response_message && !String(item.response_message).startsWith("{")) {
    return item.response_message;
  }

  return item?.response_message
    ? `❌ ${item.response_message}`
    : "❌ Une erreur est survenue.";
}

function buildStructurePreview(data: any) {
  const jsonStructure = data?.json_structure ?? data?.preview ?? data?.cube_model ?? null;

  if (!jsonStructure) {
    return {
      title: "Aucune structure",
      subtitle: data?.message ?? "",
      facts: [],
      dimensions: [],
      measures: [],
      questions: [],
    };
  }

  return {
    title:
      jsonStructure?.cube_name ||
      data?.metadata?.cube_name_used ||
      data?.cube_name_used ||
      "Structure générée",
    subtitle: data?.message ?? "",
    facts: jsonStructure?.fact_table
      ? [jsonStructure.fact_table]
      : Array.isArray(jsonStructure?.facts)
        ? jsonStructure.facts.map((f: any) => f?.name || f)
        : [],
    dimensions: Array.isArray(jsonStructure?.dimensions)
      ? jsonStructure.dimensions.map((d: any) => d?.name || d?.table || d).filter(Boolean)
      : [],
    measures: Array.isArray(jsonStructure?.measures)
      ? jsonStructure.measures.map((m: any) => m?.name || m?.id || m).filter(Boolean)
      : [],
    questions: [],
  };
}

function WorkspaceModeSelector(props: {
  mode: WorkspaceMode;
  onChange: (mode: WorkspaceMode) => void;
}) {
  const { mode, onChange } = props;

  const items: Array<{
    value: WorkspaceMode;
    label: string;
    icon: React.ReactNode;
  }> = [
    {
      value: "query",
      label: "Chat analytique",
      icon: <AutoAwesomeIcon fontSize="small" />,
    },
    {
      value: "cube_design",
      label: "Conception cube",
      icon: <DashboardCustomizeIcon fontSize="small" />,
    },
    {
      value: "deploy",
      label: "Déploiement",
      icon: <RocketLaunchIcon fontSize="small" />,
    },
  ];

  return (
    <Stack direction="row" spacing={1} flexWrap="wrap" useFlexGap>
      {items.map((item) => (
        <Chip
          key={item.value}
          icon={item.icon as any}
          label={item.label}
          clickable
          color={mode === item.value ? "primary" : "default"}
          variant={mode === item.value ? "filled" : "outlined"}
          onClick={() => onChange(item.value)}
        />
      ))}
    </Stack>
  );
}

function historyItemsToChat(items: HistoryItem[]): ChatMsg[] {
  const rows = [...(items || [])].reverse();
  const mapped: ChatMsg[] = [];

  for (const item of rows) {
    const pairId = `hist-${item.id}`;
    const rawPreview = item.preview || null;
    const preview =
      typeof rawPreview === "string" ? safeJsonParse(rawPreview) ?? rawPreview : rawPreview;

    mapped.push({
      id: `${pairId}-user`,
      role: "user",
      text: item.prompt || "",
      ts: new Date(item.created_at).getTime(),
    });

    mapped.push({
      id: `${pairId}-assistant`,
      role: "assistant",
      text: buildHistoryAssistantSentence({
        ...item,
        preview,
      } as HistoryItem),
      ts: new Date(item.created_at).getTime() + 1,
      status: item.status || undefined,
      xmla_script: item.xmla_script || null,
      mdx_script: preview?.suggested_mdx || preview?.mdx || item.generated_mdx || null,
      preview: preview || null,
      json_data: preview || null,
      linkedTo: `${pairId}-user`,
      cube_name_used: preview?.cube_name || item.cube_name || null,
      ssas_database_used: preview?.ssas_database_used || null,
    });
  }

  return mapped;
}

export default function App() {
  const [dws, setDws] = useState<DwItem[]>([]);
  const [dwId, setDwId] = useState<string>("");
  const [loadingDws, setLoadingDws] = useState<boolean>(true);

  const [chat, setChat] = useState<ChatMsg[]>([]);
  const [prompt, setPrompt] = useState<string>("");
  const [workspaceMode, setWorkspaceMode] = useState<WorkspaceMode>("query");

  const [busy, setBusy] = useState(false);
  const [syncBusy, setSyncBusy] = useState(false);

  const [snack, setSnack] = useState<{
    open: boolean;
    type: "success" | "error" | "info" | "warning";
    msg: string;
  }>({
    open: false,
    type: "info",
    msg: "",
  });

  const [tab, setTab] = useState(0);

  const [latestJson, setLatestJson] = useState<any>(null);
  const [latestMdx, setLatestMdx] = useState<string>("");
  const [latestXmla, setLatestXmla] = useState<string>("");

  const [selectedMessageId, setSelectedMessageId] = useState<string | null>(null);

  const selectedDw = useMemo(() => dws.find((x) => x.id === dwId), [dws, dwId]);

  const promptPlaceholder = useMemo(() => {
    if (workspaceMode === "query") {
      return "Ex: afficher le chiffre d'affaires par mois en 2013";
    }
    if (workspaceMode === "cube_design") {
      return "Ex: crée un cube nommé cube_v2 avec 1 fait et 2 dimensions";
    }
    return "Ex: prépare le déploiement XMLA du cube courant";
  }, [workspaceMode]);

  const structurePreview = useMemo(() => buildStructurePreview(latestJson), [latestJson]);

  const dwStatusChip = useMemo(() => {
    if (!selectedDw) return null;
    return (
      <Chip
        size="small"
        label={`DW: ${selectedDw.label}`}
        sx={{ ml: 2 }}
        variant="outlined"
      />
    );
  }, [selectedDw]);

  async function loadHistory(selectedDwId: string) {
    if (!selectedDwId) return;

    try {
      const data = await getHistory(selectedDwId);

      const helperText =
        workspaceMode === "query"
          ? "Mode actif : Chat analytique. Pose une question métier ou demande une requête."
          : workspaceMode === "cube_design"
            ? "Mode actif : Conception cube. Demande la création ou la modification d’un cube."
            : "Mode actif : Déploiement. Prévisualise la structure, génère XMLA ou prépare le déploiement.";

      if (data?.status === "success" && Array.isArray(data.items)) {
        const historyChat = historyItemsToChat(data.items);

        setChat([
          {
            id: "system-start",
            role: "system",
            text: "Sélectionne un DW, clique sur “Sync / Extract Schema”, puis choisis un mode de travail.",
            ts: Date.now() - 2,
          },
          ...historyChat,
          {
            id: "system-mode",
            role: "system",
            text: helperText,
            ts: Date.now() - 1,
          },
        ]);
      } else {
        setChat([
          {
            id: "system-start",
            role: "system",
            text: "Sélectionne un DW, clique sur “Sync / Extract Schema”, puis choisis un mode de travail.",
            ts: Date.now() - 2,
          },
          {
            id: "system-mode",
            role: "system",
            text: helperText,
            ts: Date.now() - 1,
          },
        ]);
      }
    } catch (e: any) {
      setChat([
        {
          id: "system-start",
          role: "system",
          text: "Sélectionne un DW, clique sur “Sync / Extract Schema”, puis choisis un mode de travail.",
          ts: Date.now() - 2,
        },
        {
          id: "history-error",
          role: "assistant",
          text: `⚠️ Impossible de charger l'historique : ${e?.message || "Erreur"}`,
          ts: Date.now() - 1,
          status: "error",
        },
      ]);
    }
  }

  useEffect(() => {
    (async () => {
      try {
        setLoadingDws(true);
        const list = await getDws();
        setDws(list);
        if (list.length > 0) setDwId(list[0].id);
      } catch (e: any) {
        setSnack({
          open: true,
          type: "error",
          msg: e?.message || "Impossible de charger /dws",
        });
      } finally {
        setLoadingDws(false);
      }
    })();
  }, []);

  useEffect(() => {
    if (!dwId) return;
    loadHistory(dwId);
  }, [dwId, workspaceMode]);

  async function onSync() {
    if (!dwId) return;

    try {
      setSyncBusy(true);
      await extractSchema(dwId);

      setSnack({
        open: true,
        type: "success",
        msg: "Schema snapshot mis à jour ✅",
      });

      setChat((c) => [
        ...c,
        {
          id: makeId(),
          role: "assistant",
          text: "Schema snapshot mis à jour avec succès.",
          ts: Date.now(),
          status: "success",
        },
      ]);

      setTimeout(() => {
        loadHistory(dwId);
      }, 300);
    } catch (e: any) {
      setSnack({
        open: true,
        type: "error",
        msg: e?.message || "Sync failed",
      });
    } finally {
      setSyncBusy(false);
    }
  }

  function buildContextualPrompt(rawPrompt: string) {
    const trimmed = rawPrompt.trim();
    if (!trimmed) return trimmed;

    if (workspaceMode === "query") {
      return trimmed;
    }

    if (workspaceMode === "cube_design") {
      return `[MODE: CUBE_DESIGN]\n${trimmed}`;
    }

    return `[MODE: DEPLOY]\n${trimmed}`;
  }

  function handleSelectMessage(msg: ChatMsg) {
    setSelectedMessageId(msg.id);

    if (msg.role === "user") {
      const linkedResponse = chat.find(
        (m) => m.role === "assistant" && m.linkedTo === msg.id
      );
      if (!linkedResponse) return;

      setLatestJson(linkedResponse.json_data ?? linkedResponse.preview ?? null);
      setLatestMdx(linkedResponse.mdx_script ?? "");
      setLatestXmla(linkedResponse.xmla_script ?? "");

      if (linkedResponse.xmla_script) {
        setTab(3);
      } else if (linkedResponse.mdx_script) {
        setTab(2);
      } else {
        setTab(0);
      }
      return;
    }

    if (msg.role === "assistant") {
      setLatestJson(msg.json_data ?? msg.preview ?? null);
      setLatestMdx(msg.mdx_script ?? "");
      setLatestXmla(msg.xmla_script ?? "");

      if (msg.xmla_script) {
        setTab(3);
      } else if (msg.mdx_script) {
        setTab(2);
      } else {
        setTab(0);
      }
    }
  }

  async function onSend(customPrompt?: string) {
    const q = (customPrompt ?? prompt).trim();
    if (!q || !dwId) return;

    const finalPrompt = buildContextualPrompt(q);
    
    const userId = makeId();
    const userMsg: ChatMsg = {
      id: userId,
      role: "user",
      text: q,
      ts: Date.now(),
    };

    setChat((c) => [...c, userMsg]);
    setPrompt("");
    setSelectedMessageId(userId);

    try {
      setBusy(true);

      const res: any =
        workspaceMode === "query"
          ? await agentPrompt(dwId, finalPrompt)
          : await cubeAction(dwId, finalPrompt);

      if (res?.status === "needs_clarification") {
        const clarificationJson = buildClarificationJson(res, dwId, workspaceMode);
        const clarificationMessage = formatClarificationMessage(clarificationJson);

        const assistantMsg: ChatMsg = {
          id: makeId(),
          role: "assistant",
          text: clarificationMessage,
          ts: Date.now() + 1,
          status: "warning",
          preview: res?.preview ?? null,
          json_data: clarificationJson,
          linkedTo: userId,
          cube_name_used:
            res?.cube_name_used ?? res?.preview?.cube_name ?? res?.cube_model?.cube_name ?? null,
          ssas_database_used: res?.ssas_database_used ?? null,
        };

        setChat((c) => [...c, assistantMsg]);
        setSelectedMessageId(assistantMsg.id);

        setLatestJson(clarificationJson);
        setLatestMdx("");
        setLatestXmla("");
        setTab(0);

        setSnack({
          open: true,
          type: "warning",
          msg: "Le prompt est vague. L’agent attend une précision avant génération.",
        });

        setTimeout(() => {
          loadHistory(dwId);
        }, 300);

        return;
      }

      if (res?.status !== "success" && res?.status !== "warning") {
        const errorJson = {
          status: "error",
          workspace_mode: workspaceMode,
          dw_id: dwId,
          message: res?.message || "Erreur agent",
          generation: {
            mdx_generated: false,
            xmla_generated: false,
          },
        };

        const assistantMsg: ChatMsg = {
          id: makeId(),
          role: "assistant",
          text:
            typeof res?.message === "string"
              ? `❌ ${res.message}`
              : "❌ Une erreur est survenue.",
          ts: Date.now() + 1,
          status: res?.status || "error",
          xmla_script: res?.xmla_script ?? null,
          mdx_script: res?.suggested_mdx ?? res?.mdx ?? null,
          preview: res?.preview ?? null,
          json_data: errorJson,
          linkedTo: userId,
          cube_name_used:
            res?.cube_name_used ?? res?.preview?.cube_name ?? res?.cube_model?.cube_name ?? null,
          ssas_database_used: res?.ssas_database_used ?? null,
        };

        setChat((c) => [...c, assistantMsg]);
        setSelectedMessageId(assistantMsg.id);

        setLatestJson(errorJson);
        setLatestMdx(res?.suggested_mdx ?? res?.mdx ?? "");
        setLatestXmla(res?.xmla_script ?? "");
        setTab((res?.suggested_mdx || res?.mdx || "").length > 0 ? 2 : 0);

        setSnack({
          open: true,
          type: "error",
          msg: res?.message || "Erreur agent",
        });

        setTimeout(() => {
          loadHistory(dwId);
        }, 300);

        return;
      }

      const successJson = buildSuccessJson(res, dwId, workspaceMode);
      const summary = buildAssistantSentence(res, workspaceMode);

      const assistantMsg: ChatMsg = {
        id: makeId(),
        role: "assistant",
        text: summary,
        ts: Date.now() + 1,
        status: res?.status,
        xmla_script: res?.xmla_script ?? null,
        mdx_script: res?.suggested_mdx ?? res?.mdx ?? null,
        preview: res?.preview ?? null,
        json_data: successJson,
        linkedTo: userId,
        cube_name_used:
          res?.cube_name_used ?? res?.preview?.cube_name ?? res?.cube_model?.cube_name ?? null,
        ssas_database_used: res?.ssas_database_used ?? null,
      };

      setChat((c) => [...c, assistantMsg]);
      setSelectedMessageId(assistantMsg.id);

      setLatestJson(successJson);
      setLatestMdx(res?.suggested_mdx ?? res?.mdx ?? "");
      setLatestXmla(res?.xmla_script ?? "");

      if ((res?.xmla_script || "").length > 0) {
        setTab(3);
      } else if ((res?.suggested_mdx || res?.mdx || "").length > 0) {
        setTab(2);
      } else {
        setTab(0);
      }

      setTimeout(() => {
        loadHistory(dwId);
      }, 300);
    } catch (e: any) {
      const errMsg = e?.message || "Erreur réseau";

      const assistantMsg: ChatMsg = {
        id: makeId(),
        role: "assistant",
        text: `❌ ${errMsg}`,
        ts: Date.now() + 1,
        status: "error",
        json_data: {
          status: "error",
          workspace_mode: workspaceMode,
          dw_id: dwId,
          message: errMsg,
        },
        linkedTo: userId,
      };

      setChat((c) => [...c, assistantMsg]);
      setSelectedMessageId(assistantMsg.id);

      setLatestJson(assistantMsg.json_data);
      setLatestMdx("");
      setLatestXmla("");
      setTab(0);

      setSnack({
        open: true,
        type: "error",
        msg: errMsg,
      });
    } finally {
      setBusy(false);
    }
  }

  return (
    <Box sx={{ height: "100vh", display: "flex", flexDirection: "column", bgcolor: "#071227" }}>
      <AppBar position="static" sx={{ bgcolor: "#0b1529" }}>
        <Toolbar sx={{ gap: 2 }}>
          <Typography variant="h6" sx={{ flexGrow: 1 }}>
            BI OLAP Agent — UI de test
          </Typography>

          {loadingDws ? (
            <CircularProgress size={22} color="inherit" />
          ) : (
            <Select
              size="small"
              value={dwId}
              onChange={(e) => setDwId(e.target.value)}
              sx={{
                bgcolor: "rgba(255,255,255,0.12)",
                color: "white",
                minWidth: 260,
                ".MuiOutlinedInput-notchedOutline": { borderColor: "rgba(255,255,255,0.25)" },
                ".MuiSvgIcon-root": { color: "white" },
              }}
            >
              {dws.map((dw) => (
                <MenuItem key={dw.id} value={dw.id}>
                  {dw.label}
                </MenuItem>
              ))}
            </Select>
          )}

          <Button
            color="inherit"
            variant="outlined"
            startIcon={syncBusy ? <CircularProgress size={16} color="inherit" /> : <SyncIcon />}
            onClick={onSync}
            disabled={!dwId || syncBusy}
            sx={{ borderColor: "rgba(255,255,255,0.6)" }}
          >
            Sync / Extract Schema
          </Button>

          {dwStatusChip}
        </Toolbar>
      </AppBar>

      <Box sx={{ p: 2 }}>
        <Paper sx={{ p: 2, bgcolor: "#0f172a", color: "white" }}>
          <Stack direction={{ xs: "column", md: "row" }} spacing={2} alignItems={{ xs: "flex-start", md: "center" }}>
            <Box>
              <Typography variant="subtitle1" fontWeight={700}>
                Workspace
              </Typography>
            </Box>

            <WorkspaceModeSelector mode={workspaceMode} onChange={setWorkspaceMode} />

            <Box sx={{ ml: "auto", display: "flex", alignItems: "center", gap: 1 }}>
              <TipsAndUpdatesIcon fontSize="small" />
              <Typography variant="body2">
                Historique chargé automatiquement par sujet
              </Typography>
            </Box>
          </Stack>
        </Paper>
      </Box>

      <Box sx={{ flex: 1, p: 2, overflow: "hidden" }}>
        <Grid container spacing={2} sx={{ height: "calc(100vh - 170px)" }}>
          <Grid size={{ xs: 12, md: 6 }} sx={{ height: "100%" }}>
            <Paper sx={{ height: "100%", display: "flex", flexDirection: "column", bgcolor: "#0f172a", color: "white" }}>
              <Box sx={{ p: 2, display: "flex", alignItems: "center", justifyContent: "space-between" }}>
                <Typography variant="subtitle1" fontWeight={700}>
                  AGENT (Chat)
                </Typography>
                <Chip size="small" label={busy ? "Génération..." : "Prêt"} color={busy ? "warning" : "success"} />
              </Box>

              <Divider sx={{ borderColor: "rgba(255,255,255,0.08)" }} />

              <Box sx={{ p: 2 }}>
                <Stack direction="row" spacing={1} flexWrap="wrap" useFlexGap>
                  {[
                    "Crée un cube nommé cube_v2 avec 1 fait et 2 dimensions",
                    "Ajoute une mesure quantity",
                    "Modifie la dimension date et ajoute une hiérarchie",
                    "Ajoute une mesure TauxRejet = RejectedQty / ReceivedQty",
                  ].map((item) => (
                    <Chip
                      key={item}
                      size="small"
                      label={item}
                      onClick={() => setPrompt(item)}
                      variant="outlined"
                      sx={{ color: "white", borderColor: "rgba(255,255,255,0.25)" }}
                    />
                  ))}
                </Stack>
              </Box>

              <Divider sx={{ borderColor: "rgba(255,255,255,0.08)" }} />

              <Box sx={{ flex: 1, p: 2, overflowY: "auto", bgcolor: "#0b1220" }}>
                {chat.map((m) => (
                  <Box
                    key={m.id}
                    sx={{
                      mb: 1.5,
                      display: "flex",
                      justifyContent: m.role === "user" ? "flex-end" : "flex-start",
                    }}
                  >
                    <Box
                      onClick={() => handleSelectMessage(m)}
                      sx={{
                        cursor: m.role === "system" ? "default" : "pointer",
                        maxWidth: "85%",
                        p: 1.2,
                        borderRadius: 2,
                        whiteSpace: "pre-wrap",
                        fontFamily: "ui-sans-serif, system-ui",
                        bgcolor:
                          m.role === "user"
                            ? "#1d4ed8"
                            : m.role === "system"
                              ? "#334155"
                              : "#0b1220",
                        color: "white",
                        border:
                          selectedMessageId === m.id
                            ? "1px solid #60a5fa"
                            : "1px solid rgba(255,255,255,0.12)",
                        boxShadow:
                          selectedMessageId === m.id
                            ? "0 0 0 1px rgba(96,165,250,0.25)"
                            : "none",
                      }}
                    >
                      <Typography variant="caption" sx={{ opacity: 0.8 }}>
                        {m.role.toUpperCase()}
                      </Typography>

                      <Box sx={{ mt: 0.5 }}>{m.text}</Box>

                      {m.mdx_script ? (
                        <Box sx={{ mt: 1 }}>
                          <Chip
                            size="small"
                            label="MDX disponible"
                            color="info"
                            variant="outlined"
                            onClick={(e) => {
                              e.stopPropagation();
                              setSelectedMessageId(m.id);
                              setLatestJson(m.json_data ?? m.preview ?? null);
                              setLatestMdx(m.mdx_script || "");
                              setLatestXmla(m.xmla_script || "");
                              setTab(2);
                            }}
                          />
                        </Box>
                      ) : null}

                      {m.xmla_script ? (
                        <Box sx={{ mt: 1 }}>
                          <Chip
                            size="small"
                            label="XMLA disponible"
                            color="success"
                            variant="outlined"
                            onClick={(e) => {
                              e.stopPropagation();
                              setSelectedMessageId(m.id);
                              setLatestJson(m.json_data ?? m.preview ?? null);
                              setLatestMdx(m.mdx_script || "");
                              setLatestXmla(m.xmla_script || "");
                              setTab(3);
                            }}
                          />
                        </Box>
                      ) : null}
                    </Box>
                  </Box>
                ))}
              </Box>

              <Divider sx={{ borderColor: "rgba(255,255,255,0.08)" }} />

              <Box sx={{ p: 2, display: "flex", gap: 1 }}>
                <TextField
                  fullWidth
                  size="small"
                  placeholder={promptPlaceholder}
                  value={prompt}
                  onChange={(e) => setPrompt(e.target.value)}
                  onKeyDown={(e) => {
                    if (e.key === "Enter" && !e.shiftKey) {
                      e.preventDefault();
                      onSend();
                    }
                  }}
                  multiline
                  maxRows={4}
                  InputProps={{
                    sx: {
                      color: "white",
                      bgcolor: "#111827",
                    },
                  }}
                />
                <Button
                  variant="contained"
                  onClick={() => onSend()}
                  disabled={!dwId || busy || prompt.trim().length === 0}
                  endIcon={busy ? <CircularProgress size={16} color="inherit" /> : <SendIcon />}
                >
                  Generate
                </Button>
              </Box>
            </Paper>
          </Grid>

          <Grid size={{ xs: 12, md: 6 }} sx={{ height: "100%" }}>
            <Paper sx={{ height: "100%", display: "flex", flexDirection: "column", bgcolor: "#0f172a", color: "white" }}>
              <Box sx={{ p: 2 }}>
                <Typography variant="subtitle1" fontWeight={700}>
                  WORKSPACE PREVIEW
                </Typography>
                <Typography variant="caption" sx={{ opacity: 0.7 }}>
                  Structure, JSON, MDX et XMLA dans le même espace
                </Typography>
              </Box>

              <Divider sx={{ borderColor: "rgba(255,255,255,0.08)" }} />

              <Tabs
                value={tab}
                onChange={(_, v) => setTab(v)}
                variant="fullWidth"
                textColor="primary"
                indicatorColor="primary"
              >
                <Tab label="JSON" />
                <Tab label="STRUCTURE" />
                <Tab label="MDX" />
                <Tab label="XMLA" />
              </Tabs>

              <Divider sx={{ borderColor: "rgba(255,255,255,0.08)" }} />

              <Box sx={{ flex: 1, overflow: "auto" }}>
                <TabPanel value={tab} index={0}>
                  <Box sx={{ display: "flex", justifyContent: "flex-end" }}>
                    <IconButton
                      size="small"
                      onClick={() => copyToClipboard(JSON.stringify(latestJson ?? {}, null, 2))}
                      disabled={!latestJson}
                      sx={{ color: "white" }}
                    >
                      <ContentCopyIcon fontSize="small" />
                    </IconButton>
                  </Box>

                  <Paper
                    variant="outlined"
                    sx={{
                      p: 2,
                      bgcolor: "#0b1220",
                      color: "white",
                      fontFamily: "Consolas, Monaco, monospace",
                      whiteSpace: "pre-wrap",
                      overflowX: "auto",
                    }}
                  >
                    {latestJson ? JSON.stringify(latestJson, null, 2) : "Aucun JSON généré pour le moment."}
                  </Paper>
                </TabPanel>

                <TabPanel value={tab} index={1}>
                  <Paper variant="outlined" sx={{ p: 2, bgcolor: "#0b1220", color: "white" }}>
                    {!structurePreview ? (
                      <Typography variant="body2" sx={{ opacity: 0.7 }}>
                        Aucune structure disponible pour le moment.
                      </Typography>
                    ) : (
                      <Stack spacing={2}>
                        <Box>
                          <Typography variant="h6">{structurePreview.title}</Typography>
                          {structurePreview.subtitle ? (
                            <Typography variant="body2" sx={{ opacity: 0.75 }}>
                              {structurePreview.subtitle}
                            </Typography>
                          ) : null}
                        </Box>

                        {structurePreview.facts?.length > 0 && (
                          <Box>
                            <Typography variant="subtitle2" fontWeight={700}>
                              Facts
                            </Typography>
                            <Stack direction="row" spacing={1} flexWrap="wrap" useFlexGap>
                              {structurePreview.facts.map((fact: string) => (
                                <Chip key={fact} label={fact} size="small" color="primary" variant="outlined" />
                              ))}
                            </Stack>
                          </Box>
                        )}

                        {structurePreview.dimensions?.length > 0 && (
                          <Box>
                            <Typography variant="subtitle2" fontWeight={700}>
                              Dimensions
                            </Typography>
                            <Stack direction="row" spacing={1} flexWrap="wrap" useFlexGap>
                              {structurePreview.dimensions.map((dim: string) => (
                                <Chip key={dim} label={dim} size="small" color="secondary" variant="outlined" />
                              ))}
                            </Stack>
                          </Box>
                        )}

                        {structurePreview.measures?.length > 0 && (
                          <Box>
                            <Typography variant="subtitle2" fontWeight={700}>
                              Measures
                            </Typography>
                            <Stack direction="row" spacing={1} flexWrap="wrap" useFlexGap>
                              {structurePreview.measures.map((measure: string) => (
                                <Chip key={measure} label={measure} size="small" variant="outlined" />
                              ))}
                            </Stack>
                          </Box>
                        )}
                      </Stack>
                    )}
                  </Paper>
                </TabPanel>

                <TabPanel value={tab} index={2}>
                  <Box sx={{ display: "flex", justifyContent: "flex-end" }}>
                    <IconButton
                      size="small"
                      onClick={() => copyToClipboard(latestMdx)}
                      disabled={!latestMdx}
                      sx={{ color: "white" }}
                    >
                      <ContentCopyIcon fontSize="small" />
                    </IconButton>
                  </Box>

                  <Paper
                    variant="outlined"
                    sx={{
                      p: 2,
                      bgcolor: "#0b1220",
                      color: "white",
                      fontFamily: "Consolas, Monaco, monospace",
                      whiteSpace: "pre-wrap",
                      overflowX: "auto",
                    }}
                  >
                    {latestMdx || "Aucune requête MDX disponible pour le moment."}
                  </Paper>
                </TabPanel>

                <TabPanel value={tab} index={3}>
                  <Box sx={{ display: "flex", justifyContent: "flex-end" }}>
                    <IconButton
                      size="small"
                      onClick={() => copyToClipboard(latestXmla)}
                      disabled={!latestXmla}
                      sx={{ color: "white" }}
                    >
                      <ContentCopyIcon fontSize="small" />
                    </IconButton>
                  </Box>

                  <Paper
                    variant="outlined"
                    sx={{
                      p: 2,
                      bgcolor: "#0b1220",
                      color: "white",
                      fontFamily: "Consolas, Monaco, monospace",
                      whiteSpace: "pre-wrap",
                      overflowX: "auto",
                    }}
                  >
                    {latestXmla || "Aucun script XMLA disponible pour le moment."}
                  </Paper>
                </TabPanel>
              </Box>
            </Paper>
          </Grid>
        </Grid>
      </Box>

      <Snackbar
        open={snack.open}
        autoHideDuration={3500}
        onClose={() => setSnack((s) => ({ ...s, open: false }))}
        anchorOrigin={{ vertical: "bottom", horizontal: "right" }}
      >
        <Alert
          severity={snack.type}
          variant="filled"
          onClose={() => setSnack((s) => ({ ...s, open: false }))}
        >
          {snack.msg}
        </Alert>
      </Snackbar>
    </Box>
  );
}