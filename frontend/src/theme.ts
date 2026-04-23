import { createTheme } from "@mui/material/styles";

export const theme = createTheme({
  palette: {
    mode: "dark",
    background: {
      default: "#0f172a",
      paper: "#0b1220",
    },
    primary: { main: "#3b82f6" },
    secondary: { main: "#f59e0b" },
  },
  shape: { borderRadius: 12 },
  typography: {
    fontFamily: "system-ui, -apple-system, Segoe UI, Roboto, Arial",
  },
});