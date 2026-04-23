import { AppBar, Toolbar, Typography, Box, FormControl, InputLabel, Select, MenuItem, Chip } from "@mui/material";

export default function Navbar({
  dws,
  selectedDw,
  onChangeDw,
}: {
  dws: { id: string; label: string }[];
  selectedDw: string;
  onChangeDw: (v: string) => void;
}) {
  return (
    <AppBar position="sticky">
      <Toolbar>
        <Typography variant="h6" sx={{ fontWeight: 700 }}>
          BI OLAP Agent
        </Typography>

        <Box sx={{ flexGrow: 1 }} />

        <FormControl size="small" sx={{ width: 280, mr: 2, bgcolor: "rgba(255,255,255,0.08)", borderRadius: 1 }}>
          <InputLabel sx={{ color: "white" }}>Select DW</InputLabel>
          <Select
            value={selectedDw}
            label="Select DW"
            onChange={(e) => onChangeDw(e.target.value)}
            sx={{ color: "white" }}
          >
            {dws.map((dw) => (
              <MenuItem key={dw.id} value={dw.id}>
                {dw.label}
              </MenuItem>
            ))}
          </Select>
        </FormControl>

     
      </Toolbar>
    </AppBar>
  );
}