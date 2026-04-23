from __future__ import annotations

from typing import Optional

from models import CubeFact, CubeMeasure, CubeDimension, DetectedHierarchy


def _safe_xml(value: Optional[str]) -> str:
    if value is None:
        return ""
    return (
        str(value)
        .replace("&", "&amp;")
        .replace("<", "&lt;")
        .replace(">", "&gt;")
        .replace('"', "&quot;")
        .replace("'", "&apos;")
    )


def _measure_datatype(measure: CubeMeasure) -> str:
    name = (measure.name or "").lower()
    source = (measure.source_column or "").lower()
    text = f"{name} {source}"

    if any(k in text for k in ["count", "qty", "quantity", "key", "id", "days"]):
        return "Integer"
    if any(k in text for k in ["amount", "cost", "price", "rate", "total", "avg", "average"]):
        return "Double"
    return "Double"


def _aggregate_function(agg: str | None) -> str:
    a = (agg or "sum").strip().lower()
    mapping = {
        "sum": "Sum",
        "count": "Count",
        "avg": "AverageOfChildren",
        "average": "AverageOfChildren",
        "min": "Min",
        "max": "Max",
    }
    return mapping.get(a, "Sum")


def _measure_id(fact_name: str, measure_name: str) -> str:
    raw = f"{fact_name}_{measure_name}"
    return "".join(ch if ch.isalnum() else "_" for ch in raw).strip("_")


def generate_xmla_alter_add_measure(
    ssas_database: str,
    cube_name: str,
    fact: CubeFact,
    measure: CubeMeasure,
) -> str:
    measure_id = _measure_id(fact.name, measure.name)
    data_type = _measure_datatype(measure)
    agg = _aggregate_function(measure.aggregation)

    if measure.is_calculated and measure.expression:
        return f"""<Alter xmlns="http://schemas.microsoft.com/analysisservices/2003/engine" AllowCreate="false">
  <Object>
    <DatabaseID>{_safe_xml(ssas_database)}</DatabaseID>
    <CubeID>{_safe_xml(cube_name)}</CubeID>
    <MdxScriptID>MdxScript</MdxScriptID>
  </Object>
  <ObjectDefinition>
    <MdxScript>
      <ID>MdxScript</ID>
      <Name>MdxScript</Name>
      <Commands>
        <Command>
          <Text>
CALCULATE;
CREATE MEMBER CURRENTCUBE.[Measures].[{_safe_xml(measure.name)}]
 AS {measure.expression};
          </Text>
        </Command>
      </Commands>
    </MdxScript>
  </ObjectDefinition>
</Alter>"""

    agg_xml = f"<AggregateFunction>{agg}</AggregateFunction>" if agg != "Sum" else ""

    return f"""<Alter xmlns="http://schemas.microsoft.com/analysisservices/2003/engine" AllowCreate="false">
  <Object>
    <DatabaseID>{_safe_xml(ssas_database)}</DatabaseID>
    <CubeID>{_safe_xml(cube_name)}</CubeID>
    <MeasureGroupID>{_safe_xml(fact.name)}</MeasureGroupID>
  </Object>
  <ObjectDefinition>
    <MeasureGroup>
      <ID>{_safe_xml(fact.name)}</ID>
      <Name>{_safe_xml(fact.name)}</Name>
      <Measures>
        <Measure>
          <ID>{_safe_xml(measure_id)}</ID>
          <Name>{_safe_xml(measure.name)}</Name>
          {agg_xml}
          <DataType>{_safe_xml(data_type)}</DataType>
          <Source>
            <DataType>{_safe_xml(data_type)}</DataType>
            <Source xsi:type="ColumnBinding"
                    xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance">
              <TableID>{_safe_xml(fact.source_table)}</TableID>
              <ColumnID>{_safe_xml(measure.source_column)}</ColumnID>
            </Source>
          </Source>
        </Measure>
      </Measures>
    </MeasureGroup>
  </ObjectDefinition>
</Alter>"""

def _build_hierarchy_xml(hierarchy: DetectedHierarchy) -> str:
    levels_xml = []
    for lvl in hierarchy.levels:
        levels_xml.append(f"""
        <Level>
          <ID>{_safe_xml(lvl.name)}</ID>
          <Name>{_safe_xml(lvl.name)}</Name>
          <SourceAttributeID>{_safe_xml(lvl.source_column)}</SourceAttributeID>
        </Level>""")

    return f"""
    <Hierarchy>
      <ID>{_safe_xml(hierarchy.name)}</ID>
      <Name>{_safe_xml(hierarchy.name)}</Name>
      <Levels>
        {''.join(levels_xml)}
      </Levels>
    </Hierarchy>"""


def generate_xmla_alter_modify_dimension(
    ssas_database: str,
    dimension: CubeDimension,
) -> str:
    hierarchies_xml = "".join(
        _build_hierarchy_xml(h) for h in dimension.hierarchies
    )

    attrs_xml = []
    for attr in dimension.attributes:
        attrs_xml.append(f"""
        <Attribute>
          <ID>{_safe_xml(attr.name)}</ID>
          <Name>{_safe_xml(attr.name)}</Name>
        </Attribute>""")

    return f"""<Alter xmlns="http://schemas.microsoft.com/analysisservices/2003/engine" AllowCreate="false">
  <Object>
    <DatabaseID>{_safe_xml(ssas_database)}</DatabaseID>
    <DimensionID>{_safe_xml(dimension.name)}</DimensionID>
  </Object>
  <ObjectDefinition>
    <Dimension>
      <ID>{_safe_xml(dimension.name)}</ID>
      <Name>{_safe_xml(dimension.name)}</Name>
      <Attributes>
        {''.join(attrs_xml)}
      </Attributes>
      <Hierarchies>
        {hierarchies_xml}
      </Hierarchies>
    </Dimension>
  </ObjectDefinition>
</Alter>"""