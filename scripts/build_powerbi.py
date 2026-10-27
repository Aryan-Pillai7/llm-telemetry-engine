#!/usr/bin/env python3
"""Generate the Cold Tier Analytics report as a Power BI Project, then open it.

    python tasks.py powerbi

A Power BI Project (.pbip) is a folder of text files -- data model, measures,
report layout, theme -- that Power BI Desktop opens like a .pbix. Generating it
means the report is built by code rather than by hand, and the path to the
Parquet lake is filled in from wherever this repo actually lives.

The output goes to bi/powerbi/build/ and is not committed: it contains an
absolute path to this machine's data, and it is cheap to regenerate.

The model ships without data. Power BI loads it from the Parquet files on the
first Refresh.
"""

from __future__ import annotations

import json
import os
import shutil
import sys
from pathlib import Path
from typing import Any

REPO_ROOT = Path(__file__).resolve().parents[1]
SOURCE = REPO_ROOT / "bi" / "powerbi"
OUT = SOURCE / "build"
NAME = "Cold Tier Analytics"
TABLE = "spans"

# --- Semantic model -------------------------------------------------------------

COLUMNS = [
    ("ts", "dateTime", "General Date"),
    ("tenant_id", "string", None),
    ("model", "string", None),
    ("status_class", "string", None),
    ("input_tokens", "int64", "#,0"),
    ("output_tokens", "int64", "#,0"),
    ("ttft_ms", "double", "0.0"),
    ("tokens", "int64", "#,0"),
    ("date", "dateTime", "yyyy-mm-dd"),
]

MEASURES = [
    ("Total Spans", "COUNTROWS ( spans )", "#,0"),
    ("Total Tokens", "SUM ( spans[tokens] )", "#,0"),
    ("Tenants", "DISTINCTCOUNT ( spans[tenant_id] )", "#,0"),
    # 95% of LLM calls produced their first token faster than this. Tool-call
    # spans have a blank TTFT (see spans.pq) and are skipped.
    (
        "p95 TTFT (ms)",
        "PERCENTILEX.INC ( FILTER ( spans, NOT ISBLANK ( spans[ttft_ms] ) ), spans[ttft_ms], 0.95 )",
        "#,0.0",
    ),
    (
        "Error %",
        'DIVIDE ( CALCULATE ( COUNTROWS ( spans ), spans[status_class] <> "ok" ), COUNTROWS ( spans ) )',
        "0.00%",
    ),
    # Blank outside the top 10, and a bar chart drops blank rows -- a top-N
    # chart without relying on a visual-level filter.
    (
        "Tokens (top 10 tenants)",
        "IF ( RANKX ( ALLSELECTED ( spans[tenant_id] ), [Total Tokens] ) <= 10, [Total Tokens] )",
        "#,0",
    ),
]


def m_query() -> list[str]:
    """spans.pq with the lake path pointed at this checkout."""
    lake = REPO_ROOT / "data" / "cold"
    lines = (SOURCE / "spans.pq").read_text(encoding="utf-8-sig").splitlines()
    out = []
    for line in lines:
        if line.lstrip().startswith("LakeFolder ="):
            indent = line[: len(line) - len(line.lstrip())]
            line = f'{indent}LakeFolder = "{lake}",'
        out.append(line)
    return out


def model_bim() -> dict[str, Any]:
    return {
        "compatibilityLevel": 1550,
        "model": {
            "culture": "en-US",
            "dataAccessOptions": {"legacyRedirects": True, "returnErrorValuesAsNull": True},
            "defaultPowerBIDataSourceVersion": "powerBI_V3",
            "tables": [
                {
                    "name": TABLE,
                    "columns": [
                        {
                            "name": name,
                            "dataType": dtype,
                            "sourceColumn": name,
                            "summarizeBy": "none",
                            **({"formatString": fmt} if fmt else {}),
                        }
                        for name, dtype, fmt in COLUMNS
                    ],
                    "measures": [
                        {"name": name, "expression": expr, "formatString": fmt}
                        for name, expr, fmt in MEASURES
                    ],
                    "partitions": [
                        {
                            "name": TABLE,
                            "mode": "import",
                            "source": {"type": "m", "expression": m_query()},
                        }
                    ],
                }
            ],
            "annotations": [{"name": "PBI_QueryOrder", "value": json.dumps([TABLE])}],
        },
    }


# --- Report layout --------------------------------------------------------------


def literal(value: Any) -> dict[str, Any]:
    text = f"'{value}'" if isinstance(value, str) else str(value).lower()
    return {"expr": {"Literal": {"Value": text}}}


def field(name: str) -> dict[str, Any]:
    kind = "Measure" if name in {m[0] for m in MEASURES} else "Column"
    return {kind: {"Expression": {"SourceRef": {"Source": "s"}}, "Property": name}}


def visual(
    vid: str,
    vtype: str,
    pos: tuple[int, int, int, int],
    roles: dict[str, list[str]],
    title: str | None = None,
    *,
    sort_desc: str | None = None,
    objects: dict[str, Any] | None = None,
) -> dict[str, Any]:
    x, y, w, h = pos
    names = [n for fields in roles.values() for n in fields]
    query: dict[str, Any] = {
        "Version": 2,
        "From": [{"Name": "s", "Entity": TABLE, "Type": 0}],
        "Select": [{**field(n), "Name": f"{TABLE}.{n}"} for n in dict.fromkeys(names)],
    }
    if sort_desc:
        query["OrderBy"] = [{"Direction": 2, "Expression": field(sort_desc)}]

    vc_objects: dict[str, Any] = {}
    if title:
        vc_objects["title"] = [
            {
                "properties": {
                    "show": literal(True),
                    "text": literal(title),
                    "fontSize": {"expr": {"Literal": {"Value": "13D"}}},
                    "fontColor": {"solid": {"color": literal("#1C2230")}},
                }
            }
        ]

    config = {
        "name": vid,
        "layouts": [{"id": 0, "position": {"x": x, "y": y, "z": 0, "width": w, "height": h}}],
        "singleVisual": {
            "visualType": vtype,
            "projections": {
                role: [{"queryRef": f"{TABLE}.{n}"} for n in fields]
                for role, fields in roles.items()
            },
            "prototypeQuery": query,
            "objects": objects or {},
            "vcObjects": vc_objects,
        },
    }
    return {
        "x": x,
        "y": y,
        "z": 0,
        "width": w,
        "height": h,
        "config": json.dumps(config),
        "filters": "[]",
    }


def textbox(vid: str, pos: tuple[int, int, int, int], text: str) -> dict[str, Any]:
    x, y, w, h = pos
    config = {
        "name": vid,
        "layouts": [{"id": 0, "position": {"x": x, "y": y, "z": 0, "width": w, "height": h}}],
        "singleVisual": {
            "visualType": "textbox",
            "drillFilterOtherVisuals": True,
            "objects": {
                "general": [
                    {
                        "properties": {
                            "paragraphs": [
                                {
                                    "textRuns": [
                                        {
                                            "value": text,
                                            "textStyle": {"fontWeight": "bold", "fontSize": "18pt"},
                                        }
                                    ]
                                }
                            ]
                        }
                    }
                ]
            },
        },
    }
    return {
        "x": x,
        "y": y,
        "z": 0,
        "width": w,
        "height": h,
        "config": json.dumps(config),
        "filters": "[]",
    }


def slicer_mode(mode: str) -> dict[str, Any]:
    return {"data": [{"properties": {"mode": literal(mode)}}]}


def full_number() -> dict[str, Any]:
    """Card label without auto units: 1,082,377 rather than a rounded 1M."""
    return {
        "labels": [{"properties": {"labelDisplayUnits": {"expr": {"Literal": {"Value": "1D"}}}}}]
    }


def readable_table() -> dict[str, Any]:
    """Table text at presentation size; the default is too small to read on a projector."""
    size = {"fontSize": {"expr": {"Literal": {"Value": "12D"}}}}
    return {part: [{"properties": size}] for part in ("columnHeaders", "values", "total")}


def report_json(theme_name: str) -> dict[str, Any]:
    # 1280x720 page, three bands: headline numbers, who/what, how well.
    visuals = [
        textbox("title", (20, 6, 560, 56), NAME),
        visual("slicer_model", "slicer", (940, 4, 320, 52), {"Values": ["model"]},
               objects=slicer_mode("Dropdown")),
        visual("card_spans", "card", (20, 64, 220, 100), {"Values": ["Total Spans"]},
               objects=full_number()),
        visual("card_tokens", "card", (250, 64, 220, 100), {"Values": ["Total Tokens"]}),
        visual("card_tenants", "card", (480, 64, 220, 100), {"Values": ["Tenants"]},
               objects=full_number()),
        visual("card_ttft", "card", (710, 64, 220, 100), {"Values": ["p95 TTFT (ms)"]},
               objects=full_number()),
        visual("slicer_date", "slicer", (940, 64, 320, 100), {"Values": ["date"]},
               objects=slicer_mode("Between")),
        visual("tokens_by_model", "clusteredColumnChart", (20, 174, 610, 300),
               {"Category": ["model"], "Y": ["Total Tokens"]},
               "Tokens by model", sort_desc="Total Tokens"),
        visual("top_tenants", "clusteredBarChart", (640, 174, 620, 300),
               {"Category": ["tenant_id"], "Y": ["Tokens (top 10 tenants)"]},
               "Top 10 tenants by tokens", sort_desc="Tokens (top 10 tenants)"),
        visual("model_table", "tableEx", (20, 484, 900, 226),
               {"Values": ["model", "Total Spans", "p95 TTFT (ms)", "Error %"]},
               "Model performance", sort_desc="Total Spans", objects=readable_table()),
        visual("status_donut", "donutChart", (930, 484, 330, 226),
               {"Category": ["status_class"], "Y": ["Total Spans"]},
               "Spans by status"),
    ]  # fmt: skip

    config = {
        "version": "5.43",
        "themeCollection": {"customTheme": {"name": theme_name, "version": "5.43", "type": 1}},
        "activeSectionIndex": 0,
        "linguisticSchemaSyncVersion": 0,
    }
    return {
        "config": json.dumps(config),
        "layoutOptimization": 0,
        "resourcePackages": [
            {
                "resourcePackage": {
                    "name": "RegisteredResources",
                    "type": 1,
                    "disabled": False,
                    "items": [{"name": theme_name, "path": theme_name, "type": 202}],
                }
            }
        ],
        "sections": [
            {
                "name": "ReportSection",
                "displayName": NAME,
                "displayOption": 1,
                "width": 1280,
                "height": 720,
                "config": "{}",
                "filters": "[]",
                "visualContainers": visuals,
            }
        ],
        "filters": "[]",
    }


# --- Write --------------------------------------------------------------------


def write_json(path: Path, data: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(data, indent=2), encoding="utf-8")


def build() -> Path:
    # Regenerate from scratch: a stale visual left behind by an older build
    # would survive silently in a folder Power BI reads wholesale.
    if OUT.exists():
        shutil.rmtree(OUT)

    report_dir = OUT / f"{NAME}.Report"
    model_dir = OUT / f"{NAME}.SemanticModel"
    theme_name = "TelemetryTheme.json"

    write_json(
        OUT / f"{NAME}.pbip",
        {"version": "1.0", "artifacts": [{"report": {"path": report_dir.name}}],
         "settings": {"enableAutoRecovery": True}},
    )  # fmt: skip
    write_json(
        report_dir / "definition.pbir",
        {"version": "1.0", "datasetReference": {"byPath": {"path": f"../{model_dir.name}"}}},
    )
    write_json(report_dir / "report.json", report_json(theme_name))
    resources = report_dir / "StaticResources" / "RegisteredResources"
    resources.mkdir(parents=True)
    shutil.copyfile(SOURCE / "theme.json", resources / theme_name)
    write_json(model_dir / "definition.pbism", {"version": "1.0", "settings": {}})
    write_json(model_dir / "model.bim", model_bim())
    return OUT / f"{NAME}.pbip"


def main() -> int:
    if not any((REPO_ROOT / "data" / "cold").rglob("*.parquet")):
        print("no Parquet files in data/cold yet; run `python tasks.py demo` first")
        return 1
    pbip = build()
    print(f"  built  {pbip}")
    if "--no-open" not in sys.argv and sys.platform == "win32":
        os.startfile(pbip)  # type: ignore[attr-defined]
        print("  opening in Power BI Desktop: click Home > Refresh to load the data")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
