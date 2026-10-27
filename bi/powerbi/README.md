# Power BI: Cold Tier Analytics

One report page built on the **Parquet cold tier** (`data/cold/`). The web
dashboard (`python tasks.py web`) shows data arriving *now*; this page shows
what was *kept* and what it says when analysed afterwards.

**The point to make when presenting:** Power BI reads the archive files
directly. Nothing is exported or copied for it, because the cold tier is an open
format that any tool can query, not only this project's code.

## What's on the page

| Visual | Answers |
|---|---|
| 4 cards: Total Spans · Total Tokens · Tenants · p95 TTFT | How big is the archive? |
| Column chart: tokens by model | Which models do the most work? |
| Bar chart: top 10 tenants by tokens | Who uses the most? |
| Donut: spans by status | How many requests failed, and how? |
| Table: model · spans · p95 TTFT · error % | How does each model perform? |
| Slicers: date, model | Filter everything above |

## Files here

| File | What it is |
|---|---|
| `spans.pq` | The Power Query that loads the Parquet files |
| `measures.dax` | The five measures |
| `theme.json` | Colours matching the web dashboard |

## Build it (about 15 minutes)

Run `python tasks.py demo` first so `data/cold/` has files in it.

### 1. Load the data
1. Open Power BI Desktop and start a **Blank report**.
2. **Home > Get data > Blank query**. The Power Query editor opens.
3. **Home > Advanced Editor**, delete everything, paste the whole of
   `spans.pq`, and click **Done**. If the repo is not at
   `C:\Users\aryan\llm-telemetry-engine`, edit the `LakeFolder` line.
4. In the right-hand panel, rename the query from `Query1` to **`spans`**.
5. **Home > Close & Apply**. Expect a few seconds per million rows.

### 2. Add the measures
For each line in `measures.dax`: select the `spans` table in the **Data** pane,
then **Home > New measure**, paste the line and press Enter. You should have
five measures. Then set formats:
- `Error %`: **Measure tools > Format > Percentage**, 2 decimals.
- `Total Spans`, `Total Tokens`: Whole number with thousands separator.

### 3. Apply the theme
**View > Themes > Browse for themes**, then pick `theme.json`.

### 4. Build the page
Use the canvas at the default 16:9. Suggested layout:

```
+-----------+-----------+-----------+-----------+------------------+
|  Total    |  Total    |  Tenants  |  p95 TTFT |  [Date slicer]   |
|  Spans    |  Tokens   |           |   (ms)    |  [Model slicer]  |
+-----------+-----------+-----------+-----------+------------------+
|  Tokens by model           |  Top 10 tenants by tokens           |
|  (column chart)            |  (bar chart)                        |
+----------------------------+-------------------+-----------------+
|  Model performance (table)                     | Spans by status |
|                                                | (donut)         |
+------------------------------------------------+-----------------+
```

| Visual | Type | Fields |
|---|---|---|
| Cards ×4 | **Card** | One measure each: `Total Spans`, `Total Tokens`, `Tenants`, `p95 TTFT (ms)` |
| Tokens by model | **Clustered column chart** | X-axis `model`, Y-axis `Total Tokens` |
| Top 10 tenants | **Clustered bar chart** | Y-axis `tenant_id`, X-axis `Total Tokens`. In the **Filters** pane on `tenant_id`, set **Filter type: Top N**, Show items **Top 10**, By value `Total Tokens`, then **Apply filter** |
| Spans by status | **Donut chart** | Legend `status_class`, Values `Total Spans` |
| Model performance | **Table** | `model`, `Total Spans`, `p95 TTFT (ms)`, `Error %` |
| Date slicer | **Slicer** | `date` (switch style to *Between*) |
| Model slicer | **Slicer** | `model` (style *Dropdown*) |

Give each chart a plain title (Format > General > Title), e.g.
"Tokens by model", and add a page title text box: **Cold Tier Analytics**.

Save as `cold-tier-analytics.pbix` anywhere you like. `.pbix` files are not
committed, because they embed a copy of the data.

### 5. Refresh after new demo runs
**Home > Refresh**. New Parquet files are picked up automatically.

## Check your numbers

The report should agree with DuckDB reading the same files. With the slicers
cleared, run:

```bash
telemetry-engine cold query "SELECT count(*) AS spans, sum(input_tokens + output_tokens) AS tokens, count(DISTINCT tenant_id) AS tenants, round(quantile_cont(ttft_ms, 0.95) FILTER (WHERE model <> ''), 1) AS p95_ttft_ms FROM spans"
```

The four values match the four cards. If they don't, the usual cause is a
filter left on a visual or the page.

## How to explain it in one breath

> "Hot data lives in ClickHouse for 48 hours. Before it expires we archive it as
> Parquet files, sorted for fast filtering. Power BI reads those files directly,
> so this is the long-term view: which models and tenants used the most tokens,
> how fast the models were, and how many requests failed."
