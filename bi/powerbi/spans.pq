// Power Query (M): load the Parquet cold tier into Power BI as one table, "spans".
//
// Reads every file under data/cold/dt=*/ directly. No export step, no copy:
// the archive is an open format, so a BI tool queries it as-is.
//
// Only the columns the report uses are kept, which keeps the model small.
let
    // Change this if the repo lives somewhere else.
    LakeFolder = "C:\Users\aryan\llm-telemetry-engine\data\cold",

    Files = Folder.Files(LakeFolder),
    // Only finished files. The exporter writes to a temporary name first and
    // renames on success, so anything else is a half-written export.
    ParquetFiles = Table.SelectRows(Files, each [Extension] = ".parquet"),
    Loaded = Table.AddColumn(ParquetFiles, "Data", each Parquet.Document([Content])),
    Combined = Table.Combine(Loaded[Data]),

    Kept = Table.SelectColumns(
        Combined,
        {"ts", "tenant_id", "model", "status_class", "input_tokens", "output_tokens", "ttft_ms"}
    ),
    Typed = Table.TransformColumnTypes(
        Kept,
        {
            {"ts", type datetime},
            {"tenant_id", type text},
            {"model", type text},
            {"status_class", type text},
            {"input_tokens", Int64.Type},
            {"output_tokens", Int64.Type},
            {"ttft_ms", type number}
        }
    ),

    // Spans with no model are an agent calling a tool (search, SQL, code).
    // They have no tokens and no time-to-first-token, so TTFT is blanked rather
    // than left at 0, which would pull every latency figure down.
    Labelled = Table.ReplaceValue(Typed, "", "agent tool call", Replacer.ReplaceValue, {"model"}),
    NoToolTtft = Table.ReplaceValue(
        Labelled,
        each [ttft_ms],
        each if [model] = "agent tool call" then null else [ttft_ms],
        Replacer.ReplaceValue,
        {"ttft_ms"}
    ),

    WithTokens = Table.AddColumn(NoToolTtft, "tokens", each [input_tokens] + [output_tokens], Int64.Type),
    WithDate = Table.AddColumn(WithTokens, "date", each DateTime.Date([ts]), type date)
in
    WithDate
