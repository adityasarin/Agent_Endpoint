from __future__ import annotations

from pathlib import Path
from typing import Optional

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq


def convert_csvs_to_parquet(
    csv_paths: list[str | Path],
    output_path: str | Path,
    schema_hints: dict[str, str] | None = None,
) -> str:
    """
    Read one or more CSV files and write a single Parquet file.
    Returns the path of the written Parquet file.
    """
    output_path = Path(output_path)
    output_path.parent.mkdir(parents=True, exist_ok=True)

    dfs = []
    for p in csv_paths:
        p = Path(p)
        if p.exists() and p.stat().st_size > 0:
            dfs.append(pd.read_csv(p, low_memory=False))

    if not dfs:
        raise ValueError("No non-empty CSV files found to convert")

    df = pd.concat(dfs, ignore_index=True)

    if schema_hints:
        df = _apply_type_hints(df, schema_hints)

    table = pa.Table.from_pandas(df, preserve_index=False)
    pq.write_table(table, str(output_path), compression="snappy")
    return str(output_path)


def _apply_type_hints(df: pd.DataFrame, hints: dict[str, str]) -> pd.DataFrame:
    for col, type_hint in hints.items():
        if col not in df.columns:
            continue
        try:
            if type_hint == "datetime":
                df[col] = pd.to_datetime(df[col], errors="coerce", utc=True)
            elif type_hint == "integer":
                df[col] = pd.to_numeric(df[col], errors="coerce").astype("Int64")
            elif type_hint == "float":
                df[col] = pd.to_numeric(df[col], errors="coerce")
            elif type_hint == "boolean":
                df[col] = df[col].map({"true": True, "false": False, "1": True, "0": False}).astype("boolean")
        except Exception:
            pass
    return df


def get_parquet_preview(parquet_path: str | Path, n_rows: int = 5) -> dict:
    """Return a preview dict for display."""
    path = Path(parquet_path)
    if not path.exists():
        return {"error": f"File not found: {path}"}
    pf = pq.ParquetFile(str(path))
    meta = pf.metadata
    schema = pf.schema_arrow
    sample = pf.read_row_group(0).to_pydict()
    preview_rows = {k: v[:n_rows] for k, v in sample.items()}
    return {
        "num_rows": meta.num_rows,
        "num_columns": len(schema),
        "columns": [{"name": schema.field(i).name, "type": str(schema.field(i).type)} for i in range(len(schema))],
        "preview_rows": preview_rows,
        "file_size_bytes": path.stat().st_size,
    }
