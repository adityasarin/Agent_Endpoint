from __future__ import annotations

import csv
import os
from pathlib import Path
from typing import Optional


class CSVWriter:
    """
    Streaming CSV writer. One file per extraction window.
    Calls fsync after every batch for crash safety.
    """

    def __init__(self, file_path: str | Path, fields: list[str] | None = None):
        self._path = Path(file_path)
        self._path.parent.mkdir(parents=True, exist_ok=True)
        self._fields = fields
        self._file = None
        self._writer = None
        self._rows_written = 0

    def open(self, append: bool = False) -> "CSVWriter":
        mode = "a" if append and self._path.exists() else "w"
        self._file = self._path.open(mode, newline="", encoding="utf-8")
        # Fields will be set on first write if not provided
        if self._fields:
            self._writer = csv.DictWriter(
                self._file,
                fieldnames=self._fields,
                extrasaction="ignore",
                lineterminator="\n",
            )
            if mode == "w":
                self._writer.writeheader()
        return self

    def write_batch(self, records: list[dict]) -> int:
        if not records:
            return 0

        if self._writer is None:
            # Infer fields from first batch
            self._fields = list(records[0].keys())
            self._writer = csv.DictWriter(
                self._file,
                fieldnames=self._fields,
                extrasaction="ignore",
                lineterminator="\n",
            )
            # Only write header if file is new (position == 0)
            if self._file.tell() == 0:
                self._writer.writeheader()

        self._writer.writerows(records)
        self._file.flush()
        os.fsync(self._file.fileno())
        self._rows_written += len(records)
        return len(records)

    def close(self) -> None:
        if self._file and not self._file.closed:
            self._file.close()

    def __enter__(self) -> "CSVWriter":
        return self.open()

    def __exit__(self, *_) -> None:
        self.close()

    @property
    def rows_written(self) -> int:
        return self._rows_written

    @property
    def file_path(self) -> str:
        return str(self._path)


def build_window_csv_path(output_dir: str, pipeline_id: str, window_start_unix: int) -> Path:
    return Path(output_dir) / f"{pipeline_id}_{window_start_unix}.csv"
