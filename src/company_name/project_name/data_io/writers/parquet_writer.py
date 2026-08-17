from __future__ import annotations


class ParquetWriter:
    def __init__(self, mode: str = "overwrite", options: dict | None = None):
        self.mode = mode
        self.options = options or {}

    def write(self, dataframe, path: str) -> None:
        (
            dataframe.write
            .mode(self.mode)
            .options(**self.options)
            .parquet(path)
        )
