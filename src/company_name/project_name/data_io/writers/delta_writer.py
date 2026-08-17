from __future__ import annotations


class DeltaWriter:
    def __init__(self, mode: str = "overwrite", options: dict | None = None):
        self.mode = mode
        self.options = options or {}

    def write(self, dataframe, path: str) -> None:
        (
            dataframe.write
            .format("delta")
            .mode(self.mode)
            .options(**self.options)
            .save(path)
        )
