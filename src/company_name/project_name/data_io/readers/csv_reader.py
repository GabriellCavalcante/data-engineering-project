from __future__ import annotations


class CsvReader:
    def __init__(self, spark, options: dict | None = None):
        self.spark = spark
        self.options = options or {}

    def read(self, path: str):
        return self.spark.read.options(**self.options).csv(path)
