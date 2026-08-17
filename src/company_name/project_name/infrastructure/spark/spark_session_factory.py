from __future__ import annotations

from pyspark.sql import SparkSession


class SparkSessionFactory:
    """Cria SparkSession a partir de configuração externa."""

    @staticmethod
    def create(app_name: str, spark_config: dict | None = None) -> SparkSession:
        config = spark_config or {}
        builder = SparkSession.builder.appName(app_name)

        master = config.get("master")
        if master:
            builder = builder.master(master)

        for key, value in config.get("config", {}).items():
            builder = builder.config(key, value)

        return builder.getOrCreate()
