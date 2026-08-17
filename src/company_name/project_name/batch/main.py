from __future__ import annotations

import logging

from company_name.project_name.business.processor import BusinessProcessor
from company_name.project_name.common.exceptions.processing_exception import ProcessingException
from company_name.project_name.config.environment.settings import Settings
from company_name.project_name.data_io.readers.csv_reader import CsvReader
from company_name.project_name.data_io.readers.delta_reader import DeltaReader
from company_name.project_name.data_io.readers.parquet_reader import ParquetReader
from company_name.project_name.data_io.writers.csv_writer import CsvWriter
from company_name.project_name.data_io.writers.delta_writer import DeltaWriter
from company_name.project_name.data_io.writers.parquet_writer import ParquetWriter
from company_name.project_name.data_quality.base_validator import BaseValidator
from company_name.project_name.infrastructure.connectors.api.api_client import ApiClient
from company_name.project_name.infrastructure.spark.spark_session_factory import SparkSessionFactory
from company_name.project_name.load_control.load_control import LoadControl
from company_name.project_name.service.service import Service


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
)
logger = logging.getLogger(__name__)


def _create_reader(spark, data_io_config: dict):
    readers = {
        "csv": CsvReader,
        "parquet": ParquetReader,
        "delta": DeltaReader,
    }
    input_format = data_io_config.get("input_format", "parquet").lower()
    reader_class = readers.get(input_format)
    if reader_class is None:
        raise ProcessingException(f"Formato de entrada não suportado: {input_format}")
    return reader_class(spark=spark, options=data_io_config.get("reader_options", {}))


def _create_writer(data_io_config: dict):
    writers = {
        "csv": CsvWriter,
        "parquet": ParquetWriter,
        "delta": DeltaWriter,
    }
    output_format = data_io_config.get("output_format", "parquet").lower()
    writer_class = writers.get(output_format)
    if writer_class is None:
        raise ProcessingException(f"Formato de saída não suportado: {output_format}")
    return writer_class(
        mode=data_io_config.get("write_mode", "overwrite"),
        options=data_io_config.get("writer_options", {}),
    )


def main() -> None:
    """Monta as dependências e inicia o pipeline.

    Sequência de composição:
    Settings -> Spark -> Reader/Writer -> API/LoadControl ->
    Validator/Business -> Service.run().

    A regra do fluxo fica em Service. O main apenas cria e conecta objetos.
    """
    settings = Settings.load()

    spark = SparkSessionFactory.create(
        app_name=settings.get("app.name", "data-engineering-project"),
        spark_config=settings.get("spark", {}),
    )

    try:
        data_io_config = settings.get("data_io", {})
        reader = _create_reader(spark, data_io_config)
        writer = _create_writer(data_io_config)

        load_control_config = settings.get("load_control", {})
        api_client = None
        if load_control_config.get("enabled", False):
            api_config = settings.get("api", {})
            api_client = ApiClient(
                base_url=api_config["base_url"],
                timeout_seconds=api_config.get("timeout_seconds", 30),
                headers=api_config.get("headers", {}),
            )

        load_control = LoadControl(
            api_client=api_client,
            config=load_control_config,
        )

        # Componentes genéricos. Em projetos concretos, especialize-os quando necessário.
        validator = BaseValidator()
        processor = BusinessProcessor()

        service = Service(
            reader=reader,
            writer=writer,
            validator=validator,
            processor=processor,
            load_control=load_control,
        )

        service.run(
            input_path=settings.require("paths.input"),
            output_path=settings.require("paths.output"),
            execution_context={
                "application": settings.get("app.name", "data-engineering-project")
            },
        )

        data = [
            (1, "Gabriell", 1000.0),
            (2, "Ana", 1500.0),
            (3, "Carlos", 2000.0),
        ]

        columns = ["id", "name", "value"]

        df = spark.createDataFrame(data, columns)

        df.show()
        print("Acabou o teste!!!!!!!!!!!!!")
    finally:
        #spark.stop()
        logger.info("SparkSession finalizada.")


if __name__ == "__main__":
    main()
