from __future__ import annotations

import logging
from typing import Any

from company_name.project_name.common.exceptions.processing_exception import ProcessingException


logger = logging.getLogger(__name__)


class Service:
    """Orquestrador principal do pipeline.

    Ordem padrão:
    1. valida controle de carga;
    2. registra início;
    3. lê os dados;
    4. valida os dados;
    5. aplica regras de negócio;
    6. grava os dados;
    7. registra sucesso ou falha.

    Os componentes executam responsabilidades específicas; este serviço define
    a sequência em que eles são chamados.
    """

    def __init__(self, reader, writer, validator, processor, load_control):
        self.reader = reader
        self.writer = writer
        self.validator = validator
        self.processor = processor
        self.load_control = load_control

    def run(
        self,
        input_path: str,
        output_path: str,
        execution_context: dict[str, Any] | None = None,
    ) -> None:
        context = execution_context or {}

        logger.info("1/7 - Validando controle de carga.")
        if not self.load_control.can_start(context):
            raise ProcessingException("O controle de carga não autorizou a execução.")

        logger.info("2/7 - Registrando início da carga.")
        run_id = self.load_control.start(context)

        try:
            logger.info("3/7 - Lendo dados de entrada.")
            #dataframe = self.reader.read(input_path)

            logger.info("4/7 - Executando validações de qualidade.")
            #self.validator.validate(dataframe)

            logger.info("5/7 - Aplicando regras de negócio.")
            #result = self.processor.process(dataframe)

            logger.info("6/7 - Gravando dados de saída.")
            #self.writer.write(result, output_path)

            logger.info("7/7 - Registrando sucesso da carga.")
            self.load_control.mark_success(run_id, context)
        except Exception as exc:
            logger.exception("Falha durante o processamento.")
            self.load_control.mark_failure(run_id, str(exc), context)
            raise
