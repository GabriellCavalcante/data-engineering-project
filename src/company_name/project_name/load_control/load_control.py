from __future__ import annotations

from typing import Any

from company_name.project_name.common.exceptions.processing_exception import ProcessingException


class LoadControl:
    """Regras sistêmicas para autorização e registro da execução.

    Quando habilitado, toda persistência/consulta ocorre via API.
    O pipeline não acessa o banco de controle diretamente.
    """

    def __init__(self, api_client, config: dict[str, Any] | None = None):
        self.api_client = api_client
        self.config = config or {}
        self.enabled = self.config.get("enabled", False)

        if self.enabled and self.api_client is None:
            raise ProcessingException(
                "Load control está habilitado, mas nenhum ApiClient foi informado."
            )

    def can_start(self, context: dict[str, Any]) -> bool:
        if not self.enabled:
            return True

        response = self.api_client.get(
            self._endpoint("can_start"),
            params=context,
        )
        field = self.config.get("response_fields", {}).get("can_start", "can_start")
        return bool(response.get(field, False))

    def start(self, context: dict[str, Any]) -> str | None:
        if not self.enabled:
            return None

        response = self.api_client.post(
            self._endpoint("start"),
            json=context,
        )
        field = self.config.get("response_fields", {}).get("run_id", "run_id")
        run_id = response.get(field)
        if run_id is None:
            raise ProcessingException(
                f"A API de controle de carga não retornou o campo '{field}'."
            )
        return str(run_id)

    def mark_success(
        self,
        run_id: str | None,
        context: dict[str, Any] | None = None,
    ) -> None:
        if not self.enabled:
            return

        self.api_client.post(
            self._endpoint("success"),
            json={"run_id": run_id, "context": context or {}},
        )

    def mark_failure(
        self,
        run_id: str | None,
        error: str,
        context: dict[str, Any] | None = None,
    ) -> None:
        if not self.enabled:
            return

        self.api_client.post(
            self._endpoint("failure"),
            json={"run_id": run_id, "error": error, "context": context or {}},
        )

    def _endpoint(self, name: str) -> str:
        endpoint = self.config.get("endpoints", {}).get(name)
        if not endpoint:
            raise ProcessingException(
                f"Endpoint de load control não configurado: {name}"
            )
        return endpoint
