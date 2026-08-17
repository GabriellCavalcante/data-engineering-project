from __future__ import annotations

from typing import Any

import requests


class ApiClient:
    """Cliente HTTP genérico para integrações GET/POST."""

    def __init__(
        self,
        base_url: str,
        timeout_seconds: int = 30,
        headers: dict[str, str] | None = None,
    ):
        self.base_url = base_url.rstrip("/")
        self.timeout_seconds = timeout_seconds
        self.session = requests.Session()
        self.session.headers.update(headers or {})

    def get(
        self,
        endpoint: str,
        params: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        response = self.session.get(
            self._url(endpoint),
            params=params,
            timeout=self.timeout_seconds,
        )
        response.raise_for_status()
        return response.json()

    def post(
        self,
        endpoint: str,
        json: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        response = self.session.post(
            self._url(endpoint),
            json=json,
            timeout=self.timeout_seconds,
        )
        response.raise_for_status()
        return response.json() if response.content else {}

    def _url(self, endpoint: str) -> str:
        return f"{self.base_url}/{endpoint.lstrip('/')}"
