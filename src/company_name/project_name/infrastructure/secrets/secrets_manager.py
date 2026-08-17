from __future__ import annotations

import json
from typing import Any

import boto3


class SecretsManager:
    """Acesso genérico ao AWS Secrets Manager."""

    def __init__(self, region_name: str | None = None):
        self.client = boto3.client("secretsmanager", region_name=region_name)

    def get_secret(self, secret_id: str) -> dict[str, Any] | str:
        response = self.client.get_secret_value(SecretId=secret_id)
        secret_string = response.get("SecretString", "")

        try:
            return json.loads(secret_string)
        except json.JSONDecodeError:
            return secret_string
