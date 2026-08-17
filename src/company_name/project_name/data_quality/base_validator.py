from __future__ import annotations


class BaseValidator:
    """Validador padrão sem regras específicas.

    Projetos concretos podem sobrescrever validate() com suas regras de qualidade.
    """

    def validate(self, dataframe) -> None:
        return None
