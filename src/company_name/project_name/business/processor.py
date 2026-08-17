from __future__ import annotations


class BusinessProcessor:
    """Processador de negócio padrão.

    O template mantém comportamento pass-through para não impor regra específica.
    Projetos concretos podem substituir ou especializar esta classe.
    """

    def process(self, dataframe):
        return dataframe
