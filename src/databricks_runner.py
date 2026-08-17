import os
from pathlib import Path
import sys

def find_project_root() -> Path:
    # Executando como arquivo .py
    if "__file__" in globals():
        current_path = Path(__file__).resolve().parent

    # Executando em notebook / célula Databricks
    else:
        current_path = Path.cwd().resolve()

    # Procura a raiz do projeto subindo os diretórios
    for path in [current_path, *current_path.parents]:
        if (path / "pyproject.toml").exists():
            return path

    raise RuntimeError(
        "Não foi possível localizar a raiz do projeto "
        "(pyproject.toml não encontrado)."
    )


os.environ['PYSPARK_PYTHON'] = sys.executable
os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable
PROJECT_ROOT = find_project_root()

os.environ.setdefault(
    "APP_CONFIG_FILE",
    str(PROJECT_ROOT / "config" / "settings.yaml"),
)

from company_name.project_name.batch.main import main


if __name__ == "__main__":
    main()