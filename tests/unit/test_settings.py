from pathlib import Path

from company_name.project_name.config.environment.settings import Settings


def test_settings_reads_nested_values(tmp_path: Path):
    config_file = tmp_path / "settings.yaml"
    config_file.write_text(
        "paths:\n  input: s3://bucket/input/\n",
        encoding="utf-8",
    )

    settings = Settings.load(config_file)

    assert settings.require("paths.input") == "s3://bucket/input/"
