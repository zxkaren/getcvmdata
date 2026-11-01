from pathlib import Path

from decouple import config


RAW_DATA_DIRECTORY = config("RAW_DATA_DIRECTORY")


def get_project_root() -> Path:
    """Localiza a raiz do projeto pelo arquivo pyproject.toml."""
    for directory in Path(__file__).resolve().parents:
        if (directory / "pyproject.toml").is_file():
            return directory

    raise FileNotFoundError(
        "Não foi possível localizar o arquivo pyproject.toml."
    )


def get_target_directory(source_name: str) -> Path:
    """Cria e retorna o diretório de extrações de uma fonte."""
    if not source_name.isidentifier():
        raise ValueError(
            "O nome da fonte deve ser um identificador válido."
        )

    target_directory = (
        get_project_root()
        / RAW_DATA_DIRECTORY
        / source_name
    )

    target_directory.mkdir(parents=True, exist_ok=True)

    return target_directory
