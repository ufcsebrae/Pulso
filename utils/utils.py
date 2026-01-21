# utils.py
import logging
from pathlib import Path

logger = logging.getLogger(__name__)


def carregar_script_sql(caminho_arquivo: str | Path) -> str:
    """
    Lê e retorna o conteúdo de um arquivo de script SQL.

    Args:
        caminho_arquivo: O caminho (string ou objeto Path) para o arquivo .sql.

    Returns:
        O conteúdo do arquivo como uma string.

    Raises:
        FileNotFoundError: Se o arquivo não for encontrado no caminho especificado.
    """
    try:
        return Path(caminho_arquivo).read_text(encoding="utf-8")
    except FileNotFoundError as e:
        logger.error("Arquivo de query não encontrado em '%s'", caminho_arquivo)
        # Relança a exceção para que a camada superior possa decidir como lidar com o erro.
        raise e

