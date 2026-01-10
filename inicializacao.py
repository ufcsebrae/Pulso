# inicializacao.py
import logging

import clr

# Importa a configuração centralizada para obter o caminho dos drivers.
from config import CONFIG

logger = logging.getLogger(__name__)


def carregar_drivers_externos() -> None:
    """
    Localiza e carrega a DLL do AdomdClient necessária para a conexão OLAP.

    Este processo é crítico para a aplicação, pois sem ele, a comunicação
    com cubos do Analysis Services não é possível.

    Raises:
        FileNotFoundError: Se a DLL não for encontrada no local esperado.
        Exception: Para outros erros durante o carregamento da DLL.
    """
    logger.info("Inicializando... Carregando drivers externos.")
    caminho_dll = CONFIG.paths.drivers / "Microsoft.AnalysisServices.AdomdClient.dll"

    try:
        if not caminho_dll.exists():
            # Lança uma exceção clara que será capturada no main.py
            raise FileNotFoundError(f"DLL não encontrada: {caminho_dll}")

        # Adiciona a referência à DLL para que o CLR (Common Language Runtime) a reconheça.
        clr.AddReference(str(caminho_dll))
        logger.info("Driver AdomdClient carregado com sucesso na memória.")

    except Exception as e:
        logger.critical(
            "Falha crítica ao carregar a DLL '%s'.", caminho_dll.name
        )
        logger.critical(
            "Verifique se o arquivo está na pasta 'drivers' e se foi 'Desbloqueado' "
            "nas propriedades do arquivo no Windows."
        )
        # Relança a exceção para interromper a execução do programa de forma controlada.
        raise e

