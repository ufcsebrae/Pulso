# logger_config.py
from __future__ import annotations

import logging
import sys
from typing import Final

# Nível de log configurável em um só lugar.
# Para depuração, mude para logging.DEBUG.
LOG_LEVEL: Final[int] = logging.INFO


def configurar_logger() -> logging.Logger:
    """
    Configura o logger raiz para formatar e exibir mensagens no console.

    Níveis de Log:
    - DEBUG: Informações detalhadas, para depuração.
    - INFO: Confirmação de que as coisas estão funcionando como esperado.
    - WARNING: Indicação de algo inesperado ou um problema iminente.
    - ERROR: O software não conseguiu executar alguma função.
    - CRITICAL: Erro grave que pode impedir a continuação do programa.

    Returns:
        A instância do logger raiz configurado.
    """
    # Define um formato de mensagem claro e informativo.
    formato = logging.Formatter(
        fmt="%(asctime)s - %(levelname)-8s - %(name)s - %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    # Cria um manipulador (handler) que envia as mensagens para a saída padrão.
    handler = logging.StreamHandler(sys.stdout)
    handler.setFormatter(formato)

    # Obtém o logger raiz.
    logger = logging.getLogger()
    logger.setLevel(LOG_LEVEL)

    # Limpa handlers existentes para evitar duplicação de logs em reexecuções.
    if logger.hasHandlers():
        logger.handlers.clear()

    logger.addHandler(handler)

    return logger

