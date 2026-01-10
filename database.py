# database.py
import logging
from typing import Union

from pyadomd import Pyadomd
from sqlalchemy import create_engine
from sqlalchemy.engine import Engine

# Importa a classe de configuração para tipagem e acesso seguro.
from config import DbConfig

logger = logging.getLogger(__name__)

# O tipo de união representa qualquer uma das possíveis conexões que a fábrica pode retornar.
Conexao = Union[Engine, Pyadomd]


def get_conexao(config: DbConfig) -> Conexao:
    """
    Cria e retorna um objeto de conexão de banco de dados com base na configuração fornecida.
    """
    destino_log = config.banco or config.caminho
    logger.info("Criando conexão do tipo '%s' para '%s'...", config.tipo, destino_log)

    if config.tipo == "sql":
        conn_str = (
            "mssql+pyodbc:///?odbc_connect="
            f"DRIVER={{{config.driver}}};"
            f"SERVER={config.servidor};"
            f"DATABASE={config.banco};"
            "Trusted_Connection=yes;"
        )
        # *** GARANTA QUE ESTA LINHA ESTEJA PRESENTE E ATIVA ***
        # Combina a inserção em massa com a nossa estratégia de chunking manual.
        return create_engine(conn_str, fast_executemany=True)

    elif config.tipo == "olap":
        conn_str = (
            f"Provider={config.provider};"
            f"Data Source={config.data_source};"
            f"Initial Catalog={config.catalog};"
        )
        return Pyadomd(conn_str)

    elif config.tipo == "sqlite":
        conn_str = f"sqlite:///{config.caminho}"
        return create_engine(conn_str)

    else:
        raise ValueError(
            f"Tipo de conexão desconhecido: '{config.tipo}'."
        )
