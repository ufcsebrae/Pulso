# config.py
import os
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, Final

from dotenv import load_dotenv

load_dotenv()


@dataclass(frozen=True)
class PathsConfig:
    """Centraliza todos os caminhos de arquivos e diretórios usados no projeto."""
    base_dir: Path = Path(__file__).resolve().parent.parent 
    
    # Diretórios de Saída
    docs_dir: Path = base_dir / "docs"
    relatorios_excel_dir: Path = docs_dir / "excel"
    logs_dir: Path = base_dir / "logs"

    # Diretório de Templates e Estilos
    templates_dir: Path = base_dir / "templates"
    static_dir: Path = docs_dir / "static"
    
    # Diretório de Dados
    dados_dir: Path = base_dir / "dados"

    # Arquivos de Entrada e Dados (usando a pasta 'dados_dir')
    query_nacional: Path = base_dir / "queries" / "nacional.sql"
    query_cc: Path = base_dir / "queries" / "cc.sql"
    cache_db: Path = base_dir / "cache_dados.db"
    
    # Aponta para o arquivo DENTRO da pasta 'dados'
    mapa_correcoes: Path = dados_dir / "mapa_correcoes.json"
    gerentes_csv: Path = dados_dir / "gerentes.csv"
    mapa_naturezas_csv: Path = dados_dir / "mapa_naturezas.csv" # Mantenha por consistência ou remova se já excluiu
    unidade_csv: Path = dados_dir / "UNIDADE.CSV" # Adicionando para UNIDADE.CSV
    natureza_csv: Path = dados_dir / "NATUREZA.csv" # Adicionando para NATUREZA.csv
    
    drivers: Path = base_dir / "drivers"


@dataclass(frozen=True)
class DbConfig:
    """Define a estrutura para uma configuração de banco de dados."""
    tipo: str
    driver: str | None = None
    servidor: str | None = None
    banco: str | None = None
    caminho: Path | None = None
    provider: str | None = None
    data_source: str | None = None
    catalog: str | None = None
    trusted_connection: bool = True


@dataclass(frozen=True)
class AppConfig:
    """Agrega todas as configurações da aplicação."""
    paths: PathsConfig
    conexoes: Dict[str, DbConfig]


def get_config() -> AppConfig:
    """Constrói e retorna o objeto de configuração principal da aplicação."""
    paths = PathsConfig()
    driver_sql = os.getenv("DB_DRIVER", "ODBC Driver 18 for SQL Server")

    conexoes = {
        "HubDados": DbConfig(
            tipo="sql",
            servidor=os.getenv("DB_SERVER_HUB"),
            banco=os.getenv("DB_DATABASE_HUB"),
            driver=driver_sql,
        ),
        "FINANCA_SQL": DbConfig(
            tipo="sql",
            servidor=os.getenv("DB_SERVER_FINANCA"),
            banco=os.getenv("DB_DATABASE_FINANCA"),
            driver=driver_sql,
        ),
        "CacheDB": DbConfig(tipo="sqlite", caminho=paths.cache_db),
        "OLAP": DbConfig(
            tipo="olap",
            provider=os.getenv("OLAP_PROVIDER"),
            data_source=os.getenv("OLAP_SOURCE"),
            catalog=os.getenv("OLAP_CATALOG"),
        ),
    }

    if not conexoes["HubDados"].servidor or not conexoes["FINANCA_SQL"].servidor:
        raise ValueError("Erro: Variáveis de ambiente para conexões SQL não definidas.")

    if conexoes["OLAP"].tipo == "olap" and not all([conexoes["OLAP"].provider, conexoes["OLAP"].data_source, conexoes["OLAP"].catalog]):
        raise ValueError("Erro: Variáveis de ambiente para conexão OLAP não definidas.")

    return AppConfig(paths=paths, conexoes=conexoes)


CONFIG: Final[AppConfig] = get_config()
