import pytest
from pathlib import Path
from utils.utils import carregar_script_sql

def test_carregar_script_sql_sucesso(tmp_path: Path):
    """
    Testa se a função consegue ler um arquivo SQL com sucesso.
    'tmp_path' é uma fixture do pytest que cria uma pasta temporária.
    """
    # Cria um arquivo .sql falso na pasta temporária
    sql_content = "SELECT * FROM minha_tabela;"
    p = tmp_path / "minha_query.sql"
    p.write_text(sql_content, encoding="utf-8")

    # Chama a função com o caminho do arquivo falso
    resultado = carregar_script_sql(p)

    # Verifica se o conteúdo lido é o mesmo que foi escrito
    assert resultado == sql_content

def test_carregar_script_sql_nao_encontrado():
    """
    Testa se a função levanta o erro 'FileNotFoundError' quando
    o arquivo não existe.
    """
    caminho_falso = Path("caminho/que/nao/existe/query.sql")

    # Verifica se a exceção esperada é levantada
    with pytest.raises(FileNotFoundError):
        carregar_script_sql(caminho_falso)
