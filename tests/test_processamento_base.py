# tests/test_processamento_base.py (VERSÃO CORRIGIDA COM MOCK INTELIGENTE)
import pytest
import pandas as pd
from pathlib import Path
from processamento.processamento_dados_base import obter_dados_processados

def test_obter_dados_processados_com_mocks(mocker):
    """
    Testa o fluxo de 'obter_dados_processados' simulando as dependências externas
    com um mock inteligente para pd.read_csv.
    """
    # 1. Prepara todos os dados falsos que as funções mockadas irão retornar
    df_falso_do_db = pd.DataFrame({
        'PROJETO': ['Projeto A', 'Projeto A', 'Projeto B'],
        'UNIDADE': ['UNIDADE 1', 'UNIDADE 2', 'UNIDADE 1'],
        'Valor_Planejado': [100, 100, 50],
        'Valor_Executado': [80, 70, 40],
        'MES': [1, 1, 2]
    })

    df_falso_mapa_unidade = pd.DataFrame({
        'nm_unidade_padronizada': ['UNIDADE 1', 'UNIDADE 2'],
        'final': ['UNIDADE FINAL 1', 'UNIDADE FINAL 2']
    })
    
    # DataFrame falso para o NATUREZA.csv
    df_falso_mapa_natureza = pd.DataFrame({
        'Descricao_Natureza_Orcamentaria': ['Natureza Bruta 1', 'Natureza Bruta 2'],
        'Descricao_Natureza_Orcamentaria_FINAL': ['Natureza Final 1', 'Natureza Final 2']
    })

    # 2. Configura os mocks
    mocker.patch('processamento.processamento_dados_base.carregar_drivers_externos')
    mocker.patch('processamento.processamento_dados_base.get_conexao')
    mocker.patch('pandas.read_sql', return_value=df_falso_do_db)

    # --- MOCK INTELIGENTE PARA pd.read_csv ---
    def mock_read_csv(filepath, *args, **kwargs):
        """
        Esta função será chamada no lugar de pd.read_csv.
        Ela verifica o nome do arquivo e retorna o DataFrame falso correspondente.
        """
        if Path(filepath).name == 'UNIDADE.CSV':
            return df_falso_mapa_unidade
        elif Path(filepath).name == 'NATUREZA.csv':
            return df_falso_mapa_natureza
        # Retorna um DF vazio para qualquer outro CSV que possa ser lido
        return pd.DataFrame()

    mocker.patch('pandas.read_csv', side_effect=mock_read_csv)

    # 3. Executa a função que está sendo testada
    resultado_df = obter_dados_processados()

    # 4. Verifica os resultados
    assert resultado_df is not None
    assert not resultado_df.empty
    
    tipos_esperados = {'Projeto A': 'Compartilhado', 'Projeto B': 'Exclusivo'}
    tipos_calculados = resultado_df.set_index('PROJETO')['tipo_projeto'].to_dict()
    
    assert tipos_calculados == tipos_esperados

    # Verifica se a padronização da unidade funcionou
    assert 'UNIDADE FINAL 1' in resultado_df['UNIDADE_FINAL'].values
    assert 'UNIDADE FINAL 2' in resultado_df['UNIDADE_FINAL'].values
