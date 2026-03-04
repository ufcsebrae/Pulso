import pytest
from visualizacao.preparadores_dados import preparar_dados_orcamento_ocioso

def test_preparar_dados_orcamento_ocioso_calculo_correto(sample_df_unidade):
    """
    Testa se a função 'preparar_dados_orcamento_ocioso' calcula corretamente
    os saldos e ordena os projetos.
    """
    # Chama a função com os dados de teste da nossa fixture
    resultado = preparar_dados_orcamento_ocioso(sample_df_unidade)

    # 1. Verifica se os projetos corretos foram selecionados e ordenados
    assert resultado['labels'] == ['Projeto Saldo Alto', 'Projeto Exclusivo A', 'Projeto Saldo Baixo']

    # 2. Verifica os valores dos saldos
    # O projeto 'Projeto Saldo Alto' é compartilhado
    assert resultado['values_compartilhado'][0] == pytest.approx(100000) # 175k - 75k
    assert resultado['values_exclusivo'][0] == 0

    # O projeto 'Projeto Exclusivo A' é exclusivo
    assert resultado['values_exclusivo'][1] == pytest.approx(50000) # 100k - 50k
    assert resultado['values_compartilhado'][1] == 0

    # O projeto 'Projeto Saldo Baixo' é compartilhado
    assert resultado['values_compartilhado'][2] == pytest.approx(10000) # 100k - 90k
    assert resultado['values_exclusivo'][2] == 0
    
    # 3. Verifica se o projeto com saldo negativo foi ignorado
    assert 'Projeto Executado a Mais' not in resultado['labels']
