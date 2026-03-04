import pytest
import pandas as pd

@pytest.fixture
def sample_df_unidade() -> pd.DataFrame:
    """
    Fixture que fornece um DataFrame de exemplo, similar ao que é usado
    para gerar os gráficos.
    """
    data = {
        'PROJETO': [
            'Projeto Saldo Alto', 'Projeto Saldo Alto', 'Projeto Saldo Alto',
            'Projeto Saldo Baixo', 'Projeto Saldo Baixo',
            'Projeto Exclusivo A', 'Projeto Exclusivo A',
            'Projeto Executado a Mais'
        ],
        'tipo_projeto': [
            'Compartilhado', 'Compartilhado', 'Compartilhado',
            'Compartilhado', 'Compartilhado',
            'Exclusivo', 'Exclusivo',
            'Exclusivo'
        ],
        'ACAO': [
            'Ação 1', 'Ação 2', 'Ação 3',
            'Ação 4', 'Ação 5',
            'Ação Exclusiva 1', 'Ação Exclusiva 2',
            'Ação Extra'
        ],
        'Valor_Planejado': [
            100000, 50000, 25000,  # Proj Saldo Alto: Planejado = 175k
            80000, 20000,           # Proj Saldo Baixo: Planejado = 100k
            60000, 40000,           # Proj Exclusivo A: Planejado = 100k
            0                       # Proj Executado a Mais: Planejado = 0
        ],
        'Valor_Executado': [
            50000, 20000, 5000,   # Proj Saldo Alto: Executado = 75k -> Saldo = 100k
            75000, 15000,          # Proj Saldo Baixo: Executado = 90k -> Saldo = 10k
            30000, 20000,          # Proj Exclusivo A: Executado = 50k -> Saldo = 50k
            10000                  # Proj Executado a Mais: Executado = 10k -> Saldo = -10k (não deve aparecer)
        ]
    }
    return pd.DataFrame(data)
