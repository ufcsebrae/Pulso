# verificar_dados.py
import logging
import sys
import pandas as pd

try:
    from processamento_dados_base import obter_dados_processados, formatar_brl
except ImportError:
    logging.basicConfig(level=logging.INFO)
    logging.critical("Erro: O arquivo 'processamento_dados_base.py' ou suas funções não foram encontrados.")
    sys.exit(1)

def verificar_unidade(unidade_alvo: str, df_base_total: pd.DataFrame):
    """
    Executa e imprime os cálculos para uma unidade específica, servindo como auditoria.
    """
    print("-" * 80)
    print(f"VERIFICANDO DADOS PARA A UNIDADE: {unidade_alvo}")
    print("-" * 80)

    # Filtro agora é insensível a espaços extras
    df_unidade = df_base_total[df_base_total['UNIDADE_FINAL'].str.strip().str.upper() == unidade_alvo.strip().upper()].copy()
    if df_unidade.empty:
        print("Nenhum dado encontrado para esta unidade.")
        return

    df_exclusivos = df_unidade[df_unidade['tipo_projeto'] == 'Exclusivo'].copy()
    
    print("\n[VERIFICAÇÃO DE KPIs]")
    total_planejado = df_unidade['Valor_Planejado'].sum()
    total_executado = df_unidade['Valor_Executado'].sum()
    print(f"Execução Total: {formatar_brl(total_executado)} de {formatar_brl(total_planejado)}")
    
    exclusivo_planejado = df_exclusivos['Valor_Planejado'].sum()
    exclusivo_executado = df_exclusivos['Valor_Executado'].sum()
    print(f"Execução Exclusivos: {formatar_brl(exclusivo_executado)} de {formatar_brl(exclusivo_planejado)}")
    
    # --- Verificação do Treemap (agora focado no executado) ---
    print("\n[VERIFICAÇÃO TREEMAP - GASTOS EXECUTADOS (EXCLUSIVOS)]")
    if not df_exclusivos.empty:
        df_agg = df_exclusivos.groupby(['NATUREZA_FINAL', 'PROJETO'])['Valor_Executado'].sum().reset_index()
        df_agg = df_agg[df_agg['Valor_Executado'] > 0]
        
        if not df_agg.empty:
            df_natureza_sum = df_agg.groupby('NATUREZA_FINAL')['Valor_Executado'].sum().nlargest(5)
            print("Top 5 Naturezas por Valor Executado em Projetos Exclusivos:")
            for natureza, valor in df_natureza_sum.items():
                print(f"- {natureza}: {formatar_brl(valor)}")
                df_projetos = df_agg[df_agg['NATUREZA_FINAL'] == natureza].nlargest(3, 'Valor_Executado')
                for _, row in df_projetos.iterrows():
                    print(f"  > {row['PROJETO']}: {formatar_brl(row['Valor_Executado'])}")
        else:
            print("Nenhum gasto executado para projetos exclusivos.")
    else:
        print("Nenhum projeto exclusivo nesta unidade.")
    
    print("\n" + "-" * 80)


if __name__ == "__main__":
    df_base = obter_dados_processados()
    if df_base is None:
        sys.exit(1)
        
    unidades_disponiveis = sorted(df_base['UNIDADE_FINAL'].str.upper().unique())
    
    while True:
        print("\n--- Unidades Disponíveis para Verificação ---")
        for i, unidade in enumerate(unidades_disponiveis, 1):
            print(f"  {i:2d}) {unidade}")
        print("-" * 45)
        
        escolha = input("Digite o NOME ou NÚMERO da Unidade para verificar (ou 'sair'): ").strip()
        
        if escolha.lower() == 'sair':
            break
        
        unidade_para_checar = None
        # Tenta converter para número para selecionar pela lista
        try:
            idx = int(escolha) - 1
            if 0 <= idx < len(unidades_disponiveis):
                unidade_para_checar = unidades_disponiveis[idx]
        except ValueError:
            # Se não for um número, trata como texto
            unidade_para_checar = escolha.upper()

        if unidade_para_checar and unidade_para_checar in unidades_disponiveis:
            verificar_unidade(unidade_para_checar, df_base)
        else:
            print(f"Erro: Opção '{escolha}' inválida. Tente novamente.")
