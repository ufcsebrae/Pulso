# gerar_relatorio.py
import argparse
import logging
import sys
import pandas as pd
import json
from pathlib import Path

# --- Módulo de Dados Centralizado ---
try:
    from processamento_dados_base import obter_dados_processados
except ImportError:
    logging.basicConfig(level=logging.INFO)
    logging.critical("Erro: O arquivo 'processamento_dados_base.py' não foi encontrado.")
    sys.exit(1)

from config import CONFIG

logger = logging.getLogger(__name__)

def formatar_brl(valor):
    """Formata um número para o padrão de moeda BRL (ex: R$ 1,23 M)."""
    if pd.isna(valor) or valor == 0:
        return "R$ 0"
    if abs(valor) >= 1_000_000:
        return f"R$ {(valor / 1_000_000):.2f} M"
    if abs(valor) >= 1_000:
        return f"R$ {(valor / 1_000):.1f} k"
    return f"R$ {valor:,.2f}"

def obter_unidades_disponiveis(df_base: pd.DataFrame) -> list[str]:
    """Obtém a lista de unidades únicas a partir da coluna padronizada."""
    if df_base is None or df_base.empty:
        return []
    return sorted(df_base['UNIDADE_FINAL'].unique())

def selecionar_unidades_interativamente(unidades_disponiveis: list[str]) -> list[str]:
    """Permite que o usuário selecione interativamente quais relatórios gerar."""
    if not unidades_disponiveis: return []
    print("\n--- Unidades Disponíveis para Geração de Relatório ---")
    for i, unidade in enumerate(unidades_disponiveis, 1):
        print(f"  {i:2d}) {unidade}")
    print("  all) Gerar para todas as unidades")
    print("-" * 55)
    while True:
        escolha_str = input("Escolha os números (ex: 1, 3, 5), 'all' ou enter para sair: ").strip()
        if not escolha_str:
            logger.info("Operação cancelada.")
            return []
        if escolha_str.lower() == 'all':
            return unidades_disponiveis
        try:
            indices = [int(num.strip()) - 1 for num in escolha_str.split(',')]
            selecionadas = [unidades_disponiveis[i] for i in indices if 0 <= i < len(unidades_disponiveis)]
            if len(selecionadas) < len(indices):
                logger.warning("Alguns números eram inválidos e foram ignorados.")
            return selecionadas
        except ValueError:
            print("Entrada inválida. Use números separados por vírgula ou 'all'.")

def gerar_relatorio_para_unidade(unidade_alvo: str, df_base_total: pd.DataFrame):
    """Gera um dashboard HTML específico para uma única unidade de negócio."""
    logger.info(f"Iniciando a geração do dashboard para a unidade: '{unidade_alvo}'...")

    # Filtra o DataFrame principal para obter dados apenas da unidade alvo
    df_unidade = df_base_total[df_base_total['UNIDADE_FINAL'] == unidade_alvo].copy()

    if df_unidade.empty:
        logger.warning(f"Nenhum dado encontrado para a unidade '{unidade_alvo}'. O relatório não será gerado.")
        return

    # --- 1. Cálculos de KPIs ---
    df_exclusivos = df_unidade[df_unidade['tipo_projeto'] == 'Exclusivo']
    df_compartilhados = df_unidade[df_unidade['tipo_projeto'] == 'Compartilhado']

    total_planejado = df_unidade['Valor_Planejado'].sum()
    total_executado = df_unidade['Valor_Executado'].sum()
    total_perc = (total_executado / total_planejado * 100) if total_planejado else 0

    exclusivo_planejado = df_exclusivos['Valor_Planejado'].sum()
    exclusivo_executado = df_exclusivos['Valor_Executado'].sum()
    exclusivo_perc = (exclusivo_executado / exclusivo_planejado * 100) if exclusivo_planejado else 0

    compartilhado_planejado = df_compartilhados['Valor_Planejado'].sum()
    compartilhado_executado = df_compartilhados['Valor_Executado'].sum()
    compartilhado_perc = (compartilhado_executado / compartilhado_planejado * 100) if compartilhado_planejado else 0
    
    kpi_dict = {
        "kpi_total_perc": f"{total_perc:.1f}%",
        "kpi_total_valores": f"{formatar_brl(total_executado)} de {formatar_brl(total_planejado)}",
        "kpi_exclusivo_perc": f"{exclusivo_perc:.1f}%",
        "kpi_exclusivo_valores": f"{formatar_brl(exclusivo_executado)} de {formatar_brl(exclusivo_planejado)}",
        "kpi_compartilhado_perc": f"{compartilhado_perc:.1f}%",
        "kpi_compartilhado_valores": f"{formatar_brl(compartilhado_executado)} de {formatar_brl(compartilhado_planejado)}",
    }
    logger.info(f"[{unidade_alvo}] KPIs calculados.")

    # --- 2. Preparação de Dados para Gráficos ---
    dados_graficos = {}
    meses = ['Jan', 'Fev', 'Mar', 'Abr', 'Mai', 'Jun', 'Jul', 'Ago', 'Set', 'Out', 'Nov', 'Dez']
    
    # Gráfico de Evolução Mensal
    df_trend = df_unidade.groupby(['MES', 'tipo_projeto'])['Valor_Executado'].sum().unstack(fill_value=0).reindex(range(1, 13), fill_value=0)
    df_trend['Total'] = df_trend.sum(axis=1)
    dados_graficos['trend'] = {
        "labels": meses,
        "executed_total": df_trend['Total'].tolist(),
        "executed_exclusivo": df_trend.get('Exclusivo', pd.Series([0]*12)).tolist(),
        "executed_compartilhado": df_trend.get('Compartilhado', pd.Series([0]*12)).tolist()
    }

    # Gráfico de Natureza (Top 10)
    df_nature = df_unidade.groupby('NATUREZA_FINAL')['Valor_Planejado'].sum().nlargest(10).reset_index()
    dados_graficos['nature'] = {"labels": df_nature['NATUREZA_FINAL'].tolist(), "values": df_nature['Valor_Planejado'].tolist()}

    # Gráfico de Dispersão
    df_scatter = df_unidade.groupby(['PROJETO', 'tipo_projeto'])[['Valor_Planejado', 'Valor_Executado']].sum().reset_index()
    df_scatter = df_scatter[df_scatter['Valor_Planejado'] > 0]
    df_scatter['execution_rate'] = (df_scatter['Valor_Executado'] / df_scatter['Valor_Planejado']) * 100
    dados_graficos['scatter'] = [
        {"x": row['Valor_Planejado'], "y": row['execution_rate'], "label": row['PROJETO'], "type": row['tipo_projeto']}
        for _, row in df_scatter.iterrows()
    ]
    logger.info(f"[{unidade_alvo}] Dados para os gráficos agregados.")

    # --- 3. Geração do HTML Final ---
    try:
        template_path = CONFIG.paths.base_dir / "dashboard_template.html"
        template_string = template_path.read_text(encoding='utf-8')

        final_html = template_string.format(**kpi_dict, json_data=json.dumps(dados_graficos))
        
        output_filename = f"dashboard_{unidade_alvo.replace(' ', '_').replace('/', '_')}.html"
        output_path = CONFIG.paths.docs_dir / output_filename
        
        with open(output_path, 'w', encoding='utf-8') as f:
            f.write(final_html)
        
        logger.info(f"Dashboard para '{unidade_alvo}' salvo com sucesso em: '{output_path}'")

    except FileNotFoundError:
        logger.error(f"Arquivo de template 'dashboard_template.html' não encontrado.")
    except Exception as e:
        logger.exception(f"Ocorreu um erro ao gerar o HTML para '{unidade_alvo}': {e}")


def main() -> None:
    """Função principal que orquestra a geração dos relatórios."""
    parser = argparse.ArgumentParser(description="Gera dashboards de performance orçamentária por unidade.")
    parser.add_argument("--unidade", type=str, help="Gera o dashboard para uma unidade específica (use o nome padronizado).")
    parser.add_argument("--todas", action="store_true", help="Gera relatórios para todas as unidades disponíveis.")
    args = parser.parse_args()

    CONFIG.paths.docs_dir.mkdir(parents=True, exist_ok=True)
    
    logger.info("Carregando e processando a base de dados centralizada...")
    df_base_total = obter_dados_processados()

    if df_base_total is None or df_base_total.empty:
        logger.error("A base de dados não pôde ser carregada. Encerrando.")
        sys.exit(1)

    unidades_disponiveis = obter_unidades_disponiveis(df_base_total)
    unidades_a_gerar = []

    if args.unidade:
        unidade_padronizada_arg = args.unidade.upper().strip()
        if unidade_padronizada_arg in unidades_disponiveis:
            unidades_a_gerar = [unidade_padronizada_arg]
        else:
            logger.error(f"A unidade '{args.unidade}' não foi encontrada. Unidades disponíveis: {unidades_disponiveis}")
    elif args.todas:
        unidades_a_gerar = unidades_disponiveis
    else:
        if unidades_disponiveis:
            unidades_a_gerar = selecionar_unidades_interativamente(unidades_disponiveis)

    if not unidades_a_gerar:
        logger.info("Nenhuma unidade selecionada. Encerrando.")
    else:
        logger.info(f"Gerando dashboards para: {', '.join(unidades_a_gerar)}")
        for unidade in unidades_a_gerar:
            gerar_relatorio_para_unidade(unidade, df_base_total)

    logger.info("\n--- FIM DO SCRIPT DE GERAÇÃO DE DASHBOARD ---")

if __name__ == "__main__":
    main()
