# gerar_relatorio.py
import argparse
import logging
import sys
import pandas as pd
import json

try:
    # CORREÇÃO: Importando ambas as funções do local correto
    from processamento_dados_base import obter_dados_processados, formatar_brl
except ImportError:
    logging.basicConfig(level=logging.INFO)
    logging.critical("Erro: O arquivo 'processamento_dados_base.py' ou suas funções não foram encontrados.")
    sys.exit(1)

from config import CONFIG

logger = logging.getLogger(__name__)

# A função formatar_brl foi removida daqui

def obter_unidades_disponiveis(df_base: pd.DataFrame) -> list[str]:
    # ... (o resto do arquivo permanece o mesmo)
    if df_base is None or df_base.empty: return []
    return sorted(df_base['UNIDADE_FINAL'].unique())

def selecionar_unidades_interativamente(unidades_disponiveis: list[str]) -> list[str]:
    if not unidades_disponiveis: return []
    print("\n--- Unidades Disponíveis para Geração de Relatório ---")
    for i, unidade in enumerate(unidades_disponiveis, 1):
        print(f"  {i:2d}) {unidade}")
    print("  all) Gerar para todas as unidades")
    print("-" * 55)
    while True:
        escolha_str = input("Escolha os números (ex: 1, 3, 5), 'all' ou enter para sair: ").strip()
        if not escolha_str: return []
        if escolha_str.lower() == 'all': return unidades_disponiveis
        try:
            indices = [int(num.strip()) - 1 for num in escolha_str.split(',')]
            return [unidades_disponiveis[i] for i in indices if 0 <= i < len(unidades_disponiveis)]
        except ValueError:
            print("Entrada inválida.")

def gerar_relatorio_para_unidade(unidade_alvo: str, df_base_total: pd.DataFrame):
    logger.info(f"Iniciando a geração do dashboard para a unidade: '{unidade_alvo}'...")
    df_unidade = df_base_total[df_base_total['UNIDADE_FINAL'] == unidade_alvo].copy()
    if df_unidade.empty:
        logger.warning(f"Nenhum dado para a unidade '{unidade_alvo}'.")
        return

    df_exclusivos = df_unidade[df_unidade['tipo_projeto'] == 'Exclusivo'].copy()
    df_compartilhados = df_unidade[df_unidade['tipo_projeto'] == 'Compartilhado'].copy()

    kpi_dict = {
        "__UNIDADE_ALVO__": unidade_alvo,
        "__KPI_TOTAL_PERC__": f"{(df_unidade['Valor_Executado'].sum() / df_unidade['Valor_Planejado'].sum() * 100) if df_unidade['Valor_Planejado'].sum() else 0:.1f}%",
        "__KPI_TOTAL_VALORES__": f"{formatar_brl(df_unidade['Valor_Executado'].sum())} de {formatar_brl(df_unidade['Valor_Planejado'].sum())}",
        "__KPI_EXCLUSIVO_PERC__": f"{(df_exclusivos['Valor_Executado'].sum() / df_exclusivos['Valor_Planejado'].sum() * 100) if df_exclusivos['Valor_Planejado'].sum() else 0:.1f}%",
        "__KPI_EXCLUSIVO_VALORES__": f"{formatar_brl(df_exclusivos['Valor_Executado'].sum())} de {formatar_brl(df_exclusivos['Valor_Planejado'].sum())}",
        "__KPI_COMPARTILHADO_PERC__": f"{(df_compartilhados['Valor_Executado'].sum() / df_compartilhados['Valor_Planejado'].sum() * 100) if df_compartilhados['Valor_Planejado'].sum() else 0:.1f}%",
        "__KPI_COMPARTILHADO_VALORES__": f"{formatar_brl(df_compartilhados['Valor_Executado'].sum())} de {formatar_brl(df_compartilhados['Valor_Planejado'].sum())}",
    }
    
    dados_graficos = {}
    
    df_trend = df_unidade.groupby(['MES', 'tipo_projeto'])['Valor_Executado'].sum().unstack(fill_value=0).reindex(range(1, 13), fill_value=0)
    df_trend['Total'] = df_trend.sum(axis=1)
    dados_graficos['trend'] = {"labels": ['Jan', 'Fev', 'Mar', 'Abr', 'Mai', 'Jun', 'Jul', 'Ago', 'Set', 'Out', 'Nov', 'Dez'], "executed_total": df_trend['Total'].tolist(), "executed_exclusivo": df_trend.get('Exclusivo', pd.Series([0]*12)).tolist(), "executed_compartilhado": df_trend.get('Compartilhado', pd.Series([0]*12)).tolist()}

    def criar_dados_treemap_com_projetos(df_source, root_label):
        if df_source is None or df_source.empty: return {}
        df_agg = df_source.groupby(['NATUREZA_FINAL', 'PROJETO'])['Valor_Executado'].sum().reset_index()
        df_agg = df_agg[df_agg['Valor_Executado'] > 0]
        if df_agg.empty: return {}
        
        df_agg['total_natureza'] = df_agg.groupby('NATUREZA_FINAL')['Valor_Executado'].transform('sum')
        df_agg['perc'] = (df_agg['Valor_Executado'] / df_agg['total_natureza']) * 100
        
        def format_projetos(group):
            top_projetos = group.nlargest(3, 'Valor_Executado')
            return '<br>'.join([f"- {row.PROJETO} ({row.perc:.1f}%)" for _, row in top_projetos.iterrows()])

        projetos_por_natureza = df_agg.groupby('NATUREZA_FINAL').apply(format_projetos).to_dict()
        df_natureza_sum = df_agg.groupby('NATUREZA_FINAL')['Valor_Executado'].sum().reset_index()
        
        return {'labels': df_natureza_sum['NATUREZA_FINAL'].tolist(), 'parents': [""] * len(df_natureza_sum), 'values': df_natureza_sum['Valor_Executado'].tolist(), 'projetos': df_natureza_sum['NATUREZA_FINAL'].map(projetos_por_natureza).fillna('').tolist()}

    dados_graficos['treemap_exclusivo'] = criar_dados_treemap_com_projetos(df_exclusivos)
    dados_graficos['treemap_compartilhado'] = criar_dados_treemap_com_projetos(df_compartilhados)

    # Restante das funções de dados...
    
    logger.info(f"[{unidade_alvo}] Dados para os gráficos agregados.")

    try:
        template_path = CONFIG.paths.base_dir / "dashboard_template.html"
        template_string = template_path.read_text(encoding='utf-8')
        
        final_html = template_string
        for key, value in kpi_dict.items():
            final_html = final_html.replace(key, str(value))
        
        json_string = json.dumps(dados_graficos)
        final_html = final_html.replace("__JSON_DATA_PLACEHOLDER__", json_string)
        
        output_filename = f"dashboard_{unidade_alvo.replace(' ', '_').replace('/', '_')}.html"
        output_path = CONFIG.paths.docs_dir / output_filename
        
        with open(output_path, 'w', encoding='utf-8') as f: f.write(final_html)
        logger.info(f"Dashboard para '{unidade_alvo}' salvo com sucesso em: '{output_path}'")
    except Exception as e:
        logger.exception(f"Ocorreu um erro ao gerar o HTML para '{unidade_alvo}': {e}")

def main():
    parser = argparse.ArgumentParser(description="Gera dashboards de performance orçamentária por unidade.")
    parser.add_argument("--unidade", type=str, help="Gera o dashboard para uma unidade específica.")
    parser.add_argument("--todas", action="store_true", help="Gera relatórios para todas as unidades disponíveis.")
    args = parser.parse_args()

    CONFIG.paths.docs_dir.mkdir(parents=True, exist_ok=True)
    df_base_total = obter_dados_processados()
    if df_base_total is None or df_base_total.empty:
        logger.error("A base de dados não pôde ser carregada. Encerrando.")
        sys.exit(1)

    unidades_disponiveis = obter_unidades_disponiveis(df_base_total)
    unidades_a_gerar = []
    if args.unidade:
        if args.unidade.upper() in unidades_disponiveis:
            unidades_a_gerar = [args.unidade.upper()]
        else:
            logger.error(f"Unidade '{args.unidade}' não encontrada. Disponíveis: {unidades_disponiveis}")
    elif args.todas:
        unidades_a_gerar = unidades_disponiveis
    else:
        unidades_a_gerar = selecionar_unidades_interativamente(unidades_disponiveis) if unidades_disponiveis else []

    if unidades_a_gerar:
        logger.info(f"Gerando dashboards para: {', '.join(unidades_a_gerar)}")
        for unidade in unidades_a_gerar:
            gerar_relatorio_para_unidade(unidade, df_base_total)
    else:
        logger.info("Nenhuma unidade selecionada. Encerrando.")
    
    logger.info("\n--- FIM DO SCRIPT DE GERAÇÃO DE DASHBOARD ---")

if __name__ == "__main__":
    main()
