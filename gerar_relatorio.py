# gerar_relatorio.py
import argparse
import logging
import sys
import pandas as pd
import json

try:
    from processamento.processamento_dados_base import obter_dados_processados, formatar_brl
except ImportError:
    logging.basicConfig(level=logging.INFO)
    logging.critical("Erro: O arquivo 'processamento_dados_base.py' ou suas funções não foram encontrados.")
    sys.exit(1)

from config.config import CONFIG

logger = logging.getLogger(__name__)

def obter_unidades_disponiveis(df_base: pd.DataFrame) -> list[str]:
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

    # Garante que o denominador não seja zero para evitar erros de divisão
    kpi_total_planejado = df_unidade['Valor_Planejado'].sum()
    kpi_exclusivo_planejado = df_exclusivos.get('Valor_Planejado', pd.Series([0])).sum()
    kpi_compartilhado_planejado = df_compartilhados.get('Valor_Planejado', pd.Series([0])).sum()

    kpi_dict = {
        "__UNIDADE_ALVO__": unidade_alvo,
        "__KPI_TOTAL_PERC__": f"{(df_unidade['Valor_Executado'].sum() / kpi_total_planejado * 100) if kpi_total_planejado > 0 else 0:.1f}%",
        "__KPI_TOTAL_VALORES__": f"{formatar_brl(df_unidade['Valor_Executado'].sum())} de {formatar_brl(kpi_total_planejado)}",
        "__KPI_EXCLUSIVO_PERC__": f"{(df_exclusivos['Valor_Executado'].sum() / kpi_exclusivo_planejado * 100) if kpi_exclusivo_planejado > 0 else 0:.1f}%",
        "__KPI_EXCLUSIVO_VALORES__": f"{formatar_brl(df_exclusivos['Valor_Executado'].sum())} de {formatar_brl(kpi_exclusivo_planejado)}",
        "__KPI_COMPARTILHADO_PERC__": f"{(df_compartilhados['Valor_Executado'].sum() / kpi_compartilhado_planejado * 100) if kpi_compartilhado_planejado > 0 else 0:.1f}%",
        "__KPI_COMPARTILHADO_VALORES__": f"{formatar_brl(df_compartilhados['Valor_Executado'].sum())} de {formatar_brl(kpi_compartilhado_planejado)}",
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
        
        def format_projetos(group):
            top_projetos = group.nlargest(3, 'Valor_Executado')
            return '<br>'.join([f"- {row.PROJETO} ({formatar_brl(row.Valor_Executado)})" for _, row in top_projetos.iterrows()])
        
        projetos_por_natureza = df_agg.groupby('NATUREZA_FINAL').apply(format_projetos, include_groups=False).to_dict()
        df_natureza_sum = df_agg.groupby('NATUREZA_FINAL')['Valor_Executado'].sum().reset_index()
        
        return {'labels': df_natureza_sum['NATUREZA_FINAL'].tolist(), 'parents': [""] * len(df_natureza_sum), 'values': df_natureza_sum['Valor_Executado'].tolist(), 'projetos': df_natureza_sum['NATUREZA_FINAL'].map(projetos_por_natureza).fillna('').tolist()}

    dados_graficos['treemap_exclusivo'] = criar_dados_treemap_com_projetos(df_exclusivos, unidade_alvo)
    dados_graficos['treemap_compartilhado'] = criar_dados_treemap_com_projetos(df_compartilhados, unidade_alvo)

    # --- INÍCIO DA CORREÇÃO PARA O GRÁFICO DE ORÇAMENTO OCIOSO ---
    df_unidade['saldo_nao_executado'] = df_unidade['Valor_Planejado'].fillna(0) - df_unidade['Valor_Executado'].fillna(0)
    saldos_por_projeto_unidade = df_unidade.groupby(['PROJETO', 'tipo_projeto'])['saldo_nao_executado'].sum().reset_index()
    df_top_ocioso_agg = saldos_por_projeto_unidade[saldos_por_projeto_unidade['saldo_nao_executado'] > 0].nlargest(7, 'saldo_nao_executado')
    df_pivot_ocioso = df_top_ocioso_agg.pivot_table(index='PROJETO', columns='tipo_projeto', values='saldo_nao_executado', fill_value=0)
    df_pivot_ocioso = df_pivot_ocioso.reindex(df_top_ocioso_agg['PROJETO'])
    
    # CORREÇÃO 1: Detalhes do tooltip agora agrupam as ações para evitar repetição
    def formatar_acoes(group):
        acoes_agrupadas = group.groupby('ACAO')['saldo_nao_executado'].sum()
        top_acoes = acoes_agrupadas[acoes_agrupadas > 0].nlargest(3)
        return [f"- {acao}: {formatar_brl(saldo)}" for acao, saldo in top_acoes.items()]

    df_detalhes_ocioso = df_unidade[df_unidade['PROJETO'].isin(df_top_ocioso_agg['PROJETO'])]
    detalhes_por_projeto = df_detalhes_ocioso.groupby('PROJETO').apply(formatar_acoes, include_groups=False).reindex(df_pivot_ocioso.index)

    detalhes_exclusivo = []
    detalhes_compartilhado = []
    for projeto, row in df_pivot_ocioso.iterrows():
        detalhe_formatado = detalhes_por_projeto.get(projeto, [])
        tipo_projeto_real = df_top_ocioso_agg.loc[df_top_ocioso_agg['PROJETO'] == projeto, 'tipo_projeto'].iloc[0]
        if tipo_projeto_real == 'Exclusivo':
            detalhes_exclusivo.append(detalhe_formatado)
            detalhes_compartilhado.append([])
        elif tipo_projeto_real == 'Compartilhado':
            detalhes_compartilhado.append(detalhe_formatado)
            detalhes_exclusivo.append([])
        else:
            detalhes_exclusivo.append([])
            detalhes_compartilhado.append([])
            
    dados_graficos['idle_budget'] = {
        "labels": df_pivot_ocioso.index.tolist(),
        "values_exclusivo": df_pivot_ocioso.get('Exclusivo', pd.Series(0, index=df_pivot_ocioso.index)).fillna(0).tolist(),
        "values_compartilhado": df_pivot_ocioso.get('Compartilhado', pd.Series(0, index=df_pivot_ocioso.index)).fillna(0).tolist(),
        "detalhes_exclusivo": detalhes_exclusivo,
        "detalhes_compartilhado": detalhes_compartilhado,
    }
    
    # --- INÍCIO DA CORREÇÃO PARA O GRÁFICO DE EXECUÇÃO SEM PLANEJAMENTO ---
    # CORREÇÃO 2: A lógica é refeita para agregar primeiro e depois filtrar.
    def criar_dados_exec_sem_plan(df_source):
        if df_source is None or df_source.empty: return {}
        
        df_agg_total = df_source.groupby(['NATUREZA_FINAL', 'PROJETO']).agg(
            Valor_Planejado_Total=('Valor_Planejado', 'sum'),
            Valor_Executado_Total=('Valor_Executado', 'sum')
        ).reset_index()

        df_sem_plan_agg = df_agg_total[
            (df_agg_total['Valor_Planejado_Total'] <= 0) & 
            (df_agg_total['Valor_Executado_Total'] > 0)
        ].copy()
        
        if df_sem_plan_agg.empty: return {}

        def formatar_projetos_sp(group):
            top_projetos = group.nlargest(3, 'Valor_Executado_Total')
            return [f"- {row.PROJETO}: {formatar_brl(row.Valor_Executado_Total)}" for _, row in top_projetos.iterrows()]
        
        df_sum = df_sem_plan_agg.groupby('NATUREZA_FINAL')['Valor_Executado_Total'].sum().sort_values(ascending=False)
        if df_sum.empty: return {}
        
        detalhes_projetos_series = df_sem_plan_agg.groupby('NATUREZA_FINAL').apply(formatar_projetos_sp, include_groups=False).reindex(df_sum.index)
        detalhes_projetos = [item if isinstance(item, list) else [] for item in detalhes_projetos_series]

        return {
            "labels": df_sum.index.tolist(),
            "values": df_sum.values.tolist(),
            "projetos": detalhes_projetos
        }

    dados_graficos['unplanned_exclusivo'] = criar_dados_exec_sem_plan(df_exclusivos)
    dados_graficos['unplanned_compartilhado'] = criar_dados_exec_sem_plan(df_compartilhados)
    
    logger.info(f"[{unidade_alvo}] Dados para os gráficos agregados.")

    try:
        template_path = CONFIG.paths.templates_dir / "dashboard_template.html"
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
        unidade_formatada = args.unidade.upper()
        if unidade_formatada in unidades_disponiveis:
            unidades_a_gerar = [unidade_formatada]
        else:
            logger.error(f"Unidade '{args.unidade}' não encontrada. Disponíveis: {', '.join(unidades_disponiveis)}")
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
