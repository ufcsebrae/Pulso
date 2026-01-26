# visualizacao/preparadores_dados.py (VERSÃO FINAL E CORRIGIDA)
import pandas as pd
from processamento.processamento_dados_base import formatar_brl
from config.config import CORES

def preparar_dados_kpi(df_unidade: pd.DataFrame, df_exclusivos: pd.DataFrame, df_compartilhados: pd.DataFrame, unidade_nova: str) -> dict:
    def safe_div(numerator, denominator): return (numerator / denominator * 100) if denominator > 0 else 0
    kpi_total_executado = df_unidade['Valor_Executado'].sum()
    kpi_total_planejado = df_unidade['Valor_Planejado'].sum()
    kpi_exclusivo_executado = df_exclusivos['Valor_Executado'].sum()
    kpi_exclusivo_planejado = df_exclusivos['Valor_Planejado'].sum()
    kpi_compartilhado_executado = df_compartilhados['Valor_Executado'].sum()
    kpi_compartilhado_planejado = df_compartilhados['Valor_Planejado'].sum()
    return {
        "__UNIDADE_ALVO__": unidade_nova,
        "__KPI_TOTAL_PERC__": f"{safe_div(kpi_total_executado, kpi_total_planejado):.1f}%",
        "__KPI_TOTAL_VALORES__": f"{formatar_brl(kpi_total_executado)} de {formatar_brl(kpi_total_planejado)}",
        "__KPI_EXCLUSIVO_PERC__": f"{safe_div(kpi_exclusivo_executado, kpi_exclusivo_planejado):.1f}%",
        "__KPI_EXCLUSIVO_VALORES__": f"{formatar_brl(kpi_exclusivo_executado)} de {formatar_brl(kpi_exclusivo_planejado)}",
        "__KPI_COMPARTILHADO_PERC__": f"{safe_div(kpi_compartilhado_executado, kpi_compartilhado_planejado):.1f}%",
        "__KPI_COMPARTILHADO_VALORES__": f"{formatar_brl(kpi_compartilhado_executado)} de {formatar_brl(kpi_compartilhado_planejado)}",
    }

def preparar_dados_grafico_tendencia(df_unidade: pd.DataFrame) -> dict:
    df_trend = df_unidade.groupby(['MES', 'tipo_projeto'])['Valor_Executado'].sum().unstack(fill_value=0).reindex(range(1, 13), fill_value=0)
    df_trend['Total'] = df_trend.sum(axis=1)
    return {
        "labels": ['Jan', 'Fev', 'Mar', 'Abr', 'Mai', 'Jun', 'Jul', 'Ago', 'Set', 'Out', 'Nov', 'Dez'],
        "datasets": [
            {'label': 'Executado (Total)', 'data': df_trend['Total'].tolist(), 'borderColor': CORES['trend_total'], 'fill': False, 'tension': 0.3},
            {'label': 'Exclusivos', 'data': df_trend.get('Exclusivo', pd.Series([0]*12)).tolist(), 'borderColor': CORES['project_exclusive'], 'fill': False, 'tension': 0.3, 'borderDash': [5, 5]},
            {'label': 'Compartilhados', 'data': df_trend.get('Compartilhado', pd.Series([0]*12)).tolist(), 'borderColor': CORES['project_shared'], 'fill': False, 'tension': 0.3, 'borderDash': [5, 5]}
        ]
    }

def preparar_dados_treemap(df_source: pd.DataFrame) -> dict:
    if df_source is None or df_source.empty: return {}
    df_agg = df_source.groupby(['NATUREZA_FINAL', 'PROJETO'])['Valor_Executado'].sum().reset_index()
    df_agg = df_agg[df_agg['Valor_Executado'] > 0]
    if df_agg.empty: return {}
    def format_projetos(group):
        top_projetos = group.nlargest(3, 'Valor_Executado')
        return '<br>'.join([f"- {row.PROJETO} ({formatar_brl(row.Valor_Executado)})" for _, row in top_projetos.iterrows()])
    projetos_por_natureza = df_agg.groupby('NATUREZA_FINAL').apply(format_projetos, include_groups=False).to_dict()
    df_natureza_sum = df_agg.groupby('NATUREZA_FINAL')['Valor_Executado'].sum().reset_index()
    return {
        'labels': df_natureza_sum['NATUREZA_FINAL'].tolist(),
        'parents': [""] * len(df_natureza_sum),
        'values': df_natureza_sum['Valor_Executado'].tolist(),
        'projetos': df_natureza_sum['NATUREZA_FINAL'].map(projetos_por_natureza).fillna('').tolist()
    }

def preparar_dados_orcamento_ocioso(df_unidade: pd.DataFrame) -> dict:
    """Prepara os dados para o gráfico de orçamento ocioso (Chart.js), com lógica robusta à prova de falhas."""
    if df_unidade.empty: return {}

    df_unidade['saldo_nao_executado'] = df_unidade['Valor_Planejado'].fillna(0) - df_unidade['Valor_Executado'].fillna(0)

    saldo_total_por_projeto = df_unidade.groupby('PROJETO')['saldo_nao_executado'].sum()
    top_7_projetos_com_saldo_positivo = saldo_total_por_projeto[saldo_total_por_projeto > 0].nlargest(7)

    if top_7_projetos_com_saldo_positivo.empty: return {}

    top_7_nomes_projetos = top_7_projetos_com_saldo_positivo.index
    df_filtrado = df_unidade[df_unidade['PROJETO'].isin(top_7_nomes_projetos)].copy()
    
    df_pivot = df_filtrado.pivot_table(index='PROJETO', columns='tipo_projeto', values='saldo_nao_executado', aggfunc='sum', fill_value=0)
    df_pivot = df_pivot.reindex(top_7_nomes_projetos).fillna(0)

    # --- CORREÇÃO DEFINITIVA ---
    # Verifica a existência da coluna ANTES de tentar acessá-la.
    values_exclusivo = df_pivot['Exclusivo'].tolist() if 'Exclusivo' in df_pivot.columns else [0] * len(df_pivot)
    values_compartilhado = df_pivot['Compartilhado'].tolist() if 'Compartilhado' in df_pivot.columns else [0] * len(df_pivot)

    def formatar_acoes(group):
        top_acoes = group[group['saldo_nao_executado'] > 0].groupby('ACAO')['saldo_nao_executado'].sum().nlargest(3)
        return [f"- {acao}: {formatar_brl(saldo)}" for acao, saldo in top_acoes.items()]

    detalhes_por_projeto = df_filtrado.groupby('PROJETO').apply(formatar_acoes, include_groups=False).reindex(df_pivot.index, fill_value=[])
    tipos_projeto = df_filtrado.drop_duplicates(subset=['PROJETO']).set_index('PROJETO')['tipo_projeto']
    
    detalhes_exclusivo = [detalhes_por_projeto.get(proj, []) if tipos_projeto.get(proj) == 'Exclusivo' else [] for proj in df_pivot.index]
    detalhes_compartilhado = [detalhes_por_projeto.get(proj, []) if tipos_projeto.get(proj) == 'Compartilhado' else [] for proj in df_pivot.index]
    
    return {
        "labels": df_pivot.index.tolist(),
        "values_exclusivo": values_exclusivo,
        "values_compartilhado": values_compartilhado,
        "detalhes_exclusivo": detalhes_exclusivo,
        "detalhes_compartilhado": detalhes_compartilhado
    }

def preparar_dados_execucao_sem_planejamento(df_source: pd.DataFrame, tipo: str) -> dict:
    if df_source is None or df_source.empty: return {}
    df_agg = df_source.groupby(['NATUREZA_FINAL', 'PROJETO']).agg(
        Valor_Planejado=('Valor_Planejado', 'sum'),
        Valor_Executado=('Valor_Executado', 'sum')
    ).reset_index()
    df_sem_plan = df_agg[(df_agg['Valor_Planejado'] <= 0) & (df_agg['Valor_Executado'] > 0)]
    if df_sem_plan.empty: return {}
    def formatar_projetos(group):
        top = group.nlargest(3, 'Valor_Executado')
        return [f"- {row.PROJETO}: {formatar_brl(row.Valor_Executado)}" for _, row in top.iterrows()]
    df_sum = df_sem_plan.groupby('NATUREZA_FINAL')['Valor_Executado'].sum().sort_values(ascending=False)
    detalhes = df_sem_plan.groupby('NATUREZA_FINAL').apply(formatar_projetos, include_groups=False).reindex(df_sum.index)
    return {
        "labels": df_sum.index.tolist(),
        "values": df_sum.values.tolist(),
        "projetos": [d if isinstance(d, list) else [] for d in detalhes],
        "color": CORES['alert_danger'] if tipo == 'Exclusivo' else CORES['alert_warning']
    }
