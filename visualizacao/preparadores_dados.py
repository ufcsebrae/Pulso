# visualizacao/preparadores_dados.py
import pandas as pd
from processamento.processamento_dados_base import formatar_brl

def preparar_dados_kpi(df_unidade: pd.DataFrame, df_exclusivos: pd.DataFrame, df_compartilhados: pd.DataFrame, unidade_nova: str) -> dict:
    """Prepara o dicionário com os principais KPIs da unidade."""
    
    def safe_div(numerator, denominator):
        return (numerator / denominator * 100) if denominator > 0 else 0

    kpi_total_planejado = df_unidade['Valor_Planejado'].sum()
    kpi_exclusivo_planejado = df_exclusivos['Valor_Planejado'].sum()
    kpi_compartilhado_planejado = df_compartilhados['Valor_Planejado'].sum()

    kpi_total_executado = df_unidade['Valor_Executado'].sum()
    kpi_exclusivo_executado = df_exclusivos['Valor_Executado'].sum()
    kpi_compartilhado_executado = df_compartilhados['Valor_Executado'].sum()

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
    """Prepara os dados para o gráfico de tendência mensal (Chart.js)."""
    df_trend = df_unidade.groupby(['MES', 'tipo_projeto'])['Valor_Executado'].sum().unstack(fill_value=0).reindex(range(1, 13), fill_value=0)
    df_trend['Total'] = df_trend.sum(axis=1)
    
    return {
        "labels": ['Jan', 'Fev', 'Mar', 'Abr', 'Mai', 'Jun', 'Jul', 'Ago', 'Set', 'Out', 'Nov', 'Dez'],
        "executed_total": df_trend['Total'].tolist(),
        "executed_exclusivo": df_trend.get('Exclusivo', pd.Series([0]*12)).tolist(),
        "executed_compartilhado": df_trend.get('Compartilhado', pd.Series([0]*12)).tolist()
    }

def preparar_dados_treemap(df_source: pd.DataFrame) -> dict:
    """Prepara os dados para o gráfico treemap (usado via JS no frontend)."""
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
    """Prepara os dados para o gráfico de orçamento ocioso (Chart.js)."""
    df_unidade['saldo_nao_executado'] = df_unidade['Valor_Planejado'].fillna(0) - df_unidade['Valor_Executado'].fillna(0)
    df_top_ocioso = df_unidade[df_unidade['saldo_nao_executado'] > 0].nlargest(7, 'saldo_nao_executado')
    
    if df_top_ocioso.empty: return {}
    
    df_pivot = df_top_ocioso.pivot_table(index='PROJETO', columns='tipo_projeto', values='saldo_nao_executado', fill_value=0)
    df_pivot = df_pivot.reindex(df_top_ocioso.groupby('PROJETO')['saldo_nao_executado'].sum().nlargest(7).index)

    def formatar_acoes(group):
        top_acoes = group.groupby('ACAO')['saldo_nao_executado'].sum().nlargest(3)
        return [f"- {acao}: {formatar_brl(saldo)}" for acao, saldo in top_acoes.items() if saldo > 0]

    df_detalhes = df_unidade[df_unidade['PROJETO'].isin(df_pivot.index)]
    detalhes_por_projeto = df_detalhes.groupby('PROJETO').apply(formatar_acoes, include_groups=False).reindex(df_pivot.index)
    
    detalhes_ex, detalhes_comp = [], []
    for projeto, row in df_pivot.iterrows():
        detalhe = detalhes_por_projeto.get(projeto, [])
        detalhes_ex.append(detalhe if row.get('Exclusivo', 0) > 0 else [])
        detalhes_comp.append(detalhe if row.get('Compartilhado', 0) > 0 else [])
    
    return {
        "labels": df_pivot.index.tolist(),
        "values_exclusivo": df_pivot.get('Exclusivo', pd.Series(0, index=df_pivot.index)).fillna(0).tolist(),
        "values_compartilhado": df_pivot.get('Compartilhado', pd.Series(0, index=df_pivot.index)).fillna(0).tolist(),
        "detalhes_exclusivo": detalhes_ex,
        "detalhes_compartilhado": detalhes_comp
    }

def preparar_dados_execucao_sem_planejamento(df_source: pd.DataFrame) -> dict:
    """Prepara dados de gastos sem orçamento (Chart.js)."""
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
        "projetos": [d if isinstance(d, list) else [] for d in detalhes]
    }
