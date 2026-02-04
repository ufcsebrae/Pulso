# visualizacao/componentes_plotly.py (VERSÃO CORRIGIDA)
import pandas as pd
import plotly.graph_objects as go
import numpy as np
from config.config import CORES

# ... (as funções criar_grafico_sunburst, criar_grafico_heatmap, criar_grafico_inercia permanecem inalteradas) ...

def criar_grafico_sunburst(df_exclusivos: pd.DataFrame) -> str:
    """Gera o código HTML de um gráfico Sunburst a partir dos dados de projetos exclusivos."""
    if df_exclusivos.empty:
        return '<div class="flex items-center justify-center h-full text-center text-gray-500">Sem dados para exibir.</div>'
    df_sun = df_exclusivos.groupby(['PROJETO', 'NATUREZA_FINAL']).agg(Valor_Planejado=('Valor_Planejado', 'sum'), Valor_Executado=('Valor_Executado', 'sum')).reset_index()
    df_sun = df_sun[df_sun['Valor_Planejado'] > 0]
    if df_sun.empty:
        return '<div class="flex items-center justify-center h-full text-center text-gray-500">Sem dados com orçamento planejado para exibir.</div>'
    df_sun['perc_exec'] = (df_sun['Valor_Executado'] / df_sun['Valor_Planejado']) * 100
    cores_projeto = df_sun.groupby('PROJETO').apply(lambda x: (x['Valor_Executado'].sum() / x['Valor_Planejado'].sum()) * 100 if x['Valor_Planejado'].sum() > 0 else 0, include_groups=False).tolist()
    fig = go.Figure()
    fig.add_trace(go.Sunburst(labels=df_sun['NATUREZA_FINAL'].tolist() + df_sun['PROJETO'].unique().tolist(), parents=df_sun['PROJETO'].tolist() + [""] * df_sun['PROJETO'].nunique(), values=df_sun['Valor_Planejado'].tolist() + df_sun.groupby('PROJETO')['Valor_Planejado'].sum().tolist(), branchvalues='total', marker=dict(colors=df_sun['perc_exec'].tolist() + cores_projeto, colorscale='RdYlGn', cmin=0, cmax=120, colorbar=dict(title='% Executado')), hovertemplate='<b>%{label}</b><br>Planejado: %{value:,.2f}<br>Execução: %{color:.1f}%<extra></extra>'))
    fig.update_layout(margin=dict(t=10, l=10, r=10, b=10))
    return fig.to_html(full_html=False)

def criar_grafico_heatmap(df_exclusivos: pd.DataFrame) -> str:
    """Gera o código HTML de um gráfico Heatmap da performance de execução."""
    if df_exclusivos.empty: return '<div class="flex items-center justify-center h-full text-center text-gray-500">Sem dados para exibir.</div>'
    df_agg = df_exclusivos.groupby(['PROJETO', 'NATUREZA_FINAL']).agg(Valor_Planejado=('Valor_Planejado', 'sum'), Valor_Executado=('Valor_Executado', 'sum')).reset_index()
    df_agg = df_agg[df_agg['Valor_Planejado'] > 0]
    if df_agg.empty: return '<div class="flex items-center justify-center h-full text-center text-gray-500">Sem dados com orçamento planejado para exibir.</div>'
    df_agg['perc_exec'] = (df_agg['Valor_Executado'] / df_agg['Valor_Planejado']) * 100
    pivot_df = df_agg.pivot_table(index='PROJETO', columns='NATUREZA_FINAL', values='perc_exec', fill_value=None)
    if pivot_df.empty: return '<div class="flex items-center justify-center h-full text-center text-gray-500">Não foi possível criar a visão pivotada.</div>'
    num_projetos = len(pivot_df.index)
    dynamic_height = max(400, num_projetos * 35)
    fig = go.Figure(data=go.Heatmap(z=pivot_df.values, x=pivot_df.columns, y=pivot_df.index, colorscale='RdYlGn', zmin=0, zmid=80, zmax=120, hovertemplate='Projeto: %{y}<br>Natureza: %{x}<br>Execução: %{z:.1f}%<extra></extra>', xgap=1, ygap=1))
    fig.update_layout(yaxis_nticks=num_projetos, xaxis_tickangle=-45, height=dynamic_height, margin=dict(l=250))
    return fig.to_html(full_html=False)

def criar_grafico_inercia(df_exclusivos: pd.DataFrame) -> str:
    """Gera o código HTML de um gráfico de barras para a inércia de execução."""
    if df_exclusivos.empty: return '<div class="flex items-center justify-center h-full text-center text-gray-500">Sem dados para exibir.</div>'
    def calcular_inercia(group):
        if (plan_mes := group[group['Valor_Planejado'] > 0]['MES'].min()) and pd.notna(plan_mes):
            if (gasto_mes := group[group['Valor_Executado'] > 0]['MES'].min()) and pd.notna(gasto_mes):
                return gasto_mes - plan_mes
        return np.nan
    df_inercia = df_exclusivos.groupby(['PROJETO', 'ACAO', 'NATUREZA_FINAL']).apply(calcular_inercia, include_groups=False).dropna()
    if df_inercia.empty: return '<div class="flex items-center justify-center h-full text-center text-gray-500">Não há dados de inércia para calcular.</div>'
    df_inercia = df_inercia.reset_index(name='inercia_meses')
    df_inercia = df_inercia[df_inercia['inercia_meses'] > 0]
    if df_inercia.empty: return '<div class="flex items-center justify-center h-full text-center text-gray-500">Nenhum atraso de execução identificado.</div>'
    idx_max = df_inercia.groupby('NATUREZA_FINAL')['inercia_meses'].idxmax()
    df_maior_inercia = df_inercia.loc[idx_max].sort_values(by='inercia_meses', ascending=False)
    hover_text = [f"<b>Projeto:</b> {row['PROJETO']}<br><b>Ação:</b> {row['ACAO']}<br><b>Atraso:</b> {row['inercia_meses']:.0f} meses" for _, row in df_maior_inercia.iterrows()]
    fig = go.Figure()
    fig.add_trace(go.Bar(x=df_maior_inercia['inercia_meses'], y=df_maior_inercia['NATUREZA_FINAL'], orientation='h', marker_color=CORES['alert_danger'], text=df_maior_inercia['inercia_meses'], textposition='outside', hoverinfo='text', hovertext=hover_text))
    fig.update_layout(plot_bgcolor='white', yaxis=dict(autorange="reversed"), margin=dict(l=250))
    return fig.to_html(full_html=False)
    
# --- FUNÇÃO ATUALIZADA ---
def criar_tabela_html(df: pd.DataFrame | None, titulo: str) -> str:
    """
    Gera o código HTML para um título e uma tabela estilizada, SEM o contêiner de card.
    """
    if df is None or df.empty:
        return f"""
        <div class="mt-6">
            <h3 class="text-lg font-semibold text-gray-700 mb-2">{titulo}</h3>
            <div class="p-4 border rounded-lg bg-gray-50 text-center text-gray-500">Nenhum dado encontrado para esta visualização.</div>
        </div>
        """

    # Estilos que mimetizam a aparência do Tailwind UI para tabelas
    table_style = 'width: 100%; border-collapse: collapse;'
    th_style = 'background-color: #f9fafb; border-bottom: 1px solid #e5e7eb; padding: 0.75rem 1rem; text-align: left; font-size: 0.875rem; font-weight: 600; color: #374151;'
    td_style = 'border-bottom: 1px solid #e5e7eb; padding: 0.75rem 1rem; font-size: 0.875rem; color: #1f2937;'
    
    header_html = ''.join([f'<th style="{th_style}">{col}</th>' for col in df.columns])
    
    rows_html = ''
    for _, row in df.iterrows():
        row_html = ''.join([f'<td style="{td_style}">{val}</td>' for val in row])
        rows_html += f'<tr>{row_html}</tr>'

    return f"""
    <div class="mt-8"> <!-- Adiciona um espaçamento entre as tabelas, se houver mais de uma -->
        <h3 class="text-lg font-semibold text-gray-800 mb-2">{titulo}</h3>
        <div class="overflow-x-auto shadow-sm ring-1 ring-gray-900/5 rounded-lg">
            <table style="{table_style}">
                <thead style="background-color: #f9fafb;">
                    <tr>{header_html}</tr>
                </thead>
                <tbody class="bg-white">
                    {rows_html}
                </tbody>
            </table>
        </div>
    </div>
    """
