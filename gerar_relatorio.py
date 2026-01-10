# -*- coding: utf-8 -*-
"""
Script final para gerar um relatório HTML interativo, com Visão Geral, 
Evolução por Valor, Evolução Percentual Acumulada e Tabelas Detalhadas 
com análise de execução trimestral.
"""

# %%
# ## 1. Importações e Configuração
# ==============================================================================
import pandas as pd
from sqlalchemy import create_engine
import os
from dotenv import load_dotenv
import numpy as np
import plotly.graph_objects as go
import plotly.io as pio

print("## 1. Importações e Configuração")

# --- Funções de Formatação ---
def formatar_numero_kpi(num):
    """Formata um número para os KPIs da visão geral, com 'R$'."""
    if num is None or pd.isna(num): return "N/A"
    if abs(num) >= 1_000_000: return f"R$ {num/1_000_000:,.2f} M"
    if abs(num) >= 1_000: return f"R$ {num/1_000:,.1f} k"
    return f"R$ {num:,.2f}"

def formatar_valor_tabela(num):
    """Formata um número para as tabelas, sem 'R$' e com 'M'/'k'."""
    if num is None or pd.isna(num) or num == 0: return "-"
    if abs(num) >= 1_000_000: return f"{num/1_000_000:,.1f}M"
    if abs(num) >= 1_000: return f"{num/1_000:,.1f}k"
    return f"{num:,.0f}"

# Configurações de exibição do Pandas e Plotly
pio.templates.default = "plotly_white"
pd.options.display.float_format = '{:,.2f}'.format

# --- Conexão com o Banco de Dados ---
load_dotenv()
DB_SERVER = os.getenv("DB_SERVER_FINANCA")
DB_DATABASE = os.getenv("DB_DATABASE_FINANCA")
connection_string = (f"mssql+pyodbc:///?odbc_connect=DRIVER={{ODBC Driver 17 for SQL Server}};"
                   f"SERVER={DB_SERVER};DATABASE={DB_DATABASE};Trusted_Connection=yes;")
engine = create_engine(connection_string, fast_executemany=True)
print("Conexão com o banco de dados estabelecida.\n")


# %%
# ## 2. Carregamento e Preparação dos Dados
# ==============================================================================
print("## 2. Carregamento e Preparação dos Dados")
PPA_FILTRO = 'PPA 2025 - 2025/DEZ'
ANO_FILTRO = 2025
UNIDADE_EXEMPLO = 'DESENVOLVIMENTO SETORIAL E TERRITORIAL'

sql_query = f"SELECT * FROM dbo.vw_Analise_Planejado_vs_Executado_v2 WHERE nm_ppa = '{PPA_FILTRO}' AND nm_ano = {ANO_FILTRO}"
df_total = pd.read_sql(sql_query, engine)

# Limpeza e Padronização
df_total[['vl_planejado', 'vl_executado']] = df_total[['vl_planejado', 'vl_executado']].fillna(0)
df_total['nm_unidade_padronizada'] = df_total['nm_unidade'].str.upper().str.replace('SP - ', '', regex=False).str.strip()

# Classificação de Projetos
unidades_por_projeto = df_total.groupby('nm_projeto')['nm_unidade_padronizada'].nunique().reset_index()
unidades_por_projeto.rename(columns={'nm_unidade_padronizada': 'contagem_unidades'}, inplace=True)
unidades_por_projeto['tipo_projeto'] = np.where(unidades_por_projeto['contagem_unidades'] > 1, 'Compartilhado', 'Exclusivo')
df_total = pd.merge(df_total, unidades_por_projeto[['nm_projeto', 'tipo_projeto']], on='nm_projeto', how='left')

# Adição e Ordenação de Trimestres
df_total['nm_mes_num'] = pd.to_numeric(df_total['nm_mes'], errors='coerce')
mapa_trimestre_num = {1: '1T', 2: '1T', 3: '1T', 4: '2T', 5: '2T', 6: '2T', 7: '3T', 8: '3T', 9: '3T', 10: '4T', 11: '4T', 12: '4T'}
df_total['nm_trimestre'] = df_total['nm_mes_num'].map(mapa_trimestre_num)
trimestre_dtype = pd.CategoricalDtype(categories=['1T', '2T', '3T', '4T'], ordered=True)
df_total['nm_trimestre'] = df_total['nm_trimestre'].astype(trimestre_dtype)
print("Dados preparados e classificados.\n")


# %%
# ## 3. Filtro e Cálculos para a Unidade do Relatório
# ==============================================================================
print("## 3. Filtro e Cálculos para a Unidade")
df_unidade_filtrada = df_total[df_total['nm_unidade_padronizada'] == UNIDADE_EXEMPLO].copy()
if df_unidade_filtrada.empty: exit(f"ERRO: Nenhum dado para a unidade '{UNIDADE_EXEMPLO}'.")

df_exclusivos_unidade = df_unidade_filtrada[df_unidade_filtrada['tipo_projeto'] == 'Exclusivo'].copy()
df_compartilhados_unidade = df_unidade_filtrada[df_unidade_filtrada['tipo_projeto'] == 'Compartilhado'].copy()

# Cálculos para KPIs da Visão Geral
total_planejado_unidade = df_unidade_filtrada['vl_planejado'].sum()
total_executado_unidade = df_unidade_filtrada['vl_executado'].sum()
perc_total = (total_executado_unidade / total_planejado_unidade * 100) if total_planejado_unidade > 0 else 0
kpi_total_projetos = df_unidade_filtrada['nm_projeto'].nunique()
kpi_total_acoes = df_unidade_filtrada.groupby(['nm_projeto', 'nm_acao'], observed=True).ngroups
kpi_total_planejado_str = formatar_numero_kpi(total_planejado_unidade)
kpi_total_executado_str = formatar_numero_kpi(total_executado_unidade)

planejado_exclusivos = df_exclusivos_unidade['vl_planejado'].sum()
executado_exclusivos = df_exclusivos_unidade['vl_executado'].sum()
perc_exclusivos = (executado_exclusivos / planejado_exclusivos * 100) if planejado_exclusivos > 0 else 0
kpi_exclusivos_projetos = df_exclusivos_unidade['nm_projeto'].nunique()
kpi_exclusivos_acoes = df_exclusivos_unidade.groupby(['nm_projeto', 'nm_acao'], observed=True).ngroups
kpi_exclusivos_planejado_str = formatar_numero_kpi(planejado_exclusivos)
kpi_exclusivos_executado_str = formatar_numero_kpi(executado_exclusivos)

planejado_compartilhados = df_compartilhados_unidade['vl_planejado'].sum()
executado_compartilhados = df_compartilhados_unidade['vl_executado'].sum()
perc_compartilhados = (executado_compartilhados / planejado_compartilhados * 100) if planejado_compartilhados > 0 else 0
kpi_compartilhados_projetos = df_compartilhados_unidade['nm_projeto'].nunique()
kpi_compartilhados_acoes = df_compartilhados_unidade.groupby(['nm_projeto', 'nm_acao'], observed=True).ngroups
kpi_compartilhados_planejado_str = formatar_numero_kpi(planejado_compartilhados)
kpi_compartilhados_executado_str = formatar_numero_kpi(executado_compartilhados)


# %%
# ## 4. Geração dos Gráficos
# ==============================================================================
print("## 4. Geração dos Gráficos")

# --- 4.1 Gráficos de Gauge (Visão Geral) ---
perc_contrib_exclusivos = (executado_exclusivos / total_executado_unidade * 100) if total_executado_unidade > 0 else 0
perc_contrib_compartilhados = (executado_compartilhados / total_executado_unidade * 100) if total_executado_unidade > 0 else 0
texto_contribuicao = f"% do Tot.: <br> Exclusivos: {perc_contrib_exclusivos:.2f}% | Compartilhados: {perc_contrib_compartilhados:.2f}%"

fig_gauge_total = go.Figure(go.Indicator(mode="gauge+number", value=perc_total, title={'text': "Execução Total"}, number={'valueformat': '.2f', 'suffix': '%'}, gauge={'axis': {'range': [None, 100]}, 'bar': {'color': "#004085"}}))
fig_gauge_total.add_annotation(x=0.5, y=-0.18, text=texto_contribuicao, showarrow=False, font=dict(size=12, color="#6c757d"))
fig_gauge_total.update_layout(height=250, margin=dict(t=50, b=30))

fig_gauge_exclusivos = go.Figure(go.Indicator(mode="gauge+number", value=perc_exclusivos, title={'text': "Projetos Exclusivos"}, number={'valueformat': '.2f', 'suffix': '%'}, gauge={'axis': {'range': [None, 100]}, 'bar': {'color': "green"}}))
fig_gauge_exclusivos.update_layout(height=250, margin=dict(t=50, b=20))

fig_gauge_compartilhados = go.Figure(go.Indicator(mode="gauge+number", value=perc_compartilhados, title={'text': "Projetos Compartilhados"}, number={'valueformat': '.2f', 'suffix': '%'}, gauge={'axis': {'range': [None, 100]}, 'bar': {'color': "goldenrod"}}))
fig_gauge_compartilhados.update_layout(height=250, margin=dict(t=50, b=20))

# --- 4.2 Gráficos de Linha (Evolução por Valor) ---
HOVER_TEMPLATE_VALOR = '<b>%{data.name}</b><br>Valor: R$ %{y:,.2f}<extra></extra>'
execucao_mensal_exclusivos = df_exclusivos_unidade.groupby('nm_mes_num', observed=False).agg(Planejado=('vl_planejado', 'sum'), Executado=('vl_executado', 'sum')).reset_index()
fig_line_valor_exclusivos = go.Figure()
fig_line_valor_exclusivos.add_trace(go.Scatter(x=execucao_mensal_exclusivos['nm_mes_num'], y=execucao_mensal_exclusivos['Planejado'], mode='lines', name='Planejado', line=dict(color='lightblue', dash='dot'), hovertemplate=HOVER_TEMPLATE_VALOR))
fig_line_valor_exclusivos.add_trace(go.Scatter(x=execucao_mensal_exclusivos['nm_mes_num'], y=execucao_mensal_exclusivos['Executado'], mode='lines', name='Executado', line=dict(color='green'), hovertemplate=HOVER_TEMPLATE_VALOR))
fig_line_valor_exclusivos.update_layout(title='Valores Mensais - Exclusivos', xaxis_title='Mês', yaxis_title='Valor (R$)', hovermode='x unified', separators=',.')

execucao_mensal_compartilhados = df_compartilhados_unidade.groupby('nm_mes_num', observed=False).agg(Planejado=('vl_planejado', 'sum'), Executado=('vl_executado', 'sum')).reset_index()
fig_line_valor_compartilhados = go.Figure()
fig_line_valor_compartilhados.add_trace(go.Scatter(x=execucao_mensal_compartilhados['nm_mes_num'], y=execucao_mensal_compartilhados['Planejado'], mode='lines', name='Planejado', line=dict(color='moccasin', dash='dot'), hovertemplate=HOVER_TEMPLATE_VALOR))
fig_line_valor_compartilhados.add_trace(go.Scatter(x=execucao_mensal_compartilhados['nm_mes_num'], y=execucao_mensal_compartilhados['Executado'], mode='lines', name='Executado', line=dict(color='goldenrod'), hovertemplate=HOVER_TEMPLATE_VALOR))
fig_line_valor_compartilhados.update_layout(title='Valores Mensais - Compartilhados', xaxis_title='Mês', yaxis_title='Valor (R$)', hovermode='x unified', separators=',.')

# --- 4.3 Gráficos de Linha (Percentual Acumulado por Trimestre) ---
def criar_grafico_execucao_trimestral(df, tipo_projeto, total_anual_planejado):
    cor_map = {'Exclusivos': 'Green', 'Compartilhados': 'goldenrod'}
    cor_map_light = {'Exclusivos': 'lightgreen', 'Compartilhados': 'moccasin'}
    if df.empty: return go.Figure().update_layout(title=f'Execução Percentual - {tipo_projeto} (Sem Dados)')
    
    dados_trimestrais = df.groupby('nm_trimestre', observed=False).agg(Planejado_T=('vl_planejado', 'sum'), Executado_T=('vl_executado', 'sum')).reset_index()
    
    dados_trimestrais['%_Exec_Trimestral'] = np.where(dados_trimestrais['Planejado_T'] > 0, (dados_trimestrais['Executado_T'] / dados_trimestrais['Planejado_T']) * 100, 0)
    dados_trimestrais['Exec_Acumulado'] = dados_trimestrais['Executado_T'].cumsum()
    dados_trimestrais['%_Acum_Total'] = np.where(total_anual_planejado > 0, (dados_trimestrais['Exec_Acumulado'] / total_anual_planejado) * 100, 0)
    
    fig = go.Figure()
    fig.add_trace(go.Scatter(x=dados_trimestrais['nm_trimestre'], y=dados_trimestrais['%_Exec_Trimestral'], mode='lines+markers', name='Execução no Trimestre (%)', line=dict(color=cor_map_light[tipo_projeto], dash='dot'), hovertemplate='<b>%{x}</b><br>Exec no Trimestre: %{y:.1f}%<extra></extra>'))
    fig.add_trace(go.Scatter(x=dados_trimestrais['nm_trimestre'], y=dados_trimestrais['%_Acum_Total'], mode='lines+markers', name='Acumulado sobre Total Anual (%)', line=dict(color=cor_map[tipo_projeto]), hovertemplate='<b>%{x}</b><br>Acumulado sobre Total: %{y:.1f}%<extra></extra>'))
    fig.add_hline(y=100, line_width=2, line_dash="dash", line_color="gray", annotation_text="Meta 100%", annotation_position="bottom right")
    fig.update_layout(title=f'Execução Percentual - {tipo_projeto}', xaxis_title='Trimestre', yaxis_title='Percentual (%)', hovermode='x unified', yaxis=dict(ticksuffix='%'), legend=dict(yanchor="top", y=0.98, xanchor="left", x=0.01))
    return fig

fig_perc_exclusivos = criar_grafico_execucao_trimestral(df_exclusivos_unidade, 'Exclusivos', planejado_exclusivos)
fig_perc_compartilhados = criar_grafico_execucao_trimestral(df_compartilhados_unidade, 'Compartilhados', planejado_compartilhados)

print("Todos os gráficos foram gerados.\n")


# %%
# ## 5. Geração das Tabelas Analíticas
# ==============================================================================
print("## 5. Geração das Tabelas Analíticas")

def criar_tabela_analitica_trimestral(df, index_col, index_col_name):
    """NOVA VERSÃO: Cria tabelas com Executado no trimestre e % de execução no trimestre."""
    if df.empty:
        return pd.DataFrame({index_col_name: []})

    # 1. Agrupar dados por item e trimestre
    base = df.groupby([index_col, 'nm_trimestre'], observed=False).agg(
        Executado=('vl_executado', 'sum'),
        Planejado=('vl_planejado', 'sum')
    ).reset_index()

    # 2. Construir tabela de resultados brutos
    itens_unicos = sorted(df[index_col].unique())
    dados_brutos = []
    for item in itens_unicos:
        row_data = {'Projeto': item}
        total_exec_ano = 0
        for t in ['1T', '2T', '3T', '4T']:
            trim_data = base[(base[index_col] == item) & (base['nm_trimestre'] == t)]
            executado = trim_data['Executado'].iloc[0] if not trim_data.empty else 0
            planejado = trim_data['Planejado'].iloc[0] if not trim_data.empty else 0
            
            row_data[f'{t}_Exec'] = executado
            row_data[f'{t}_%'] = (executado / planejado * 100) if planejado > 0 else 0.0
            total_exec_ano += executado
        row_data['Total_Exec_Ano'] = total_exec_ano
        dados_brutos.append(row_data)

    tabela_bruta = pd.DataFrame(dados_brutos)

    # 3. Adicionar linha de Total Geral
    total_geral = df.groupby('nm_trimestre', observed=False).agg(
        Executado=('vl_executado', 'sum'),
        Planejado=('vl_planejado', 'sum')
    ).reset_index()
    total_row_data = {'Projeto': 'Total Geral'}
    total_exec_ano_geral = 0
    for t in ['1T', '2T', '3T', '4T']:
        trim_data = total_geral[total_geral['nm_trimestre'] == t]
        executado = trim_data['Executado'].iloc[0] if not trim_data.empty else 0
        planejado = trim_data['Planejado'].iloc[0] if not trim_data.empty else 0
        
        total_row_data[f'{t}_Exec'] = executado
        total_row_data[f'{t}_%'] = (executado / planejado * 100) if planejado > 0 else 0.0
        total_exec_ano_geral += executado
    total_row_data['Total_Exec_Ano'] = total_exec_ano_geral

    tabela_bruta = pd.concat([tabela_bruta, pd.DataFrame([total_row_data])], ignore_index=True)
    
    # 4. Ordenar
    tabela_bruta['sort_order'] = np.where(tabela_bruta['Projeto'] == 'Total Geral', 1, 0)
    tabela_bruta = tabela_bruta.sort_values(by=['sort_order', 'Total_Exec_Ano'], ascending=[True, False]).drop(columns=['sort_order'])

    # 5. Formatar para exibição final
    tabela_formatada = pd.DataFrame()
    tabela_formatada[index_col_name] = tabela_bruta['Projeto']
    for t in ['1T', '2T', '3T', '4T']:
        tabela_formatada[f'{t} Exec'] = tabela_bruta[f'{t}_Exec'].apply(formatar_valor_tabela)
        tabela_formatada[f'{t} %'] = tabela_bruta[f'{t}_%'].apply('{:.1f}%'.format)
    tabela_formatada['Total Exec Ano'] = tabela_bruta['Total_Exec_Ano'].apply(formatar_valor_tabela)
        
    return tabela_formatada

# Geração das 4 tabelas
tb_projetos_exc = criar_tabela_analitica_trimestral(df_exclusivos_unidade, 'nm_projeto', 'Projeto')
tb_projetos_comp = criar_tabela_analitica_trimestral(df_compartilhados_unidade, 'nm_projeto', 'Projeto')
tb_natureza_exc = criar_tabela_analitica_trimestral(df_exclusivos_unidade, 'nm_desc_natureza_orcamentaria_origem', 'Natureza Orçamentária')
tb_natureza_comp = criar_tabela_analitica_trimestral(df_compartilhados_unidade, 'nm_desc_natureza_orcamentaria_origem', 'Natureza Orçamentária')

print("Tabelas analíticas formatadas criadas com sucesso.\n")


# %%
# ## 6. Montagem e Salvamento do Arquivo HTML
# ==============================================================================
print("## 6. Montagem e Salvamento do Arquivo HTML")

# Converte todos os elementos para HTML
html_gauge_total = pio.to_html(fig_gauge_total, full_html=False, include_plotlyjs='cdn')
html_gauge_exclusivos = pio.to_html(fig_gauge_exclusivos, full_html=False, include_plotlyjs=False)
html_gauge_compartilhados = pio.to_html(fig_gauge_compartilhados, full_html=False, include_plotlyjs=False)
html_line_valor_exclusivos = pio.to_html(fig_line_valor_exclusivos, full_html=False, include_plotlyjs=False)
html_line_valor_compartilhados = pio.to_html(fig_line_valor_compartilhados, full_html=False, include_plotlyjs=False)
html_perc_exclusivos = fig_perc_exclusivos.to_html(full_html=False, include_plotlyjs=False)
html_perc_compartilhados = fig_perc_compartilhados.to_html(full_html=False, include_plotlyjs=False)

# Usa 'escape=False' para renderizar o HTML da formatação condicional
html_table_projetos_exc = tb_projetos_exc.to_html(classes='table table-striped table-hover table-sm', index=False, border=0, escape=False)
html_table_projetos_comp = tb_projetos_comp.to_html(classes='table table-striped table-hover table-sm', index=False, border=0, escape=False)
html_table_natureza_exc = tb_natureza_exc.to_html(classes='table table-striped table-hover table-sm', index=False, border=0, escape=False)
html_table_natureza_comp = tb_natureza_comp.to_html(classes='table table-striped table-hover table-sm', index=False, border=0, escape=False)

html_string = f'''
<html>
<head>
    <title>Relatório: {UNIDADE_EXEMPLO}</title>
    <link rel="stylesheet" href="https://stackpath.bootstrapcdn.com/bootstrap/4.5.2/css/bootstrap.min.css">
    <script src="https://cdn.plot.ly/plotly-latest.min.js"></script>
    <style>
        body {{ font-family: Arial, sans-serif; padding: 20px; background-color: #f8f9fa; }}
        h1, h2, h3, h4 {{ color: #004085; }}
        .card {{ margin-top: 20px; box-shadow: 0 4px 8px 0 rgba(0,0,0,0.2); }}
        .card-header {{ background-color: #004085; color: white; }}
        .kpi-box, .kpi-values-box {{ text-align: center; padding: 10px 0; }}
        .kpi-box {{ border-top: 1px solid #dee2e6; }}
        .kpi-value {{ font-size: 1.8rem; font-weight: bold; color: #004085; }}
        .kpi-label {{ font-size: 0.9rem; color: #6c757d; text-transform: uppercase; }}
        .kpi-values-box {{ display: flex; justify-content: space-around; }}
        .kpi-financial-value {{ font-size: 1.5rem; font-weight: bold; }}
        .kpi-financial-label {{ font-size: 0.8rem; color: #6c757d; text-transform: uppercase; }}
        .green-text {{ color: #28a745; }}
        .blue-text {{ color: #007bff; }}
        .plotly-graph-div {{ min-height: 350px; }}
        .gauge-container .plotly-graph-div {{ min-height: 250px; }}
        .table-responsive {{ max-height: 450px; overflow-y: auto; }}
        .table th, .table td {{ text-align: left; vertical-align: middle; white-space: nowrap; padding: 0.4rem;}}
        .table td:not(:first-child), .table th:not(:first-child) {{ text-align: right; }}
    </style>
</head>
<body>
<div class="container-fluid">
    <div class="text-center mt-4"><h1>Relatório de Performance Orçamentária</h1><h2 class="text-muted">{UNIDADE_EXEMPLO}</h2></div><hr>
    
    <div class="card">
        <div class="card-header"><h3>Visão Geral da Execução</h3></div>
        <div class="card-body gauge-container">
            <div class="row">
                <div class="col-lg-4">{html_gauge_total}
                    <div class="kpi-values-box"><div class="text-center"><div class="kpi-financial-value blue-text">{kpi_total_planejado_str}</div><div class="kpi-financial-label">Planejado</div></div><div class="text-center"><div class="kpi-financial-value green-text">{kpi_total_executado_str}</div><div class="kpi-financial-label">Executado</div></div></div>
                    <div class="kpi-box"><div class="row"><div class="col-6"><div class="kpi-value">{kpi_total_projetos}</div><div class="kpi-label">Projetos</div></div><div class="col-6"><div class="kpi-value">{kpi_total_acoes}</div><div class="kpi-label">Ações</div></div></div></div>
                </div>
                <div class="col-lg-4">{html_gauge_exclusivos}
                     <div class="kpi-values-box"><div class="text-center"><div class="kpi-financial-value blue-text">{kpi_exclusivos_planejado_str}</div><div class="kpi-financial-label">Planejado</div></div><div class="text-center"><div class="kpi-financial-value green-text">{kpi_exclusivos_executado_str}</div><div class="kpi-financial-label">Executado</div></div></div>
                    <div class="kpi-box"><div class="row"><div class="col-6"><div class="kpi-value">{kpi_exclusivos_projetos}</div><div class="kpi-label">Projetos</div></div><div class="col-6"><div class="kpi-value">{kpi_exclusivos_acoes}</div><div class="kpi-label">Ações</div></div></div></div>
                </div>
                <div class="col-lg-4">{html_gauge_compartilhados}
                    <div class="kpi-values-box"><div class="text-center"><div class="kpi-financial-value blue-text">{kpi_compartilhados_planejado_str}</div><div class="kpi-financial-label">Planejado</div></div><div class="text-center"><div class="kpi-financial-value green-text">{kpi_compartilhados_executado_str}</div><div class="kpi-financial-label">Executado</div></div></div>
                    <div class="kpi-box"><div class="row"><div class="col-6"><div class="kpi-value">{kpi_compartilhados_projetos}</div><div class="kpi-label">Projetos</div></div><div class="col-6"><div class="kpi-value">{kpi_compartilhados_acoes}</div><div class="kpi-label">Ações</div></div></div></div>
                </div>
            </div>
        </div>
    </div>

    <div class="card">
        <div class="card-header"><h3>Evolução Mensal (Planejado vs. Executado)</h3></div>
        <div class="card-body">
            <div class="row">
                <div class="col-lg-6">{html_line_valor_exclusivos}</div>
                <div class="col-lg-6">{html_line_valor_compartilhados}</div>
            </div>
        </div>
    </div>

    <div class="card">
        <div class="card-header"><h3>Execução Percentual Trimestral</h3></div>
        <div class="card-body">
            <div class="row">
                <div class="col-lg-6">{html_perc_exclusivos}</div>
                <div class="col-lg-6">{html_perc_compartilhados}</div>
            </div>
        </div>
    </div>

    <div class="card">
        <div class="card-header"><h3>Análise Detalhada por Trimestre</h3></div>
        <div class="card-body">
            <h4 class="mt-3">Projetos Exclusivos</h4>
            <div class="table-responsive">{html_table_projetos_exc}</div>
            
            <h4 class="mt-4">Projetos Compartilhados</h4>
            <div class="table-responsive">{html_table_projetos_comp}</div>

            <hr class="my-4">

            <h4 class="mt-4">Natureza Orçamentária (Projetos Exclusivos)</h4>
            <div class="table-responsive">{html_table_natureza_exc}</div>
            
            <h4 class="mt-4">Natureza Orçamentária (Projetos Compartilhados)</h4>
            <div class="table-responsive">{html_table_natureza_comp}</div>
        </div>
    </div>
</div>
</body>
</html>
'''

# Salva o arquivo final
nome_arquivo_html = f"relatorio_{UNIDADE_EXEMPLO.replace(' ', '_')}.html"
with open(nome_arquivo_html, 'w', encoding='utf-8') as f:
    f.write(html_string)

print(f"Relatório HTML interativo salvo com sucesso como '{nome_arquivo_html}'")
print("\n--- FIM DO SCRIPT ---")
