# -*- coding: utf-8 -*-
# gerar_relatorio.py

import argparse
import logging
import sys
import os
from typing import Any, Dict, List

import numpy as np
import pandas as pd
import plotly.graph_objects as go
import plotly.io as pio

# --- Inicialização ---
try:
    from logger_config import configurar_logger
    configurar_logger("geracao_relatorios.log")
    from inicializacao import carregar_drivers_externos
    carregar_drivers_externos()
except (ImportError, FileNotFoundError, Exception) as e:
    logging.basicConfig(level=logging.INFO)
    logging.critical("Falha na inicialização: %s", e, exc_info=True)
    sys.exit(1)

logger = logging.getLogger(__name__)

try:
    from config import CONFIG
    from database import get_conexao
except ImportError as e:
    logger.critical("Erro de importação de config/database: %s.", e)
    sys.exit(1)


# --- Funções de Formatação e Lógica ---

def formatar_numero_kpi(num):
    if num is None or pd.isna(num): return "N/A"
    if abs(num) >= 1_000_000: return f"R$ {num/1_000_000:,.2f} M"
    if abs(num) >= 1_000: return f"R$ {num/1_000:,.1f} k"
    return f"R$ {num:,.2f}"

pio.templates.default = "plotly_white"
pd.options.display.float_format = '{:,.2f}'.format

def carregar_mapa_naturezas() -> Dict[str, str]:
    """Carrega o mapeamento de nomes de natureza do CSV."""
    caminho_csv = CONFIG.paths.mapa_naturezas_csv
    if not caminho_csv.exists():
        logger.warning(f"Arquivo de mapeamento '{caminho_csv.name}' não encontrado. Usando nomes originais.")
        return {}
    try:
        df_mapa = pd.read_csv(caminho_csv)
        return pd.Series(df_mapa.nome_simplificado.values, index=df_mapa.nome_original).to_dict()
    except Exception as e:
        logger.error(f"Falha ao carregar mapa de naturezas: {e}")
        return {}

def obter_unidades_disponiveis(engine_db: Any) -> List[str]:
    logger.info("Consultando unidades de negócio disponíveis...")
    PPA_FILTRO = os.getenv("PPA_FILTRO", 'PPA 2025 - 2025/DEZ')
    ANO_FILTRO = int(os.getenv("ANO_FILTRO", 2025))
    query_unidades = "SELECT DISTINCT UNIDADE FROM dbo.vw_Analise_Planejado_vs_Executado_v2(?, ?, ?)"
    params = (f'{ANO_FILTRO}-01-01', f'{ANO_FILTRO}-12-31', PPA_FILTRO)
    try:
        df_unidades = pd.read_sql(query_unidades, engine_db, params=params)
        unidades = sorted(df_unidades['UNIDADE'].str.upper().str.replace('SP - ', '', regex=False).str.strip().unique())
        logger.info(f"{len(unidades)} unidades encontradas.")
        return unidades
    except Exception as e:
        logger.exception(f"Falha ao consultar as unidades: {e}")
        return []

def selecionar_unidades_interativamente(unidades_disponiveis: List[str]) -> List[str]:
    if not unidades_disponiveis: return []
    print("\n--- Unidades Disponíveis ---")
    for i, unidade in enumerate(unidades_disponiveis, 1):
        print(f"  {i:2d}) {unidade}")
    print("  all) Todas")
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

def criar_grafico_barras_trimestral(df: pd.DataFrame, titulo: str) -> go.Figure:
    if df.empty: return go.Figure().update_layout(title=f'{titulo} (Sem Dados)')
    
    df_agrupado = df.groupby('nm_trimestre', observed=False)[['Valor_Planejado', 'Valor_Executado']].sum().reset_index()
    
    fig = go.Figure()
    fig.add_trace(go.Bar(
        x=df_agrupado['nm_trimestre'],
        y=df_agrupado['Valor_Planejado'],
        name='Planejado',
        marker_color='#aed6f1'
    ))
    fig.add_trace(go.Bar(
        x=df_agrupado['nm_trimestre'],
        y=df_agrupado['Valor_Executado'],
        name='Executado',
        marker_color='#3498db'
    ))
    fig.update_layout(
        barmode='group',
        title_text=titulo,
        xaxis_title='Trimestre',
        yaxis_title='Valor (R$)',
        legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1)
    )
    return fig

def criar_grafico_dispersao_natureza(df: pd.DataFrame, titulo: str) -> go.Figure:
    if df.empty: return go.Figure().update_layout(title=f'{titulo} (Sem Dados)')

    df_natureza = df.groupby('natureza_simplificada').agg(
        Valor_Planejado=('Valor_Planejado', 'sum'),
        Valor_Executado=('Valor_Executado', 'sum')
    ).reset_index()

    df_natureza = df_natureza[df_natureza['Valor_Planejado'] > 0]
    df_natureza['perc_execucao'] = (df_natureza['Valor_Executado'] / df_natureza['Valor_Planejado']) * 100
    df_natureza['tamanho_bolha'] = np.sqrt(df_natureza['Valor_Planejado']) / np.sqrt(df_natureza['Valor_Planejado'].max()) * 50

    fig = go.Figure()
    fig.add_trace(go.Scatter(
        x=df_natureza['Valor_Planejado'],
        y=df_natureza['Valor_Executado'],
        mode='markers+text',
        text=df_natureza['natureza_simplificada'],
        textposition="top center",
        marker=dict(
            size=df_natureza['tamanho_bolha'],
            color=df_natureza['perc_execucao'],
            colorscale='RdYlGn', # Vermelho-Amarelo-Verde
            cmin=50, cmax=110, # Define o range da escala de cor
            colorbar=dict(title="Execução (%)"),
            showscale=True
        ),
        hovertemplate='<b>%{text}</b><br>Planejado: R$ %{x:,.2f}<br>Executado: R$ %{y:,.2f}<br>Execução: %{marker.color:.1f}%<extra></extra>'
    ))

    max_val = max(df_natureza['Valor_Planejado'].max(), df_natureza['Valor_Executado'].max())
    fig.add_shape(type='line', x0=0, y0=0, x1=max_val, y1=max_val,
                  line=dict(color='grey', width=2, dash='dash'), name='Meta 100%')

    fig.update_layout(
        title_text=titulo,
        xaxis_title='Valor Planejado (R$)',
        yaxis_title='Valor Executado (R$)',
        height=600
    )
    return fig


def gerar_relatorio_para_unidade(unidade_alvo: str, df_base: pd.DataFrame, mapa_naturezas: Dict[str, str]) -> None:
    logger.info(f"Iniciando geração de relatório para a unidade: {unidade_alvo}")

    df_unidade_filtrada = df_base[df_base['nm_unidade_padronizada'] == unidade_alvo].copy()
    if df_unidade_filtrada.empty: 
        logger.warning(f"Nenhum dado encontrado para a unidade '{unidade_alvo}'. Pulando.")
        return

    # Aplica o mapa de nomes simplificados
    df_unidade_filtrada['natureza_simplificada'] = df_unidade_filtrada['Descricao_Natureza_Orcamentaria'].map(mapa_naturezas).fillna(df_unidade_filtrada['Descricao_Natureza_Orcamentaria'])
    
    df_exclusivos = df_unidade_filtrada[df_unidade_filtrada['tipo_projeto'] == 'Exclusivo'].copy()
    df_compartilhados = df_unidade_filtrada[df_unidade_filtrada['tipo_projeto'] == 'Compartilhado'].copy()

    # Cálculos de KPIs
    total_planejado_unidade = df_unidade_filtrada['Valor_Planejado'].sum()
    total_executado_unidade = df_unidade_filtrada['Valor_Executado'].sum()
    perc_total = (total_executado_unidade / total_planejado_unidade * 100) if total_planejado_unidade > 0 else 0
    kpi_total_planejado_str = formatar_numero_kpi(total_planejado_unidade)
    kpi_total_executado_str = formatar_numero_kpi(total_executado_unidade)
    
    planejado_exclusivos = df_exclusivos['Valor_Planejado'].sum()
    executado_exclusivos = df_exclusivos['Valor_Executado'].sum()
    perc_exclusivos = (executado_exclusivos / planejado_exclusivos * 100) if planejado_exclusivos > 0 else 0
    kpi_exclusivos_planejado_str = formatar_numero_kpi(planejado_exclusivos)
    kpi_exclusivos_executado_str = formatar_numero_kpi(executado_exclusivos)

    planejado_compartilhados = df_compartilhados['Valor_Planejado'].sum()
    executado_compartilhados = df_compartilhados['Valor_Executado'].sum()
    perc_compartilhados = (executado_compartilhados / planejado_compartilhados * 100) if planejado_compartilhados > 0 else 0
    kpi_compartilhados_planejado_str = formatar_numero_kpi(planejado_compartilhados)
    kpi_compartilhados_executado_str = formatar_numero_kpi(executado_compartilhados)
    
    logger.info(f"Gerando gráficos para {unidade_alvo}...")
    fig_barras_exclusivos = criar_grafico_barras_trimestral(df_exclusivos, "Projetos Exclusivos")
    fig_barras_compartilhados = criar_grafico_barras_trimestral(df_compartilhados, "Projetos Compartilhados")
    fig_dispersao_natureza = criar_grafico_dispersao_natureza(df_unidade_filtrada, f"Performance por Natureza Orçamentária - {unidade_alvo}")

    logger.info(f"Montando e salvando o arquivo HTML para {unidade_alvo}...")
    try:
        template_path = CONFIG.paths.templates_dir / "template_relatorio.html"
        template_string = template_path.read_text(encoding='utf-8')
    except FileNotFoundError:
        logger.error(f"Arquivo de template não encontrado em '{template_path}'.")
        return

    contexto = {
        "unidade_alvo": unidade_alvo,
        "perc_total": perc_total,
        "kpi_total_planejado_str": kpi_total_planejado_str,
        "kpi_total_executado_str": kpi_total_executado_str,
        "perc_exclusivos": perc_exclusivos,
        "kpi_exclusivos_planejado_str": kpi_exclusivos_planejado_str,
        "kpi_exclusivos_executado_str": kpi_exclusivos_executado_str,
        "perc_compartilhados": perc_compartilhados,
        "kpi_compartilhados_planejado_str": kpi_compartilhados_planejado_str,
        "kpi_compartilhados_executado_str": kpi_compartilhados_executado_str,
        "html_barras_exclusivos": pio.to_html(fig_barras_exclusivos, full_html=False, include_plotlyjs=False),
        "html_barras_compartilhados": pio.to_html(fig_barras_compartilhados, full_html=False, include_plotlyjs=False),
        "html_dispersao_natureza": pio.to_html(fig_dispersao_natureza, full_html=False, include_plotlyjs=False),
    }

    html_final = template_string.format(**contexto)
    caminho_arquivo_html = CONFIG.paths.docs_dir / f"relatorio_{unidade_alvo.replace(' ', '_')}.html"
    caminho_arquivo_html.parent.mkdir(parents=True, exist_ok=True)
    with open(caminho_arquivo_html, 'w', encoding='utf-8') as f:
        f.write(html_final)
    logger.info(f"Relatório salvo com sucesso em: '{caminho_arquivo_html}'")

def main() -> None:
    parser = argparse.ArgumentParser(description="Gera relatórios de performance orçamentária.")
    parser.add_argument("--unidade", type=str, help="Gera o relatório para uma unidade específica.")
    parser.add_argument("--todas-unidades", action="store_true", help="Gera relatórios para todas as unidades.")
    args = parser.parse_args()

    CONFIG.paths.docs_dir.mkdir(parents=True, exist_ok=True)
    CONFIG.paths.logs_dir.mkdir(parents=True, exist_ok=True)
    CONFIG.paths.templates_dir.mkdir(parents=True, exist_ok=True)
    CONFIG.paths.static_dir.mkdir(parents=True, exist_ok=True)
    
    logger.info("Estabelecendo conexão com o banco de dados...")
    try:
        engine_db = get_conexao(CONFIG.conexoes["FINANCA_SQL"])
        logger.info("Conexão estabelecida com sucesso.")
    except Exception as e:
        logger.critical(f"Falha crítica ao conectar ao banco de dados: {e}")
        sys.exit(1)

    unidades_a_gerar = []
    if args.unidade:
        unidades_a_gerar = [args.unidade.upper().strip()]
    elif args.todas_unidades:
        unidades_a_gerar = obter_unidades_disponiveis(engine_db)
    else:
        unidades_disponiveis = obter_unidades_disponiveis(engine_db)
        if unidades_disponiveis:
            unidades_a_gerar = selecionar_unidades_interativamente(unidades_disponiveis)

    if not unidades_a_gerar:
        logger.info("Nenhuma unidade selecionada. Encerrando.")
    else:
        logger.info(f"Gerando relatórios para: {', '.join(unidades_a_gerar)}")
        
        PPA_FILTRO = os.getenv("PPA_FILTRO", 'PPA 2025 - 2025/DEZ')
        ANO_FILTRO = int(os.getenv("ANO_FILTRO", 2025))
        sql_query = "SELECT * FROM dbo.vw_Analise_Planejado_vs_Executado_v2(?, ?, ?)"
        params = (f'{ANO_FILTRO}-01-01', f'{ANO_FILTRO}-12-31', PPA_FILTRO)
        
        logger.info("Carregando dados base...")
        df_base_total = pd.read_sql(sql_query, engine_db, params=params)
        
        df_base_total['nm_unidade_padronizada'] = df_base_total['UNIDADE'].str.upper().str.replace('SP - ', '', regex=False).str.strip()
        unidades_por_projeto = df_base_total.groupby('PROJETO')['nm_unidade_padronizada'].nunique().reset_index()
        unidades_por_projeto.rename(columns={'nm_unidade_padronizada': 'contagem_unidades'}, inplace=True)
        unidades_por_projeto['tipo_projeto'] = np.where(unidades_por_projeto['contagem_unidades'] > 1, 'Compartilhado', 'Exclusivo')
        df_base_total = pd.merge(df_base_total, unidades_por_projeto[['PROJETO', 'tipo_projeto']], on='PROJETO', how='left')
        
        mapa_trimestre_num = {1: '1T', 2: '1T', 3: '1T', 4: '2T', 5: '2T', 6: '2T', 7: '3T', 8: '3T', 9: '3T', 10: '4T', 11: '4T', 12: '4T'}
        df_base_total['nm_trimestre'] = pd.to_numeric(df_base_total['MES'], errors='coerce').map(mapa_trimestre_num)
        trimestre_dtype = pd.CategoricalDtype(categories=['1T', '2T', '3T', '4T'], ordered=True)
        df_base_total['nm_trimestre'] = df_base_total['nm_trimestre'].astype(trimestre_dtype)
        
        mapa_naturezas = carregar_mapa_naturezas()

        for unidade in unidades_a_gerar:
            gerar_relatorio_para_unidade(unidade, df_base=df_base_total, mapa_naturezas=mapa_naturezas)

    logger.info("\n--- FIM DO SCRIPT DE GERAÇÃO DE RELATÓRIOS ---")

if __name__ == "__main__":
    main()
