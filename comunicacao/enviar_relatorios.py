# comunicacao/enviar_relatorios.py
import logging
import sys
import os
import pandas as pd
from pathlib import Path
import win32com.client as win32
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options as ChromeOptions
import time

try:
    from processamento.processamento_dados_base import obter_dados_processados
    from config.config import CONFIG
except ImportError:
    logging.basicConfig(level=logging.INFO)
    logging.critical("Erro: Arquivos essenciais não encontrados.")
    sys.exit(1)

logger = logging.getLogger(__name__)

# --- ALTERAÇÃO 1: Função de carregamento atualizada ---
def carregar_gerentes_do_csv() -> dict:
    """
    Carrega os dados de gerentes, incluindo os novos nomes, tratamento e equipe em cópia.
    A chave do dicionário é o nome antigo da unidade (para filtros).
    """
    caminho_csv = CONFIG.paths.gerentes_csv
    if not caminho_csv.exists():
        logger.error(f"Arquivo de gerentes não encontrado: {caminho_csv}")
        return {}
    try:
        df_gerentes = pd.read_csv(caminho_csv, sep=',', encoding='utf-8-sig').fillna('')
        gerentes_dict = {
            str(row['unidade']).upper().strip(): {
                'nome_novo': str(row['nome_novo']).strip(),
                'gerente': str(row['gerente']).strip(),
                'email': str(row['email']).strip(),
                'tratamento': str(row['tratamento']).strip(),
                'equipe_cc': str(row['equipe']).strip()
            }
            for _, row in df_gerentes.iterrows()
        }
        logger.info(f"{len(gerentes_dict)} gerentes carregados de '{caminho_csv.name}'.")
        return gerentes_dict
    except Exception as e:
        logger.exception(f"Falha ao processar o arquivo CSV de gerentes: {e}")
        return {}

def capturar_screenshot_relatorio(html_path: Path) -> Path | None:
    if not html_path.exists():
        logger.error(f"Arquivo HTML para screenshot não encontrado: {html_path}")
        return None
    screenshot_path = CONFIG.paths.docs_dir / f"temp_screenshot_{html_path.stem}.png"
    options = ChromeOptions()
    options.add_argument("--headless")
    options.add_argument("--window-size=1200,900")
    options.add_argument("--hide-scrollbars")
    try:
        caminho_driver = CONFIG.paths.drivers / "chromedriver.exe"
        if not caminho_driver.is_file():
            logger.error(f"ERRO: Driver do Chrome não encontrado em: {caminho_driver}")
            return None
        service = Service(executable_path=str(caminho_driver))
        driver = webdriver.Chrome(service=service, options=options)
        driver.get(f"file:///{html_path.resolve()}")
        time.sleep(3)
        driver.save_screenshot(str(screenshot_path))
        driver.quit()
        logger.info(f"Screenshot salvo em: '{screenshot_path}'")
        return screenshot_path
    except Exception as e:
        logger.error(f"Falha ao capturar screenshot para '{html_path.name}': {e}", exc_info=True)
        return None

# --- ALTERAÇÃO 2: Adiciona o campo 'cc' (cópia) ---
def enviar_via_outlook(destinatario: str, cc: str, assunto: str, corpo_html: str, anexos: list[Path] | None = None):
    try:
        outlook = win32.Dispatch('outlook.application')
        mail = outlook.CreateItem(0)
        mail.To = destinatario
        if cc:
            mail.CC = cc
        mail.Subject = assunto
        if anexos:
            for anexo_path in anexos:
                if anexo_path and anexo_path.exists():
                    attachment = mail.Attachments.Add(str(anexo_path.resolve()))
                    if anexo_path.suffix.lower() == '.png':
                        cid = "screenshot_cid"
                        attachment.PropertyAccessor.SetProperty("http://schemas.microsoft.com/mapi/proptag/0x3712001F", cid)
                        corpo_html = corpo_html.replace('cid:screenshot_placeholder', f'cid:{cid}')
        mail.HTMLBody = corpo_html
        mail.Display()
        logger.info(f"E-mail para {destinatario} (CC: {cc or 'Nenhum'}) criado para revisão.")
        return True
    except Exception as e:
        logger.exception(f"Falha ao criar e-mail no Outlook para {destinatario}.")
        return False

# --- ALTERAÇÃO 3: Função principal adaptada para usar os novos dados ---
def preparar_e_enviar_email_por_unidade(unidade_antiga_nome: str, gerentes_info: dict, df_base_total: pd.DataFrame):
    
    info_gerente = gerentes_info[unidade_antiga_nome.upper()]
    unidade_nova_nome = info_gerente['nome_novo']
    
    logger.info(f"\n--- Preparando envio para a unidade: {unidade_nova_nome} (Dados de: {unidade_antiga_nome}) ---")
    
    nome_arquivo_sanitizado = unidade_nova_nome.replace(' ', '_').replace('/', '_')
    nome_arquivo_html = f"dashboard_{nome_arquivo_sanitizado}.html"
    html_path = CONFIG.paths.docs_dir / nome_arquivo_html
    if not html_path.exists():
        logger.warning(f"Relatório '{nome_arquivo_html}' não encontrado. Verifique se 'gerar_relatorio.py' também foi atualizado para usar os novos nomes.")
        return

    dashboard_url = f"{os.getenv('GITHUB_PAGES_URL', '')}{html_path.name}"
    
    # Filtro de dados continua usando o nome antigo
    df_unidade = df_base_total[df_base_total['UNIDADE_FINAL'] == unidade_antiga_nome].copy()
    if df_unidade.empty:
        logger.warning(f"Sem dados para a unidade '{unidade_antiga_nome}'. Pulando.")
        return

    excel_filename = f"dados_analiticos_{nome_arquivo_sanitizado}.xlsx"
    excel_path = CONFIG.paths.relatorios_excel_dir / excel_filename 
    logger.info(f"Gerando arquivo Excel em '{excel_path}'...")
    try:
        df_para_excel = df_unidade.drop(columns=['Descricao_Natureza_Orcamentaria'], errors='ignore')
        df_para_excel.to_excel(excel_path, index=False, sheet_name="Dados_Detalhados")
        logger.info(f"Arquivo Excel '{excel_filename}' gerado.")
    except Exception as e:
        logger.error(f"Falha ao gerar arquivo Excel para '{unidade_nova_nome}': {e}")
        return

    unidade_planejado = df_unidade['Valor_Planejado'].sum()
    unidade_executado = df_unidade['Valor_Executado'].sum()
    
    screenshot_path = capturar_screenshot_relatorio(html_path)
    assunto = f"Análise de Performance Orçamentária: Diagnóstico da Unidade {unidade_nova_nome}"
    
    tratamento = info_gerente.get('tratamento', 'Prezado(a)')
    nome_gerente = info_gerente.get('gerente', 'Gestor(a)')
    
    corpo_email = f"""
    <html><body>
        <p>{tratamento} {nome_gerente},</p>
        <p>Segue a <strong>análise final da execução orçamentária</strong> para a sua unidade: <strong>{unidade_nova_nome}</strong>.</p>
        <p>Para sua conveniência, os dados detalhados estão anexados em formato Excel.</p>
        <p>Para uma análise visual completa, acesse o relatório interativo:</p>
        <a href="{dashboard_url}">Acessar Dashboard Interativo da Unidade</a>
        <br><br>
        <a href="{dashboard_url}"><img src="cid:screenshot_placeholder" alt="Prévia do Dashboard" style="width:100%; max-width:800px; border: 1px solid #ccc;"></a>
        <p>Atenciosamente,<br><strong>Equipe de Planejamento e Controladoria</strong></p>
    </body></html>
    """

    try:
        anexos_para_enviar = [excel_path]
        if screenshot_path:
            anexos_para_enviar.append(screenshot_path)
        
        enviar_via_outlook(
            destinatario=info_gerente['email'],
            cc=info_gerente['equipe_cc'],
            assunto=assunto,
            corpo_html=corpo_email,
            anexos=anexos_para_enviar
        )
    finally:
        if screenshot_path and screenshot_path.exists():
            os.remove(screenshot_path)
            logger.info(f"Screenshot temporário removido.")

def main():
    logger.info("Carregando base de dados e arquivo de gerentes...")
    df_base_total = obter_dados_processados()
    gerentes_info = carregar_gerentes_do_csv()
    if df_base_total is None or df_base_total.empty or not gerentes_info:
        logger.error("Base de dados ou arquivo de gerentes não pôde ser carregado. Encerrando.")
        sys.exit(1)
        
    CONFIG.paths.relatorios_excel_dir.mkdir(parents=True, exist_ok=True)

    # --- ALTERAÇÃO 4: A lógica de seleção agora mostra os nomes novos ---
    unidades_antigas_disponiveis = sorted(df_base_total['UNIDADE_FINAL'].unique())
    unidades_para_envio_map = {
        unidade_antiga.upper(): gerentes_info[unidade_antiga.upper()]
        for unidade_antiga in unidades_antigas_disponiveis
        if unidade_antiga.upper() in gerentes_info
    }

    if not unidades_para_envio_map:
        logger.warning("Nenhuma unidade na base de dados corresponde a um gerente no arquivo. Encerrando.")
        sys.exit(0)

    # Cria uma lista para exibição com os nomes novos
    lista_exibicao = sorted([(v['nome_novo'], k) for k, v in unidades_para_envio_map.items()])

    print("\n--- Unidades Disponíveis para Envio de Relatório ---")
    for i, (nome_novo, _) in enumerate(lista_exibicao, 1):
        print(f"  {i:2d}) {nome_novo}")
    print("  all) Enviar para todas as unidades")
    print("-" * 65)
    
    escolha_str = input("Escolha os relatórios para enviar (por número, ex: 1, 3), 'all' ou enter para sair: ").strip()

    if not escolha_str:
        logger.info("Nenhuma seleção feita. Encerrando.")
        sys.exit(0)
    
    unidades_a_processar = []
    if escolha_str.lower() == 'all':
        unidades_a_processar = [item[1] for item in lista_exibicao] # Pega o nome antigo (chave)
    else:
        try:
            indices = [int(num.strip()) - 1 for num in escolha_str.split(',')]
            unidades_a_processar = [lista_exibicao[idx][1] for idx in indices if 0 <= idx < len(lista_exibicao)]
        except (ValueError, IndexError):
            logger.error("Entrada inválida. Encerrando.")
            sys.exit(1)

    if unidades_a_processar:
        for unidade_antiga in unidades_a_processar:
            preparar_e_enviar_email_por_unidade(unidade_antiga, gerentes_info, df_base_total)

    logger.info("\n--- FIM DO PROCESSO DE ENVIO DE E-MAILS ---")

if __name__ == "__main__":
    main()
