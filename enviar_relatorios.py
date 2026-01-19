# enviar_relatorios.py
import logging
import sys
import os
import pandas as pd
from pathlib import Path
import win32com.client as win32
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from webdriver_manager.chrome import ChromeDriverManager
from selenium.webdriver.chrome.options import Options as ChromeOptions
import time

# --- Módulo de Dados Centralizado ---
try:
    from processamento_dados_base import obter_dados_processados
    from config import CONFIG
except ImportError:
    logging.basicConfig(level=logging.INFO)
    logging.critical("Erro: Arquivos essenciais como 'processamento_dados_base.py' ou 'config.py' não foram encontrados.")
    sys.exit(1)

logger = logging.getLogger(__name__)

def carregar_gerentes_do_csv() -> dict:
    """Carrega os dados de gerentes do arquivo CSV e os transforma em um dicionário."""
    caminho_csv = CONFIG.paths.gerentes_csv
    if not caminho_csv.exists():
        logger.error(f"Arquivo de gerentes não encontrado: {caminho_csv}")
        return {}
    try:
        df_gerentes = pd.read_csv(caminho_csv, sep=',', encoding='utf-8-sig')
        gerentes_dict = {
            str(row['unidade']).upper().strip(): {'email': str(row['email']), 'gerente': str(row['gerente'])}
            for _, row in df_gerentes.iterrows()
        }
        logger.info(f"{len(gerentes_dict)} gerentes carregados de '{caminho_csv.name}'.")
        return gerentes_dict
    except Exception as e:
        logger.exception(f"Falha ao processar o arquivo CSV de gerentes: {e}")
        return {}

def capturar_screenshot_relatorio(html_path: Path) -> Path | None:
    """Tira um screenshot de um arquivo HTML local."""
    if not html_path.exists():
        logger.error(f"Arquivo HTML para screenshot não encontrado: {html_path}")
        return None
        
    logger.info(f"Capturando screenshot de '{html_path.name}'...")
    screenshot_path = CONFIG.paths.docs_dir / f"temp_screenshot_{html_path.stem}.png"
    
    options = ChromeOptions()
    options.add_argument("--headless")
    options.add_argument("--window-size=1200,800")
    options.add_argument("--hide-scrollbars")
    os.environ['WDM_SSL_VERIFY'] = '0'
    os.environ['WDM_LOCAL'] = '1'

    try:
        service = Service(ChromeDriverManager().install())
        driver = webdriver.Chrome(service=service, options=options)
        driver.get(f"file:///{html_path.resolve()}")
        time.sleep(3)
        driver.save_screenshot(str(screenshot_path))
        driver.quit()
        
        logger.info(f"Screenshot salvo em: '{screenshot_path}'")
        return screenshot_path
    except Exception as e:
        logger.error(f"Falha ao capturar screenshot para '{html_path.name}': {e}")
        return None

def enviar_via_outlook(destinatario: str, assunto: str, corpo_html: str, anexo_screenshot: Path | None):
    """Cria e exibe um e-mail no Outlook, pronto para ser enviado."""
    try:
        outlook = win32.Dispatch('outlook.application')
        mail = outlook.CreateItem(0)
        mail.To = destinatario
        mail.Subject = assunto
        
        if anexo_screenshot and anexo_screenshot.exists():
            attachment = mail.Attachments.Add(str(anexo_screenshot.resolve()))
            cid = "screenshot_cid"
            attachment.PropertyAccessor.SetProperty("http://schemas.microsoft.com/mapi/proptag/0x3712001F", cid)
            corpo_html = corpo_html.replace('cid:screenshot_placeholder', f'cid:{cid}')
        
        mail.HTMLBody = corpo_html
        mail.Display()
        
        logger.info(f"E-mail para {destinatario} criado e exibido para revisão.")
        return True
    except Exception as e:
        logger.exception(f"Falha ao criar e-mail no Outlook para {destinatario}.")
        return False

def preparar_e_enviar_email_por_unidade(unidade_nome: str, df_base_total: pd.DataFrame, gerentes: dict):
    """Filtra os dados, calcula KPIs, formata e envia o e-mail para uma unidade."""
    
    logger.info(f"\n--- Preparando envio para a unidade: {unidade_nome} ---")
    
    info_gerente = gerentes.get(unidade_nome.upper())
    if not info_gerente:
        logger.warning(f"Nenhum gerente encontrado para '{unidade_nome}' no arquivo de gerentes. Pulando.")
        return

    nome_arquivo = f"dashboard_{unidade_nome.replace(' ', '_').replace('/', '_')}.html"
    html_path = CONFIG.paths.docs_dir / nome_arquivo
    if not html_path.exists():
        logger.warning(f"Relatório '{nome_arquivo}' não encontrado para a unidade '{unidade_nome}'. Pulando.")
        return

    GITHUB_PAGES_BASE_URL = os.getenv("GITHUB_PAGES_URL", "https://<seu-usuario>.github.io/<seu-repositorio>/")
    dashboard_url = f"{GITHUB_PAGES_BASE_URL}{html_path.name}"

    df_unidade = df_base_total[df_base_total['UNIDADE_FINAL'] == unidade_nome].copy()
    if df_unidade.empty:
        logger.warning(f"Sem dados para a unidade '{unidade_nome}' após o filtro. Pulando.")
        return

    unidade_planejado = df_unidade['Valor_Planejado'].sum()
    unidade_executado = df_unidade['Valor_Executado'].sum()
    unidade_saldo = unidade_planejado - unidade_executado
    gap_global_perc = (unidade_saldo / unidade_planejado * 100) if unidade_planejado > 0 else 0

    df_serv_esp = df_unidade[df_unidade['NATUREZA_FINAL'] == 'Serviços Especializados']
    saldo_serv_esp = df_serv_esp['Valor_Planejado'].sum() - df_serv_esp['Valor_Executado'].sum()
    perc_ponto_atencao = (saldo_serv_esp / unidade_saldo * 100) if unidade_saldo > 0 else 0

    logger.info(f"[{unidade_nome}] Saldo Final Não Executado: {gap_global_perc:.1f}%. Foco em Serviços Especializados: {perc_ponto_atencao:.1f}%.")

    screenshot_path = capturar_screenshot_relatorio(html_path)

    assunto = f"Fechamento Orçamentário 2025: Análise de Performance da Unidade {unidade_nome}"
    
    corpo_email = f"""
    <html>
    <body style="font-family: 'Roboto', sans-serif; color: #1F2937; line-height: 1.6;">
        <p>Prezado(a) {info_gerente['gerente']},</p>
        <p>Apresentamos a <strong>análise final da execução orçamentária de 2025</strong> para a sua unidade. O objetivo é consolidar os resultados e extrair insights para o planejamento do próximo exercício.</p>
        
        <div style="background-color: #FFFBEB; border-left: 4px solid #F59E0B; padding: 15px; margin: 20px 0; border-radius: 4px;">
            <h4 style="margin: 0 0 10px 0; font-weight: bold; font-size: 1.1em;">Diagnóstico Final: {unidade_nome}</h4>
            <ul style="padding-left: 20px; margin: 0;">
                <li><strong>Resultado Final:</strong> A unidade encerrou o ano com um saldo não executado de <strong>{gap_global_perc:.1f}%</strong> (R$ {unidade_saldo:,.2f}). Este valor representa um capital que não foi empregado nas iniciativas planejadas.</li>
                <li><strong>Principal Ponto de Atenção 2025:</strong> <strong>{perc_ponto_atencao:.1f}%</strong> do saldo ocioso está concentrado em <strong>"Serviços Especializados"</strong>, apontando para um gargalo recorrente em processos de contratação.</li>
            </ul>
        </div>

        <h4>Recomendações para o Planejamento de 2026:</h4>
        <ol style="padding-left: 20px;">
            <li><strong>Analisar Causas Raiz:</strong> Utilize o dashboard interativo para investigar os projetos e ações com os maiores desvios entre o planejado e o executado.</li>
            <li><strong>Revisar Planejamento Futuro:</strong> Os saldos significativos devem ser questionados no novo ciclo. Ações não iniciadas podem indicar desalinhamento entre o plano e a capacidade operacional.</li>
            <li><strong>Discutir Resultados:</strong> Estamos à disposição para agendar uma reunião, analisar os dados em conjunto e traçar um plano de ação para otimizar a execução no próximo ano.</li>
        </ol>

        <p>Para uma análise completa, acesse o relatório detalhado clicando no botão abaixo:</p>
        
        <!-- BOTÃO DE CALL TO ACTION -->
        <table width="100%" cellspacing="0" cellpadding="0" style="margin: 20px 0;">
          <tr>
            <td>
              <table cellspacing="0" cellpadding="0">
                <tr>
                  <td style="border-radius: 5px;" bgcolor="#4F46E5">
                    <a href="{dashboard_url}" target="_blank" style="padding: 12px 25px; border: 1px solid #4F46E5; border-radius: 5px; font-family: 'Roboto', sans-serif; font-size: 16px; color: #ffffff; text-decoration: none; display: inline-block; font-weight: bold;">
                      Acessar Análise Detalhada
                    </a>
                  </td>
                </tr>
              </table>
            </td>
          </tr>
        </table>

        <p style="margin-top: 20px; font-size: 0.9em; color: #555;"><i>Você também pode clicar na prévia abaixo para abrir o relatório.</i></p>
        <a href="{dashboard_url}">
            <img src="cid:screenshot_placeholder" alt="Prévia do Dashboard" style="width:100%; max-width:800px; border:1px solid #ccc; border-radius: 4px;">
        </a>
        <p>Atenciosamente,<br><strong>Equipe de Planejamento e Controladoria</strong></p>
    </body>
    </html>
    """

    enviar_via_outlook(
        destinatario=info_gerente['email'],
        assunto=assunto,
        corpo_html=corpo_email,
        anexo_screenshot=screenshot_path
    )
    
    if screenshot_path and screenshot_path.exists():
        os.remove(screenshot_path)
        logger.info(f"Screenshot temporário '{screenshot_path.name}' removido.")

def main():
    """Ponto de entrada do script de envio de relatórios."""
    
    logger.info("Carregando base de dados e arquivo de gerentes...")
    df_base_total = obter_dados_processados()
    gerentes = carregar_gerentes_do_csv()

    if df_base_total is None or df_base_total.empty:
        logger.error("A base de dados não pôde ser carregada. Encerrando.")
        sys.exit(1)
    if not gerentes:
        logger.error("O arquivo de gerentes não pôde ser carregado ou está vazio. Encerrando.")
        sys.exit(1)

    relatorios_dir = CONFIG.paths.docs_dir
    unidades_com_relatorio = [f.stem.replace('dashboard_', '').replace('_', ' ') for f in relatorios_dir.glob("dashboard_*.html")]
    
    unidades_validas_para_envio = sorted([
        unidade for unidade in unidades_com_relatorio if unidade.upper() in gerentes
    ])

    if not unidades_validas_para_envio:
        logger.warning(f"Nenhum relatório HTML na pasta '{relatorios_dir}' corresponde a um gerente no arquivo 'gerentes.csv'.")
        sys.exit(0)

    print("\n--- Relatórios com Gerente Correspondente Disponíveis para Envio ---")
    for i, nome_unidade in enumerate(unidades_validas_para_envio, 1):
        print(f"  {i:2d}) {nome_unidade}")
    print("  all) Enviar todos os relatórios")
    print("-" * 65)

    escolha_str = input("Escolha os relatórios para enviar (por número, ex: 1, 3), 'all' ou enter para sair: ").strip()

    if not escolha_str:
        logger.info("Nenhuma seleção feita. Encerrando.")
        sys.exit(0)

    unidades_para_enviar = []
    if escolha_str.lower() == 'all':
        unidades_para_enviar = unidades_validas_para_envio
    else:
        try:
            indices_escolhidos = [int(num.strip()) - 1 for num in escolha_str.split(',')]
            for idx in indices_escolhidos:
                if 0 <= idx < len(unidades_validas_para_envio):
                    unidades_para_enviar.append(unidades_validas_para_envio[idx])
                else:
                    logger.warning(f"Número {idx + 1} é inválido e será ignorado.")
        except ValueError:
            logger.error("Entrada inválida. Encerrando.")
            sys.exit(1)

    if not unidades_para_enviar:
        logger.info("Nenhum relatório válido selecionado. Encerrando.")
    else:
        for unidade in unidades_para_enviar:
            preparar_e_enviar_email_por_unidade(unidade, df_base_total, gerentes)

    logger.info("\n--- FIM DO PROCESSO DE ENVIO DE E-MAILS ---")

if __name__ == "__main__":
    main()
