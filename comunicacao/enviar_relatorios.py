# comunicacao/enviar_relatorios.py (VERSÃO COMPLETA E FINAL)
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
import argparse

try:
    from processamento.processamento_dados_base import obter_dados_processados
    from config.config import CONFIG
except ImportError:
    logging.basicConfig(level=logging.INFO)
    logging.critical("Erro: Arquivos essenciais de 'processamento' ou 'config' não foram encontrados.")
    sys.exit(1)

logger = logging.getLogger(__name__)

def carregar_gerentes_do_csv() -> dict:
    caminho_csv = CONFIG.paths.gerentes_csv
    if not caminho_csv.exists():
        logger.error(f"Arquivo de gerentes não encontrado: {caminho_csv}")
        return {}
    try:
        df_gerentes = pd.read_csv(caminho_csv, engine='python', encoding='utf-8-sig').fillna('')
        df_gerentes.columns = df_gerentes.columns.str.strip()
        gerentes_dict = { str(row['unidade']).upper().strip(): { 'nome_novo': str(row['nome_novo']).strip(), 'gerente': str(row['gerente']).strip(), 'email': str(row['email']).strip(), 'tratamento': str(row['tratamento']).strip(), 'equipe_cc': str(row['equipe']).strip() } for _, row in df_gerentes.iterrows() }
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
    options.add_argument("--headless"); options.add_argument("--window-size=1280,1024"); options.add_argument("--hide-scrollbars")
    try:
        caminho_driver = CONFIG.paths.drivers / "chromedriver.exe"
        if not caminho_driver.is_file():
            logger.error(f"ERRO CRÍTICO: O driver do Chrome não foi encontrado em: {caminho_driver}"); return None
        service = Service(executable_path=str(caminho_driver))
        driver = webdriver.Chrome(service=service, options=options)
        driver.get(f"file:///{html_path.resolve()}"); time.sleep(4)
        driver.save_screenshot(str(screenshot_path)); driver.quit()
        logger.info(f"Screenshot salvo com sucesso em: '{screenshot_path}'")
        return screenshot_path
    except Exception as e:
        logger.error(f"Falha ao capturar screenshot para '{html_path.name}': {e}", exc_info=True); return None

def enviar_via_outlook(destinatario: str, cc: str, assunto: str, corpo_html: str, anexos: list[Path] | None = None):
    try:
        outlook = win32.Dispatch('outlook.application')
        mail = outlook.CreateItem(0); mail.To = destinatario
        if cc: mail.CC = cc
        mail.Subject = assunto
        if anexos:
            for anexo_path in anexos:
                if anexo_path and anexo_path.exists() and anexo_path.suffix.lower() == '.png':
                    attachment = mail.Attachments.Add(str(anexo_path.resolve()))
                    cid = "dashboard_preview"
                    attachment.PropertyAccessor.SetProperty("http://schemas.microsoft.com/mapi/proptag/0x3712001F", cid)
                    corpo_html = corpo_html.replace('cid:screenshot_placeholder', f'cid:{cid}')
        mail.HTMLBody = corpo_html
        if anexos:
            for anexo_path in anexos:
                if anexo_path and anexo_path.exists() and anexo_path.suffix.lower() != '.png':
                    mail.Attachments.Add(str(anexo_path.resolve()))
        mail.Display()
        logger.info(f"E-mail para {destinatario} (CC: {cc or 'Nenhum'}) criado para revisão.")
        return True
    except Exception as e:
        logger.exception(f"Falha ao criar e-mail no Outlook para {destinatario}."); return False

def preparar_e_enviar_email_por_unidade(unidade_antiga_nome: str, gerentes_info: dict):
    info_gerente = gerentes_info[unidade_antiga_nome.upper()]
    unidade_nova_nome = info_gerente['nome_novo']
    
    logger.info(f"\n--- Preparando envio para a unidade: {unidade_nova_nome} (Dados de: {unidade_antiga_nome}) ---")
    
    nome_arquivo_sanitizado = unidade_nova_nome.replace(' ', '_').replace('/', '_')
    nome_arquivo_html = f"dashboard_{nome_arquivo_sanitizado}.html"
    html_path = CONFIG.paths.docs_dir / nome_arquivo_html
    if not html_path.exists():
        logger.warning(f"Relatório HTML '{nome_arquivo_html}' não encontrado. O e-mail para esta unidade não será enviado.")
        return

    base_url = os.getenv('GITHUB_PAGES_URL')
    if not base_url or not base_url.strip():
        error_msg = "A variável de ambiente 'GITHUB_PAGES_URL' não está definida ou está vazia no arquivo .env. O processo não pode continuar."
        logger.critical(error_msg)
        raise ValueError(error_msg)
    dashboard_url = f"{base_url.rstrip('/')}/{nome_arquivo_html}"
    
    anexos_para_enviar = []
    
    excel_filename = f"dados_analiticos_{nome_arquivo_sanitizado}.xlsx"
    excel_path = CONFIG.paths.relatorios_excel_dir / excel_filename
    if excel_path.exists():
        anexos_para_enviar.append(excel_path)
        logger.info(f"Anexo de dados analíticos encontrado: {excel_path.name}")
    else:
        logger.warning(f"Anexo de dados analíticos NÃO encontrado: {excel_path.name}")

    if unidade_nova_nome.upper() == "ATENDIMENTO AO CLIENTE":
        logger.info(f"Verificando anexos de correlação para a unidade '{unidade_nova_nome}'.")
        path_fato_v2 = CONFIG.paths.relatorios_excel_dir / f"correlacao_fatofechamento_v2_{nome_arquivo_sanitizado}.xlsx"
        if path_fato_v2.exists(): anexos_para_enviar.append(path_fato_v2); logger.info(f"Anexo de correlação encontrado: {path_fato_v2.name}")
        else: logger.warning(f"Anexo de correlação NÃO encontrado: {path_fato_v2.name}")
        path_comprometido = CONFIG.paths.relatorios_excel_dir / f"correlacao_comprometido_{nome_arquivo_sanitizado}.xlsx"
        if path_comprometido.exists(): anexos_para_enviar.append(path_comprometido); logger.info(f"Anexo de correlação encontrado: {path_comprometido.name}")
        else: logger.warning(f"Anexo de correlação NÃO encontrado: {path_comprometido.name}")

    screenshot_path = capturar_screenshot_relatorio(html_path)
    if screenshot_path: anexos_para_enviar.append(screenshot_path)
        
    screenshot_html_block = f'''
        <div style="margin-top: 25px; padding-top: 25px; border-top: 1px solid #e2e8f0;">
            <p style="margin: 0 0 15px 0; font-size: 14px; color: #475569; font-weight: 500;">Prévia do Painel Interativo:</p>
            <a href="{dashboard_url}" target="_blank" style="text-decoration: none;"><img src="cid:screenshot_placeholder" alt="Prévia do Dashboard" style="width:100%; max-width:800px; border: 1px solid #e2e8f0; border-radius: 8px;"></a>
        </div>''' if screenshot_path else ""

    assunto = f"📊 [pulso] - [fechamento 2025]: relatório despesas - {unidade_nova_nome}"
    tratamento = info_gerente.get('tratamento', 'Prezado(a)'); nome_gerente = info_gerente.get('gerente', 'Gestor(a)')

    corpo_email = f"""
    <!DOCTYPE html><html lang="pt-BR"><head><meta charset="UTF-8"></head><body style="margin: 0; padding: 0; width: 100%; background-color: #f8fafc; font-family: Calibri, 'Segoe UI', Tahoma, Geneva, Verdana, sans-serif;"><table width="100%" border="0" cellpadding="0" cellspacing="0" role="presentation" style="background-color: #f8fafc;"><tr><td align="center" style="padding: 40px 20px;"><table width="100%" border="0" cellpadding="0" cellspacing="0" role="presentation" style="max-width: 680px; background-color: #ffffff; border: 1px solid #e2e8f0; border-radius: 12px; box-shadow: 0 4px 6px -1px rgba(0,0,0,0.1), 0 2px 4px -2px rgba(0,0,0,0.1);"><tr><td style="padding: 32px;"><p style="margin: 0 0 24px 0; font-size: 18px; font-weight: 600; color: #0f172a;">{tratamento} {nome_gerente} e equipe,</p><p style="margin: 0 0 16px 0; font-size: 16px; color: #334155; line-height: 1.75;">Com a conclusão do fechamento orçamentário de 2025, <b>disponibilizamos os dados finais da execução orçamentária de 2025 da sua unidade</b></p><p style="margin: 0 0 24px 0; font-size: 16px; color: #334155; line-height: 1.75;">Nosso objetivo é democratizar o acesso à informação para apoiar sua gestão. O acompanhamento está disponível em duas frentes:</p><div style="background-color: #f8fafc; border: 1px solid #e2e8f0; border-radius: 8px; padding: 20px; margin-bottom: 16px;"><p style="margin: 0 0 8px 0; font-size: 16px; font-weight: 600; color: #1e293b;">1. Painel Interativo (Dashboard)</p><p style="margin: 0; font-size: 15px; color: #475569; line-height: 1.7;">Visão tática para análise rápida de tendências e desvios.</p></div><div style="background-color: #f8fafc; border: 1px solid #e2e8f0; border-radius: 8px; padding: 20px; margin-bottom: 24px;"><p style="margin: 0 0 8px 0; font-size: 16px; font-weight: 600; color: #1e293b;">2. Base Analítica:</p><p style="margin: 0; font-size: 15px; color: #475569; line-height: 1.7;">Arquivo em Excel (anexo) com o detalhamento completo para conferência e filtros personalizados.</p></div><table width="100%" border="0" cellpadding="0" cellspacing="0" role="presentation"><tr><td align="center" style="padding: 12px 0;"><a href="{dashboard_url}" target="_blank" style="background-color: #2563eb; color: #ffffff; padding: 15px 30px; text-decoration: none; border-radius: 8px; font-weight: bold; font-size: 16px; display: inline-block;">👉 Acessar Painel Interativo</a></td></tr></table>{screenshot_html_block}<div style=" color: #334155; line-height: 1.6; max-width: 800px;"><p style="margin-bottom: 16px; font-size: 16px;">O Dashboard interativo possui algumas nomenclaturas que estão detalhadas abaixo:</p><ul style="list-style: none; padding: 0; font-size: 15px;"><li style="margin-bottom: 8px;"><strong>Projetos Exclusivos:</strong> São os projetos onde as ações são todas da sua unidade.</li><li style="margin-bottom: 8px;"><strong>Projetos Compartilhados:</strong> São os projetos onde diversas unidades possuem orçamento (ex: segurança, limpeza, folha, etc).</li><li style="margin-bottom: 8px;"><strong>Orçamento Ocioso:</strong> É o saldo remanescente do Planejado no LEME subtraído do valor gasto.</li><li style="margin-bottom: 8px;"><strong>Execução Sem Planejamento:</strong> Natureza não planejada no LEME, mas que possui execução.</li><li style="margin-bottom: 8px;"><strong>Visão Hierárquica (Sunburst ou Rosca multinível):</strong> O círculo interno são os projetos e o externo as naturezas. Cores indicam % de execução (verde é melhor) e o tamanho representa o valor em R$.</li><li style="margin-bottom: 8px;"><strong>Mapa de Performance (Heatmap ou Mapa de calor):</strong> Explica qual natureza específica impacta na % de execução frente ao planejado.</li><li style="margin-bottom: 16px;"><strong>Inércia:</strong> Tempo que cada natureza demorou para ter o primeiro gasto, indicando gargalos operacionais.</li></ul><p style="margin-top: 20px; font-size: 15px; border-top: 1px solid #e2e8f0; padding-top: 10px;">Este ecossistema de dados foi desenhado para que a informação circule, servindo de suporte estratégico. Seguimos à disposição para apoio técnico.</p></div><p style="margin: 40px 0 0 0; font-size: 16px; color: #475569;">Atenciosamente,<br><b style="color: #1e293b;">Equipe Contabilidade/Orçamento</b></p></td></tr></table></td></tr></table></body></html>
    """
    try:
        enviar_via_outlook(destinatario=info_gerente['email'], cc=info_gerente['equipe_cc'], assunto=assunto, corpo_html=corpo_email, anexos=anexos_para_enviar)
    finally:
        if screenshot_path and screenshot_path.exists(): os.remove(screenshot_path); logger.info(f"Screenshot temporário '{screenshot_path.name}' removido.")


def main():
    parser = argparse.ArgumentParser(description="Envia relatórios de performance orçamentária por e-mail.")
    parser.add_argument("--enviar-todos", action="store_true", help="Envia e-mails para todas as unidades elegíveis sem interação manual.")
    args = parser.parse_args()

    df_base_total = obter_dados_processados()
    if df_base_total is None or df_base_total.empty: logger.error("Base de dados não pôde ser carregada. Encerrando."); sys.exit(1)
        
    gerentes_info = carregar_gerentes_do_csv()
    if not gerentes_info: logger.error("Arquivo de gerentes não pôde ser carregado. Encerrando."); sys.exit(1)
        
    CONFIG.paths.relatorios_excel_dir.mkdir(parents=True, exist_ok=True)
    unidades_antigas_disponiveis = df_base_total['UNIDADE_FINAL'].unique()
    unidades_map = { unidade_antiga.upper(): gerentes_info[unidade_antiga.upper()] for unidade_antiga in unidades_antigas_disponiveis if unidade_antiga.upper() in gerentes_info }

    if not unidades_map: logger.warning("Nenhuma unidade na base de dados corresponde a um gerente no arquivo. Encerrando."); sys.exit(0)

    lista_exibicao = sorted([(v['nome_novo'], k) for k, v in unidades_map.items()])
    
    unidades_a_processar = []
    if args.enviar_todos:
        unidades_a_processar = [item[1] for item in lista_exibicao]
    else:
        print("\n--- Unidades Disponíveis para Envio de Relatório ---")
        for i, (nome_novo, _) in enumerate(lista_exibicao, 1): print(f"  {i:2d}) {nome_novo}")
        print("  all) Enviar para todas as unidades"); print("-" * 65)
        escolha_str = input("Escolha os relatórios para enviar (por número, ex: 1, 3), 'all' ou enter para sair: ").strip()
        if not escolha_str: logger.info("Nenhuma seleção feita. Encerrando."); sys.exit(0)
        if escolha_str.lower() == 'all':
            unidades_a_processar = [item[1] for item in lista_exibicao]
        else:
            try:
                indices = [int(num.strip()) - 1 for num in escolha_str.split(',')]
                unidades_a_processar = [lista_exibicao[idx][1] for idx in indices if 0 <= idx < len(lista_exibicao)]
            except (ValueError, IndexError): logger.error("Entrada inválida. Encerrando."); sys.exit(1)

    if unidades_a_processar:
        logger.info(f"Iniciando processo de envio para: {', '.join([unidades_map[k.upper()]['nome_novo'] for k in unidades_a_processar])}")
        for unidade_antiga in unidades_a_processar:
            preparar_e_enviar_email_por_unidade(unidade_antiga, gerentes_info)
    else:
        logger.info("Nenhuma unidade válida selecionada para envio.")

    logger.info("\n--- FIM DO PROCESSO DE ENVIO DE E-MAILS ---")

if __name__ == "__main__":
    main()
