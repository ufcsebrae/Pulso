# processamento/correcao_chaves.py
import logging
from typing import Dict, Set, Optional
import pandas as pd

# Importa a função de salvar para uso incremental
from .validacao import salvar_mapa_correcoes, carregar_mapa_correcoes

logger = logging.getLogger(__name__)

def _encontrar_melhor_sugestao_por_ano(
    projeto: str, acao: str, unidade: str, df_referencia: pd.DataFrame
) -> Optional[str]:
    """
    Busca uma correspondência de Projeto/Ação/Unidade e retorna a chave com o
    ano mais recente disponível.

    A lógica agora é flexível e ignora diferenças de prefixo "SP - " na UNIDADE.
    """
    projeto_lower = projeto.lower()
    acao_lower = acao.lower()
    unidade_lower = unidade.lower()

    ref_unidade_lower = df_referencia['UNIDADE'].str.lower()

    # *** LÓGICA DE COMPARAÇÃO FLEXÍVEL PARA A UNIDADE ***
    mask_unidade = (
        (ref_unidade_lower == unidade_lower) |                      # Correspondência exata
        (ref_unidade_lower == 'sp - ' + unidade_lower) |       # Referência tem o prefixo
        ('sp - ' + ref_unidade_lower == unidade_lower)        # Orçado tem o prefixo
    )

    mask_final = (
        (df_referencia['PROJETO'].str.lower() == projeto_lower) &
        (df_referencia['ACAO'].str.lower() == acao_lower) &
        mask_unidade
    )
    
    candidatos = df_referencia[mask_final]

    if not candidatos.empty:
        # Encontra a linha com o ano mais recente entre os candidatos
        sugestao_ideal = candidatos.loc[candidatos['ANO'].idxmax()]
        return sugestao_ideal['CHAVE_CONCAT']
    
    return None

def iniciar_correcao_interativa_chaves(
    chaves_com_falha: Set[str],
    df_referencia: pd.DataFrame
):
    """
    Inicia um fluxo de correção interativo que salva cada decisão
    imediatamente no mapa de correções.
    """
    logger.info("Iniciando correção interativa para %d chaves...", len(chaves_com_falha))
    
    mapa_atual = carregar_mapa_correcoes()
    chaves_a_validar = sorted(list(chaves_com_falha))

    for i, chave_incorreta in enumerate(chaves_a_validar, 1):
        if chave_incorreta in mapa_atual:
            continue

        print("\n" + "="*100)
        print(f"[CORREÇÃO {i}/{len(chaves_a_validar)}]")
        print(f"  > Chave não encontrada: {chave_incorreta}")

        try:
            partes = chave_incorreta.split('|')
            projeto, acao, unidade, _ = partes
        except (IndexError, ValueError):
            logger.error("Formato inválido para a chave '%s'. Pulando para busca manual.", chave_incorreta)
            _executar_busca_manual(chave_incorreta, df_referencia, mapa_atual)
            continue

        # 1. Tenta encontrar a sugestão inteligente
        melhor_sugestao = _encontrar_melhor_sugestao_por_ano(projeto, acao, unidade, df_referencia)

        # 2. Apresenta a sugestão para confirmação rápida
        if melhor_sugestao and melhor_sugestao != chave_incorreta:
            print(f"  > SUGESTÃO: Chave correspondente encontrada com potencial ajuste de prefixo/ano.")
            print(f"    DE: {chave_incorreta}")
            print(f"  PARA: {melhor_sugestao}")
            
            resposta = input("  > Aceitar (s), buscar manualmente (p) ou ignorar (enter)? [s/p/enter]: ").lower().strip()

            if resposta == 's':
                mapa_atual[chave_incorreta] = melhor_sugestao
                salvar_mapa_correcoes(mapa_atual)
                logger.info("Correção salva. Continuando...")
                continue
            elif resposta == 'p':
                _executar_busca_manual(chave_incorreta, df_referencia, mapa_atual)
                continue
            else:
                logger.warning("Chave '%s' ignorada nesta sessão.", chave_incorreta)
                continue
        
        # 3. Fallback para a busca manual se não houver sugestão
        print("  > Nenhuma sugestão automática encontrada.")
        _executar_busca_manual(chave_incorreta, df_referencia, mapa_atual)

    logger.info("Processo de correção interativa concluído.")


def _executar_busca_manual(chave_incorreta: str, df_referencia: pd.DataFrame, mapa_atual: Dict):
    """Função auxiliar para o fluxo de busca manual que salva incrementalmente."""
    while True:
        termo_pesquisa = input("  > Pesquise por um termo (ou enter para ignorar): ").strip()
        if not termo_pesquisa:
            logger.warning("Busca manual para '%s' ignorada.", chave_incorreta)
            break

        mask = (
            df_referencia['PROJETO'].str.contains(termo_pesquisa, case=False, na=False) |
            df_referencia['ACAO'].str.contains(termo_pesquisa, case=False, na=False)
        )
        resultados = df_referencia[mask]['CHAVE_CONCAT'].unique().tolist()

        if not resultados:
            print(f"  > Nenhum resultado encontrado para '{termo_pesquisa}'. Tente novamente.")
            continue

        print(f"\n--- Opções encontradas para '{termo_pesquisa}' ---")
        resultados_ordenados = sorted(resultados)
        for idx, chave_resultado in enumerate(resultados_ordenados, 1):
            print(f"    {idx}) {chave_resultado}")
        
        try:
            num_escolha_str = input(f"  > Escolha o número (1-{len(resultados)}) ou 0 para nova busca: ")
            num_escolha = int(num_escolha_str)
            
            if 1 <= num_escolha <= len(resultados):
                escolha_final = resultados_ordenados[num_escolha - 1]
                mapa_atual[chave_incorreta] = escolha_final
                salvar_mapa_correcoes(mapa_atual)
                logger.info("Correção manual salva. Continuando...")
                break
            elif num_escolha == 0:
                continue
            else:
                print("  Número fora do intervalo.")
        except (ValueError, IndexError):
            print("  Entrada inválida.")
