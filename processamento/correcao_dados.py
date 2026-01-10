# processamento/correcao_dados.py
import json
import logging
from pathlib import Path
from typing import Dict
import pandas as pd
# A linha 'from thefuzz import process' foi REMOVIDA

logger = logging.getLogger(__name__)

def carregar_mapa_correcao(caminho_mapa: str) -> Dict[str, dict]:
    """Carrega o mapa de correções do arquivo JSON."""
    if not Path(caminho_mapa).exists():
        return {}
    with open(caminho_mapa, 'r', encoding='utf-8') as f:
        return json.load(f)

def salvar_mapa_correcao(mapa: Dict[str, dict], caminho_mapa: str):
    """Salva o mapa de correções no arquivo JSON."""
    with open(caminho_mapa, 'w', encoding='utf-8') as f:
        json.dump(mapa, f, indent=4, ensure_ascii=False, sort_keys=True)
    logger.info("Mapa de correções salvo com sucesso em '%s'.", caminho_mapa)

def criar_chave_de_linha(row: pd.Series) -> str:
    """Cria uma string única para uma linha, combinando PROJETO, ACAO e UNIDADE."""
    return f"{row['PROJETO']}|{row['ACAO']}|{row['UNIDADE']}"

def iniciar_correcao_interativa(df_falhas: pd.DataFrame, df_referencia_cc: pd.DataFrame) -> Dict[str, dict]:
    """
    Inicia um fluxo de correção 100% manual, sem sugestões automáticas.
    """
    chaves_unicas_falha = df_falhas.apply(criar_chave_de_linha, axis=1).unique()
    
    logger.info("Iniciando correção manual para %d chaves órfãs...", len(chaves_unicas_falha))
    
    novas_correcoes = {}
    for i, chave_falha in enumerate(sorted(chaves_unicas_falha), 1):
        print("\n" + "="*50)
        print(f"[CORREÇÃO MANUAL {i}/{len(chaves_unicas_falha)}]")
        print(f"  > Chave do Orçado (ÓRFÃ): {chave_falha}")

        # Entra diretamente no modo de busca manual
        while True:
            acao = input("  > Ação: [p]rocurar na referência, [i]gnorar por agora: ").lower().strip()
            
            escolha_final_obj = None

            if acao == 'p':
                termo = input("  > Digite o termo para procurar na Estrutura de CC: ").lower()
                if not termo:
                    print("  Termo de busca não pode ser vazio.")
                    continue
                
                # Procura o termo em qualquer uma das colunas chave
                mask = (
                    df_referencia_cc['PROJETO'].str.lower().str.contains(termo, na=False) |
                    df_referencia_cc['ACAO'].str.lower().str.contains(termo, na=False) |
                    df_referencia_cc['UNIDADE'].str.lower().str.contains(termo, na=False)
                )
                resultados_df = df_referencia_cc[mask].drop_duplicates(subset=['PROJETO', 'ACAO', 'UNIDADE'])

                if resultados_df.empty:
                    print(f"  Nenhum resultado encontrado para '{termo}'.")
                    continue

                print("\n--- Resultados Encontrados ---")
                # Exibe os resultados numerados para o usuário escolher
                resultados_para_escolha = []
                for idx, (_, row) in enumerate(resultados_df.iterrows(), 1):
                    chave_resultado = criar_chave_de_linha(row)
                    print(f"    {idx}) {chave_resultado} | CC: {row['CODCCUSTO']}")
                    resultados_para_escolha.append(row)
                
                try:
                    num_escolha = int(input("  > Escolha o número correto (ou 0 para nova busca): "))
                    if 1 <= num_escolha <= len(resultados_para_escolha):
                        escolha_final_obj = resultados_para_escolha[num_escolha - 1]
                    elif num_escolha == 0:
                        continue # Volta para o prompt de ação
                    else:
                        print("  Número inválido.")
                except (ValueError, IndexError):
                    print("  Entrada inválida.")
            
            elif acao == 'i':
                logger.warning("Chave '%s' ignorada pelo usuário nesta sessão.", chave_falha)
                break # Sai do loop de busca e vai para a próxima chave órfã

            else:
                print("  Ação inválida. Tente 'p' ou 'i'.")
                continue

            if escolha_final_obj is not None:
                # Cria o objeto de correção baseado na escolha do usuário
                correcao = {
                    'PROJETO': escolha_final_obj['PROJETO'],
                    'ACAO': escolha_final_obj['ACAO'],
                    'UNIDADE': escolha_final_obj['UNIDADE'],
                    'CODCCUSTO': escolha_final_obj['CODCCUSTO']
                }
                novas_correcoes[chave_falha] = correcao
                logger.info("Mapeamento manual definido: '%s' -> %s", chave_falha, correcao)
                break # Sai do loop de busca e vai para a próxima chave órfã

    return novas_correcoes
