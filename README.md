# Robô de Análise Orçamentária e Geração de Dashboards

Este projeto consiste em um conjunto de ferramentas e pipelines de dados em Python, projetado para automatizar a análise de performance orçamentária. O sistema extrai dados de múltiplas fontes, enriquece-os, aplica correções, e gera tanto bases de dados analíticas quanto dashboards interativos em HTML para as unidades de negócio.

## ✨ Funcionalidades Principais

*   **Extração e Cache de Dados:** Busca dados de planejamento (OLAP) e estrutura (SQL Server) e utiliza um cache local (SQLite) para acelerar execuções futuras.
*   **Enriquecimento de Dados:** Enriquece os dados orçamentários com os códigos de centro de custo correspondentes.
*   **Limpeza de Dados Interativa:** Inclui um modo interativo para corrigir falhas de cruzamento de dados, salvando as correções para uso futuro.
*   **Geração de Dashboards Interativos:** Cria relatórios HTML dinâmicos por unidade de negócio usando Plotly e Chart.js, com métricas de performance, gráficos de tendência e análises detalhadas.
*   **Automação de Comunicação:** Envia os relatórios e bases analíticas por e-mail para os gestores via Outlook, incluindo um screenshot do dashboard.

## 🏛️ Arquitetura do Projeto

O projeto é organizado em uma arquitetura modular para garantir alta coesão, baixo acoplamento e facilidade de manutenção.

.
├── config/ # Módulos de configuração centralizada
│ ├── config.py # Classe principal de configuração (caminhos, conexões)
│ ├── inicializacao.py # Carregamento de drivers externos (.dll)
│ └── logger_config.py # Configuração do logger
│
├── comunicacao/ # Módulos para entrada e saída de dados
│ ├── carregamento.py # Carrega DataFrames para o SQL Server
│ └── enviar_relatorios.py# Gera e envia e-mails com os relatórios
│
├── processamento/ # Lógica de transformação e regras de negócio
│ ├── correcao_chaves.py # Módulo de correção interativa de dados
│ ├── enriquecimento.py # Lógica de junção (merge) dos dados
│ ├── extracao.py # Extração de dados das fontes (SQL, OLAP) com cache
│ └── validacao.py # Preparação e validação das chaves de junção
│
├── visualizacao/ # Módulos para a camada de apresentação
│ ├── componentes_plotly.py # Funções que criam gráficos Plotly
│ └── preparadores_dados.py # Prepara os dados para os gráficos (Chart.js, etc.)
│
├── templates/ # Templates HTML
│ └── dashboard_template.html # Template base para os dashboards
│
├── dados/ # Arquivos de mapeamento e dados auxiliares (CSVs)
├── docs/ # Onde os relatórios HTML e Excel são salvos
├── queries/ # Scripts SQL
└── cache/ # Arquivos de cache (gerados automaticamente)
│
├── main.py # Ponto de entrada: Pipeline de enriquecimento de dados
├── gerar_relatorio.py # Ponto de entrada: Geração dos dashboards HTML
├── enviar_relatorios.py # Ponto de entrada: Envio dos e-mails
├── requirements.txt # Dependências do projeto
└── .env.example # Arquivo de exemplo para variáveis de ambiente


## ⚙️ Configuração do Ambiente

**Pré-requisitos:**
*   Python 3.9+
*   Acesso aos bancos de dados de origem.
*   **Ambiente Windows** (para a funcionalidade de envio de e-mail via Outlook).
*   Microsoft Outlook (para o envio de e-mails).
*   Gateway de dados On-premises da Microsoft (para acesso OLAP).

**Passos para Instalação:**

1.  **Clonar o Repositório:**
    ```bash
    git clone <url-do-repositorio>
    cd <nome-do-repositorio>
    ```

2.  **Criar e Ativar Ambiente Virtual:**
    ```bash
    # Windows
    python -m venv .venv
    .\.venv\Scripts\activate
    ```

3.  **Instalar Dependências:**
    ```bash
    pip install -r requirements.txt
    ```

4.  **Configurar Variáveis de Ambiente:**
    *   Crie uma cópia do arquivo `.env.example` e renomeie para `.env`.
    *   Preencha as variáveis com as informações dos seus servidores de banco de dados e caminhos necessários. Este arquivo é sensível e **não deve** ser versionado no Git.

    **Exemplo de `.env`:**
    ```dotenv
    # Conexões de Banco de Dados
    DB_SERVER_FINANCA="seu-servidor-financa"
    DB_DATABASE_FINANCA="FINANCA"
    DB_SERVER_HUB="seu-servidor-hub"
    DB_DATABASE_HUB="HubDados"

    # Filtros para Queries
    PPA_FILTRO="PPA 2025 - 2025/DEZ"
    ANO_FILTRO="2025"

    # Caminho para DLL do Analysis Services (se necessário)
    ADOMD_DLL_PATH="Caminho/Completo/Para/Microsoft.AnalysisServices.AdomdClient.dll"
    
    # URL para os dashboards publicados (Github Pages, etc.)
    GITHUB_PAGES_URL="https://seu-usuario.github.io/seu-repositorio/"
    ```

## 🚀 Uso do Projeto

O projeto possui três pontos de entrada principais, cada um para uma finalidade específica.

#### 1. Enriquecer a Base de Dados

Este pipeline executa o processo de ETL: extrai dados brutos, aplica correções e salva a tabela `ORCADO_ENRIQUECIDO_COM_CC` no banco de dados.

```bash
python main.py

Para corrigir chaves de junção que não foram encontradas automaticamente, execute em modo interativo:
```bash
python main.py --modo-interativo

2. Gerar os Dashboards
Este script utiliza os dados processados para gerar os relatórios HTML interativos na pasta docs/.

```bash
# Execução interativa para escolher as unidades
python gerar_relatorio.py

# Gerar relatório para uma unidade específica
```bash
python gerar_relatorio.py --unidade "NOME DA UNIDADE"

# Gerar para todas as unidades de uma vez
```bash
python gerar_relatorio.py --todas

3. Enviar Relatórios por E-mail
Este script (exclusivo para Windows com Outlook) prepara e exibe os e-mails para envio, com o dashboard em anexo e um preview no corpo do e-mail.

# Execução interativa para escolher para quais unidades enviar
```bash
python enviar_relatorios.py

# Preparar e-mails para todas as unidades de uma vez
```bash
python enviar_relatorios.py --enviar-todos

🧑‍💻 Guia de Manutenção e Contribuição
Qualidade dos Dados: Para corrigir permanentemente um cruzamento de dados (ex: uma UNIDADE com nome incorreto), adicione a correção no arquivo dados/mapa_correcoes.json ou use o modo interativo do main.py.

Novos Gráficos:

Crie uma nova função em visualizacao/preparadores_dados.py para formatar os dados.

Adicione um placeholder no templates/dashboard_template.html.

Chame a nova função em gerar_relatorio.py e injete os dados no template.

Dependências: Para adicionar uma nova biblioteca, adicione-a ao requirements.txt e atualize o ambiente virtual.
