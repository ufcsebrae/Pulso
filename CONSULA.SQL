
DECLARE @DATAINICIO AS DATE = '2025-01-01';
DECLARE @DATAFIM AS DATE = '2025-12-31';
DECLARE @PPA AS VARCHAR(MAX) = 'PPA 2025 - 2025/DEZ';
DECLARE @DESCNVL1 TABLE (Descnvl1 VARCHAR (MAX));
INSERT INTO @DESCNVL1 (Descnvl1) VALUES
('DESPESAS'), ('DESPESAS EXCLUSIVAS DO ORÇAMENTO');

WITH EXECUTADO AS (
    -- 1. Agrupa tudo o que foi executado no ano de 2025
    SELECT 
        YEAR(DATA) AS ANO,
        MONTH(DATA) AS MES,
        PROJETO,
        ACAO,
        UNIDADE,
        CC AS CODCCUSTO,
        cdgContaNvl4 AS CODIGO_NATUREZA_ORCAMENTARIA,
  DESCNVL4 AS Descricao_Natureza_Orcamentaria,
        SUM(VALOR) AS VALOR_EXECUTADO
    FROM FATOFECHAMENTO_V2 A
	INNER JOIN @DESCNVL1 B ON A.DESCNVL1 = B.Descnvl1
    WHERE CC IS NOT NULL
      AND (DATA BETWEEN @DATAINICIO AND @DATAFIM)
    GROUP BY YEAR(DATA), MONTH(DATA), PROJETO, ACAO, UNIDADE, CC, cdgContaNvl4,DESCNVL4
),
PLANEJADO AS (
    -- 2. Seleciona os dados do planejamento de 2025 usando as colunas que você confirmou
    SELECT
        ANO,
        MES,
        PROJETO,
        ACAO,
        UNIDADE,
        CODCCUSTO,
        Valor_Ajustado,
        Descricao_PPA,
        Codigo_Natureza_Orcamentaria,
        Descricao_Natureza_Orcamentaria,
        DTUNIDADE,
        DTPROJETO,
        DTACAO
    FROM [FINANCA].[dbo].[ORCADO_ENRIQUECIDO_COM_CC]
    WHERE descricao_ppa IN (@PPA)
),


PC AS (SELECT CODCONTA,DESCRICAO
  FROM [HubDados].[CorporeRM].[CCONTA] WHERE LEN(CODCONTA)= 7
)

-- 3. Junta as duas fontes (PLANEJADO e EXECUTADO) e formata a saída final
SELECT 
    COALESCE(A.ANO, B.ANO) AS ANO,
    COALESCE(A.MES, B.MES) AS MES,
    COALESCE(A.PROJETO, B.PROJETO) AS PROJETO,
    COALESCE(A.ACAO, B.ACAO) AS ACAO,
    COALESCE(A.UNIDADE, B.UNIDADE) AS UNIDADE,
    COALESCE(A.CODCCUSTO, B.CODCCUSTO) AS CODCCUSTO,
    A.Valor_Ajustado,
    A.Descricao_PPA,
    COALESCE(A.Codigo_Natureza_Orcamentaria, B.CODIGO_NATUREZA_ORCAMENTARIA) AS Codigo_Natureza_Orcamentaria,
    COALESCE(A.Descricao_Natureza_Orcamentaria,B.Descricao_Natureza_Orcamentaria) AS Descricao_Natureza_Orcamentaria,
	C.DESCRICAO AS NM_NVL4,
    B.VALOR_EXECUTADO AS VALOR -- Coluna com o valor executado

FROM PLANEJADO A 
FULL OUTER JOIN EXECUTADO B ON A.ANO = B.ANO 
                            AND A.MES = B.MES 
                            AND A.CODCCUSTO = B.CODCCUSTO 
                            AND A.CODIGO_NATUREZA_ORCAMENTARIA = B.CODIGO_NATUREZA_ORCAMENTARIA
LEFT JOIN PC C ON  COALESCE(A.Codigo_Natureza_Orcamentaria, B.CODIGO_NATUREZA_ORCAMENTARIA) = C.CODCONTA COLLATE SQL_Latin1_General_CP1_CI_AI
ORDER BY 
    ANO, 
    MES;
 