# Relatório de Tratamento de Dados

Este relatório descreve os procedimentos de limpeza, padronização e validação aplicados aos dados durante o processo de ETL (Extraction, Transformation, Loading) utilizando Apache Spark.

## 1. Identificação de Valores Nulos
Foi implementada uma verificação de campos obrigatórios conforme definido no Modelo Entidade-Relacionamento (MER). 
- **Ação:** Registros que possuem valores nulos em colunas essenciais (ex: chaves primárias ou chaves estrangeiras como `ag_id`, `prof_id`, `benef_id`) são filtrados e movidos para um relatório de rejeições.
- **Implementação:** Utilização da função `validate()` que verifica a nulidade e gera o motivo da rejeição.

## 2. Remoção de Registros Duplicados
Para garantir a integridade referencial e evitar redundância no banco de dados, foi aplicada a deduplicação.
- **Ação:** Identificação de registros duplicados com base em uma "chave de negócio" (Business Key), composta por campos que tornam o registro único no mundo real (ex: `ag_id`, `benef_id`, `prof_id`, `dthoraagenda`).
- **Implementação:** Uso da função `dropDuplicates()` do PySpark.

## 3. Padronização e Limpeza de Texto
Os dados de entrada podem conter inconsistências de formatação (espaços extras, variações de caixa, pontuação indesejada).
- **Ação:**
    - Remoção de espaços em branco no início e fim das strings (`trim`).
    - Conversão de nomes e descrições para letras maiúsculas (`upper`).
    - Remoção de pontuação em campos como CPF e CNPJ (quando aplicável).
    - Colapso de múltiplos espaços internos para apenas um.
- **Implementação:** Funções de transformação de strings do Spark SQL (`F.trim`, `F.upper`, `F.regexp_replace`).

## 4. Validação e Coerção de Tipos de Dados
Garante que os dados lidos do arquivo Excel/CSV correspondam aos tipos esperados pelo banco de dados relacional.
- **Ação:**
    - Conversão de strings para números inteiros (`BIGINT`) e decimais (`DECIMAL(10,2)`).
    - Conversão de strings de data/hora para o tipo `DATETIME` ou `DATE`, tratando diferentes formatos de entrada (ex: `dd/MM/yyyy HH:mm:ss`, `yyyy-MM-dd`).
- **Implementação:** Uso de `try_cast` e `to_timestamp`/`to_date` com suporte a múltiplos formatos via `coalesce`.

## 5. Correção de Inconsistências e Validação de Intervalos
Verificação se os valores numéricos estão dentro de faixas aceitáveis.
- **Ação:** Validação de campos de valor (ex: `agp_valor`) para garantir que não sejam negativos.
- **Implementação:** Filtros de intervalo (`range_condition`) aplicados durante a fase de validação.

## 6. Relatório de Rejeições
Todos os registros que falham em qualquer uma das validações acima não são inseridos no banco de dados.
- **Ação:** Gravação de um arquivo CSV em `output/rejeicoes/` contendo a linha original e o motivo específico da rejeição, permitindo a auditoria e correção posterior na origem.
