# ETL Pipeline Spark

Aplicação desenvolvida em Python capaz de realizar a leitura, o
tratamento e a inserção de dados em um banco de dados hospedado em nuvem, utilizando
técnicas de programação aplicadas a Big Data.

## Arquitetura

```
Excel/CSV/Parquet → spark.read → Mapeamento → Limpeza → Coerção de Tipos → Validação → Deduplicação → MySQL / Parquet / Delta Lake
```

### Fluxo do Pipeline

| Etapa | Descrição | API Spark |
|-------|-----------|-----------|
| **Extração** | Leitura de Excel, CSV ou Parquet | `spark.read.parquet()`, `spark.read.csv()`, pandas (Excel) |
| **Mapeamento** | Renomeação de colunas origem → destino | `df.select()`, `F.col().alias()` |
| **Limpeza** | Trim, upper, remoção de pontuação, colapso de espaços | `F.trim()`, `F.upper()`, `F.regexp_replace()` |
| **Coerção de Tipos** | Conversão para int, decimal, datetime, date | `df.cast()`, `F.to_date()`, `F.to_timestamp()` |
| **Validação** | Campos obrigatórios, faixas, comprimentos máximos | `df.filter()`, `F.col().isNotNull()` |
| **Deduplicação** | Remoção de duplicatas por chave de negócio | `df.dropDuplicates()` |
| **Carga** | Gravação no MySQL, Parquet ou Delta Lake | `df.write.jdbc()`, `df.write.parquet()` |

## Requisitos

- Python 3.8+
- Java 11 ou 17 (JDK)
- 4 GB de RAM disponível

## Instalação

```bash
pip install -r requirements.txt
```

### Conectores (JARs via spark-submit)

| Conector | Pacote Maven | Finalidade |
|----------|-------------|-----------|
| MySQL JDBC | `com.mysql:mysql-connector-j:8.3.0` | Conexão com MySQL |
| Delta Lake | `io.delta:delta-spark_2.12:3.1.0` | Gravação em formato Delta |

## Uso

Para instruções detalhadas de configuração, execução e **testes**, consulte o [Guia de Uso Detalhado](docs/usage.md).

## Entregáveis do Trabalho

Esta aplicação atende a todos os requisitos do trabalho de "Processamento de Dados com Python e Big Data":

1.  **Código-fonte**: `etl_spark.py` e arquivos de configuração.
2.  **Script SQL**: `create_tables.sql` (Criação das tabelas conforme MER).
3.  **Documentação de Execução**: Este `README.md`.
4.  **Banco de Dados Povoado**: Conexão configurada em `config_bigdata.json` para banco na nuvem.
5.  **Relatório de Tratamentos**: Localizado em `docs/relatorio-tratamento.md`.

## Estrutura do Projeto

```
etl-pipeline-spark/
├── etl_spark.py           # Pipeline PySpark principal
├── config_bigdata.json    # Configuração do pipeline
├── mapping.json           # Mapeamento de colunas e tipos
├── create_tables.sql      # DDL das tabelas MySQL
├── requirements.txt       # Dependências Python
├── Dockerfile             # Imagem Docker com Spark
├── docker-compose.yml     # MySQL + Spark para desenvolvimento
├── README.md              # Este arquivo
├── docs/                  # Documentação detalhada
│   ├── usage.md           # Guia de uso e configuração
│   ├── relatorio-tratamento.md
│   ├── diagrama-entidade-relacionamento.md
│   └── bigdata-trabalho.md
└── tests/
    └── test_etl_spark.py  # Testes unitários e de integração
```

## Performance Estimada

| Volume | PySpark (local) | PySpark (cluster 4 nós) |
|--------|----------------|------------------------|
| 10K linhas | ~30s, ~1 GB RAM | Não recomendado |
| 100K linhas | ~1 min, ~2 GB RAM | ~30s, ~2 GB RAM/nó |
| 1M linhas | ~5 min, ~4 GB RAM | ~2 min, ~2 GB RAM/nó |
| 10M+ linhas | ~30 min, ~8 GB RAM | ~8 min, ~4 GB RAM/nó |

## Documentação

Consulte o [Guia de Uso Detalhado](docs/usage.md) para informações sobre configuração e execução avançada.
Para a documentação completa da arquitetura e decisões de design, veja `docs/`
