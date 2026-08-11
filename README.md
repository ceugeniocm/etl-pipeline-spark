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

### Execução Local

```bash
spark-submit \
    --master "local[*]" \
    --driver-memory 4g \
    --driver-java-options "-Dlog4j.configurationFile=file:log4j2.properties" \
    --packages com.mysql:mysql-connector-j:8.3.0 \
    etl_spark.py config_bigdata.json
```
Esse comando executa uma aplicação Apache Spark em Python (`etl_spark.py`) no seu ambiente local, configurando memória, dependências de banco de dados e parâmetros adicionais.

Aqui está o detalhamento de cada parte:

1. `spark-submit`

É o utilitário do Apache Spark usado para **submeter e iniciar scripts ou aplicações** no cluster ou máquina local.

2. `--master "local[*]"`

Define onde e como a aplicação será executada:

* **`local`**: Executa no modo local (na própria máquina), sem precisar de um cluster Spark separado.
* **`[*]`**: Utiliza **todos os núcleos de CPU disponíveis** na sua máquina para processamento paralelo.

3. `--driver-memory 4g`

Aloca **4 Gigabytes de memória RAM** especificamente para o processo *Driver* do Spark (o processo principal que gerencia o script, constrói os planos de execução e coordena os dados).

4. `--driver-java-options "-Dlog4j.configurationFile=file:log4j2.properties"`

Carrega o arquivo `log4j2.properties` do projeto na inicialização da JVM, suprimindo as mensagens **INFO** de startup do Spark (SparkContext, BlockManager, Executor, etc.), que são emitidas antes de o `log_level` do `config_bigdata.json` ser aplicado. Assim, apenas mensagens `WARN` e `ERROR` aparecem no console.

5. `--packages com.mysql:mysql-connector-j:8.3.0`

Instrui o Spark a baixar e incluir automaticamente uma biblioteca/dependência do repositório Maven central antes de rodar o código:

* **`com.mysql:mysql-connector-j:8.3.0`**: É o driver JDBC oficial do MySQL (versão 8.3.0). Ele permite que o script Spark se conecte e leia/escreva em bancos de dados MySQL.

6. `etl_spark.py`

É o arquivo do seu **script em Python** (PySpark) que contém a lógica de ETL (Extração, Transformação e Carga) que você quer executar.

7. `config_bigdata.json`

É um **argumento fornecido ao script Python**. O `etl_spark.py` vai ler esse arquivo JSON no início da execução (provavelmente para carregar configurações como credenciais do banco, caminhos de arquivos, tabelas, etc.).

Ao final da execução, o pipeline imprime um **resumo de execução** em pt_BR com o total de linhas lidas, carregadas, rejeitadas, duplicatas descartadas e o tempo total decorrido.

### Execução em Cluster YARN (Produção)

```bash
spark-submit \
    --master yarn \
    --deploy-mode cluster \
    --num-executors 4 \
    --executor-memory 8g \
    --executor-cores 4 \
    --driver-memory 4g \
    --packages com.mysql:mysql-connector-j:8.3.0 \
    etl_spark.py config_bigdata.json
```

### Docker

```bash
docker-compose up --build
```

## Configuração

O pipeline é configurado via arquivo JSON (`config_bigdata.json`):

```json
{
  "source": {
    "path": "hdfs:///dados/agendamentos.parquet",
    "format": "parquet",
    "partition_column": "data"
  },
  "mapping": "mapping.json",
  "validation": {
    "required": ["id_agendamento", "benef_id", "data"],
    "ranges": { "valor": { "minimum": 0 } },
    "max_lengths": { "paciente_nome": 255 },
    "business_key": ["id_agendamento", "benef_id", "data_hora"],
    "on_duplicate": "discard"
  },
  "database": {
    "jdbc_url": "jdbc:mysql://host:3306/database",
    "user": "etl_user",
    "password": "${ETL_DB_PASSWORD}",
    "driver": "com.mysql.cj.jdbc.Driver"
  },
  "load": {
    "table": "tb_agendamentos",
    "mode": "overwrite",
    "output_format": "jdbc"
  },
  "spark": {
    "app_name": "ETL Agendamentos - Big Data",
    "shuffle_partitions": 200,
    "executor_memory": "8g"
  }
}
```

### Formatos de Entrada Suportados

| Formato | Velocidade | Compressão | Recomendação |
|---------|-----------|------------|--------------|
| Excel (.xlsx) | Lenta | Nenhuma | Apenas para migração inicial |
| CSV | Média | Nenhuma | Boa para interoperabilidade |
| **Parquet** | **Rápida** | **Snappy/Gzip** | **Ideal para Big Data** |

### Formatos de Saída Suportados

| Formato | `output_format` | Recomendação |
|---------|----------------|--------------|
| MySQL (JDBC) | `jdbc` | Quando a aplicação já consome MySQL |
| Parquet | `parquet` | Data lake simples, análises batch |
| Delta Lake | `delta` | Data lake com controle transacional |

## Testes

```bash
python -m pytest tests/ -v
```

Ou com unittest:

```bash
python -m unittest discover tests/ -v
```

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

Consulte `docs/bigdata-trabalho.md` no projeto [etl-pipeline](https://github.com/ceugeniocm/etl-pipeline) para a documentação completa da arquitetura e decisões de design.
