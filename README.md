# ETL Pipeline Spark

Pipeline ETL Big Data com PySpark para processamento distribuído de agendamentos médicos.

Versão distribuída do [etl-pipeline](../etl-pipeline/), utilizando Apache Spark (PySpark) para processar volumes de dados significativamente maiores (1M+ registros).

## Arquitetura

```
Excel/CSV/Parquet → spark.read → Mapeamento → Limpeza → Coerção de Tipos → Validação → Deduplicação → MySQL / Parquet / Delta Lake
```

### Fluxo do Pipeline

| Etapa | Descrição | API Spark |
|-------|-----------|-----------|
| **Extração** | Leitura de Excel, CSV ou Parquet | `spark.read.parquet()`, `spark.read.csv()`, spark-excel |
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
| spark-excel | `com.crealytics:spark-excel_2.12:0.20.4` | Leitura de arquivos Excel |
| Delta Lake | `io.delta:delta-spark_2.12:3.1.0` | Gravação em formato Delta |

## Uso

### Execução Local (Desenvolvimento)

```bash
spark-submit \
    --master "local[*]" \
    --driver-memory 4g \
    --packages com.mysql:mysql-connector-j:8.3.0,com.crealytics:spark-excel_2.12:0.20.4 \
    etl_spark.py config_bigdata.json
```

### Execução em Cluster YARN (Produção)

```bash
spark-submit \
    --master yarn \
    --deploy-mode cluster \
    --num-executors 4 \
    --executor-memory 8g \
    --executor-cores 4 \
    --driver-memory 4g \
    --packages com.mysql:mysql-connector-j:8.3.0,com.crealytics:spark-excel_2.12:0.20.4 \
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

Consulte `docs/bigdata.md` no projeto [etl-pipeline](../etl-pipeline/) para a documentação completa da arquitetura e decisões de design.
