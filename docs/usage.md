# Guia de Uso Detalhado e Configuração

Este documento fornece detalhes técnicos sobre a execução, parametrização e suporte a formatos do pipeline ETL Spark.

## Detalhamento do Comando spark-submit

Ao executar localmente, o comando utilizado é:

```bash
spark-submit \
    --master "local[*]" \
    --driver-memory 4g \
    --driver-java-options "-Dlog4j.configurationFile=file:log4j2.properties" \
    --packages com.mysql:mysql-connector-j:8.3.0 \
    etl_spark.py config_bigdata.json
```

### Explicação dos Parâmetros

1. **`spark-submit`**: Utilitário para iniciar aplicações Spark.
2. **`--master "local[*]"`**: Executa no modo local usando todos os núcleos de CPU disponíveis.
3. **`--driver-memory 4g`**: Aloca 4GB de RAM para o processo Driver.
4. **`--driver-java-options`**: Configura o Log4j para suprimir mensagens INFO desnecessárias no startup.
5. **`--packages`**: Faz o download automático do driver JDBC do MySQL.
6. **`etl_spark.py`**: O script principal do pipeline.
7. **`config_bigdata.json`**: Arquivo de configuração passado como argumento.

## Execução em Cluster YARN (Produção)

Para ambientes de produção, recomenda-se o uso do YARN:

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

## Configuração do JSON (`config_bigdata.json`)

O pipeline é altamente configurável. Abaixo, um exemplo completo da estrutura:

```json
{
  "source": {
    "path": "./input/agendaAnonimizado.xlsx",
    "format": "excel",
    "sheet": "AGENDA_1"
  },
  "mapping": "mapping.json",
  "validation": {
    "required": ["ag_id", "prof_id", "esp_id", "benef_id", "tblproced_id", "plano_id", "cli_id", "sala_id", "tpa_id", "user_id", "dthoraagenda", "ag_statusagendamento"],
    "ranges": {
      "agp_valor": { "minimum": 0 }
    },
    "max_lengths": {
      "benef_nome": 255
    },
    "rejection_threshold": "5%",
    "business_key": ["ag_id", "benef_id", "prof_id", "dthoraagenda"],
    "on_duplicate": "discard"
  },
  "database": {
    "jdbc_url": "jdbc:mysql://savir005.vpshost12372.mysql.dbaas.com.br:3306/savir005",
    "user": "savir005",
    "password": "Carlos#2025",
    "driver": "com.mysql.cj.jdbc.Driver",
    "num_partitions": 1,
    "batch_size": 1000,
    "socket_timeout": 600000
  },
  "load": {
    "table": "tb_agendamentos",
    "mode": "append",
    "truncate": false,
    "output_format": "jdbc",
    "delta_path": "hdfs:///output/agendamentos_delta"
  },
  "spark": {
    "app_name": "ETL Agendamentos - Big Data",
    "shuffle_partitions": 200,
    "executor_memory": "4g",
    "num_executors": 4
  },
  "run": {
    "execute_schema": true,
    "log_level": "WARN",
    "log_file": "etl.log"
  }
}
```

## Formatos Suportados

### Entrada

| Formato | Velocidade | Compressão | Recomendação |
|---------|-----------|------------|--------------|
| Excel (.xlsx) | Lenta | Nenhuma | Apenas para migração inicial |
| CSV | Média | Nenhuma | Boa para interoperabilidade |
| **Parquet** | **Rápida** | **Snappy/Gzip** | **Ideal para Big Data** |

### Saída

| Formato | `output_format` | Recomendação |
|---------|----------------|--------------|
| MySQL (JDBC) | `jdbc` | Quando a aplicação já consome MySQL |
| Parquet | `parquet` | Data lake simples, análises batch |
| Delta Lake | `delta` | Data lake com controle transacional |

## Performance Estimada

| Volume | PySpark (local) | PySpark (cluster 4 nós) |
|--------|----------------|------------------------|
| 10K linhas | ~30s, ~1 GB RAM | Não recomendado |
| 100K linhas | ~1 min, ~2 GB RAM | ~30s, ~2 GB RAM/nó |
| 1M linhas | ~5 min, ~4 GB RAM | ~2 min, ~2 GB RAM/nó |
| 10M+ linhas | ~30 min, ~8 GB RAM | ~8 min, ~4 GB RAM/nó |
