"""
Pipeline ETL Big Data com PySpark.

Versão distribuída do pipeline ETL de agendamentos médicos,
utilizando Apache Spark (PySpark) para processar grandes volumes de dados.

Uso:
    spark-submit --master "local[*]" etl_spark.py config_bigdata.json
"""

import json
import os
import sys

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import (
    LongType,
    DecimalType,
    TimestampType,
    DateType,
    StringType,
)
from pyspark.sql.window import Window


# ---------------------------------------------------------------------------
# Configuração
# ---------------------------------------------------------------------------

def load_config(config_path: str) -> dict:
    """Carrega o arquivo de configuração JSON."""
    with open(config_path, "r", encoding="utf-8") as fh:
        return json.load(fh)


def load_mapping(mapping_path: str) -> dict:
    """Carrega o arquivo de mapeamento de colunas JSON."""
    with open(mapping_path, "r", encoding="utf-8") as fh:
        return json.load(fh)


def create_spark_session(config: dict) -> SparkSession:
    """Cria e retorna uma SparkSession configurada."""
    spark_cfg = config.get("spark", {})
    builder = SparkSession.builder \
        .appName(spark_cfg.get("app_name", "ETL Agendamentos - Big Data")) \
        .config(
            "spark.sql.shuffle.partitions",
            str(spark_cfg.get("shuffle_partitions", 200)),
        ) \
        .config(
            "spark.executor.memory",
            spark_cfg.get("executor_memory", "4g"),
        )
    return builder.getOrCreate()


# ---------------------------------------------------------------------------
# Extração
# ---------------------------------------------------------------------------

def _excel_to_parquet(
    excel_path: str,
    sheet: str,
    parquet_path: str,
) -> str:
    """Converte um arquivo Excel para Parquet quando necessário.

    Se o Parquet já existir e for mais recente que o Excel de origem,
    a conversão é ignorada e o caminho do cache é retornado diretamente.

    Retorna o caminho do arquivo Parquet resultante.
    """
    import pandas as pd  # importação local — só carrega quando necessário

    needs_conversion = True
    if os.path.exists(parquet_path):
        excel_mtime = os.path.getmtime(excel_path)
        parquet_mtime = os.path.getmtime(parquet_path)
        if parquet_mtime >= excel_mtime:
            needs_conversion = False

    if needs_conversion:
        print(f"  Convertendo {excel_path} (aba: {sheet}) para Parquet...")
        df_pd = pd.read_excel(excel_path, sheet_name=sheet)
        df_pd.to_parquet(parquet_path, index=False)
        print(f"  Parquet gerado: {parquet_path}")
    else:
        print(f"  Cache Parquet encontrado: {parquet_path}")

    return parquet_path


def extract(spark: SparkSession, config: dict) -> DataFrame:
    """Lê os dados de entrada conforme o formato configurado.

    Formatos suportados: ``parquet``, ``csv`` e ``excel``.

    Quando o formato é ``excel``, o arquivo é automaticamente convertido
    para Parquet (via pandas) e o Spark lê o Parquet resultante.
    O Parquet é cacheado ao lado do Excel original e reutilizado enquanto
    o arquivo de origem não for modificado.
    """
    source = config["source"]
    path = source["path"]
    fmt = source.get("format", "parquet")

    if fmt == "parquet":
        return spark.read.parquet(path)

    if fmt == "csv":
        return spark.read \
            .option("header", "true") \
            .option("inferSchema", "true") \
            .option("delimiter", source.get("delimiter", ";")) \
            .option("encoding", source.get("encoding", "UTF-8")) \
            .csv(path)

    if fmt == "excel":
        sheet = source.get("sheet", "AGENDA_1")
        parquet_cache = os.path.splitext(path)[0] + ".parquet"
        cached_path = _excel_to_parquet(path, sheet, parquet_cache)
        return spark.read.parquet(cached_path)

    raise ValueError(f"Formato de entrada não suportado: {fmt}")


# ---------------------------------------------------------------------------
# Transformação — Mapeamento de Colunas
# ---------------------------------------------------------------------------

def apply_mapping(df: DataFrame, mapping: dict) -> DataFrame:
    """Renomeia colunas de origem para nomes de destino."""
    column_mapping = mapping.get("columns", {})
    available = set(df.columns)
    select_exprs = []
    for source, target in column_mapping.items():
        if source in available:
            select_exprs.append(F.col(source).alias(target))
    if not select_exprs:
        return df
    return df.select(select_exprs)


# ---------------------------------------------------------------------------
# Transformação — Limpeza e Normalização
# ---------------------------------------------------------------------------

def clean(df: DataFrame, mapping: dict) -> DataFrame:
    """Aplica limpeza e normalização em colunas de texto.

    * ``trim`` em todas as colunas string.
    * Strings vazias convertidas para ``null``.
    * Normalizers do mapping (``upper``, ``strip_punctuation``,
      ``collapse_spaces``, ``trim``).
    """
    string_cols = [
        f.name for f in df.schema.fields if isinstance(f.dataType, StringType)
    ]

    # 1. Trim em todas as colunas string
    for col_name in string_cols:
        df = df.withColumn(col_name, F.trim(F.col(col_name)))

    # 2. Converter strings vazias para null
    for col_name in string_cols:
        df = df.withColumn(
            col_name,
            F.when(F.col(col_name) == "", None).otherwise(F.col(col_name)),
        )

    # 3. Aplicar normalizers do mapping
    normalizers = mapping.get("normalizers", {})
    for col_name, norms in normalizers.items():
        if col_name not in df.columns:
            continue
        for norm in norms:
            if norm == "upper":
                df = df.withColumn(col_name, F.upper(F.col(col_name)))
            elif norm == "strip_punctuation":
                df = df.withColumn(
                    col_name,
                    F.regexp_replace(F.col(col_name), r"[.\-/]", ""),
                )
            elif norm == "collapse_spaces":
                df = df.withColumn(
                    col_name,
                    F.regexp_replace(F.col(col_name), r"\s+", " "),
                )
            elif norm == "trim":
                df = df.withColumn(col_name, F.trim(F.col(col_name)))

    return df


# ---------------------------------------------------------------------------
# Transformação — Coerção de Tipos
# ---------------------------------------------------------------------------

def coerce_types(df: DataFrame, mapping: dict) -> DataFrame:
    """Converte colunas para os tipos esperados pelo banco de dados."""
    types_map = mapping.get("types", {})
    for col_name, target_type in types_map.items():
        if col_name not in df.columns:
            continue
        if target_type == "int":
            df = df.withColumn(
                col_name,
                F.expr(f"try_cast(`{col_name}` as BIGINT)"),
            )
        elif target_type == "decimal":
            df = df.withColumn(
                col_name,
                F.expr(f"try_cast(`{col_name}` as DECIMAL(15,2))"),
            )
        elif target_type == "datetime":
            df = df.withColumn(
                col_name,
                F.coalesce(
                    F.expr(f"try_cast(`{col_name}` as TIMESTAMP)"),
                    F.to_timestamp(F.col(col_name), "dd/MM/yyyy HH:mm:ss"),
                    F.to_timestamp(F.col(col_name), "dd/MM/yyyy"),
                ),
            )
        elif target_type == "date":
            df = df.withColumn(
                col_name,
                F.coalesce(
                    F.to_date(F.col(col_name), "yyyy-MM-dd"),
                    F.to_date(F.col(col_name), "dd/MM/yyyy"),
                ),
            )

    return df


# ---------------------------------------------------------------------------
# Transformação — Validação
# ---------------------------------------------------------------------------

def validate(df: DataFrame, config: dict):
    """Separa linhas válidas e rejeitadas com base nas regras de validação.

    Retorna ``(df_valid, df_rejected_report)``.
    """
    validation = config.get("validation", {})
    required_columns = validation.get("required", [])
    ranges = validation.get("ranges", {})
    max_lengths = validation.get("max_lengths", {})

    # Campos obrigatórios
    required_condition = F.lit(True)
    for col_name in required_columns:
        if col_name in df.columns:
            required_condition = required_condition & F.col(col_name).isNotNull()

    # Faixas de valores
    range_condition = F.lit(True)
    for col_name, rules in ranges.items():
        if col_name not in df.columns:
            continue
        minimum = rules.get("minimum")
        maximum = rules.get("maximum")
        col_cond = F.lit(True)
        if minimum is not None:
            col_cond = col_cond & F.when(
                F.col(col_name).isNotNull(), F.col(col_name) >= minimum,
            ).otherwise(True)
        if maximum is not None:
            col_cond = col_cond & F.when(
                F.col(col_name).isNotNull(), F.col(col_name) <= maximum,
            ).otherwise(True)
        range_condition = range_condition & col_cond

    # Comprimento máximo
    length_condition = F.lit(True)
    for col_name, max_len in max_lengths.items():
        if col_name not in df.columns:
            continue
        length_condition = length_condition & F.when(
            F.col(col_name).isNotNull(),
            F.length(F.col(col_name)) <= max_len,
        ).otherwise(True)

    all_valid = required_condition & range_condition & length_condition

    df_valid = df.filter(all_valid)
    df_rejected = df.filter(~all_valid)

    # Motivos de rejeição
    reason_exprs = []
    for col_name in required_columns:
        if col_name in df.columns:
            reason_exprs.append(
                F.when(
                    F.col(col_name).isNull(),
                    F.lit(f"{col_name} é obrigatório"),
                )
            )
    for col_name, rules in ranges.items():
        if col_name not in df.columns:
            continue
        minimum = rules.get("minimum")
        if minimum is not None:
            reason_exprs.append(
                F.when(
                    F.col(col_name).isNotNull() & (F.col(col_name) < minimum),
                    F.lit(f"{col_name} fora do intervalo permitido"),
                )
            )
    for col_name, max_len in max_lengths.items():
        if col_name not in df.columns:
            continue
        reason_exprs.append(
            F.when(
                F.col(col_name).isNotNull()
                & (F.length(F.col(col_name)) > max_len),
                F.lit(f"{col_name} excede comprimento máximo"),
            )
        )

    df_rejected_report = df_rejected.withColumn(
        "motivo_rejeicao", F.concat_ws("; ", *reason_exprs),
    )

    return df_valid, df_rejected_report


# ---------------------------------------------------------------------------
# Transformação — Deduplicação
# ---------------------------------------------------------------------------

def deduplicate(df: DataFrame, config: dict) -> DataFrame:
    """Remove registros duplicados com base na chave de negócio."""
    validation = config.get("validation", {})
    business_key = validation.get("business_key", [])
    on_duplicate = validation.get("on_duplicate", "discard")

    if not business_key:
        return df

    existing_keys = [k for k in business_key if k in df.columns]
    if not existing_keys:
        return df

    if on_duplicate == "discard":
        return df.dropDuplicates(existing_keys)

    # Para "report", mantém a primeira ocorrência usando window
    window = Window.partitionBy(existing_keys).orderBy(F.lit(1))
    df_numbered = df.withColumn("_row_num", F.row_number().over(window))
    df_dedup = df_numbered.filter(F.col("_row_num") == 1).drop("_row_num")
    return df_dedup


# ---------------------------------------------------------------------------
# Carga
# ---------------------------------------------------------------------------

def load_jdbc(df: DataFrame, config: dict, table: str, mode: str) -> None:
    """Grava um DataFrame no MySQL via JDBC."""
    db = config["database"]
    jdbc_url = db.get("jdbc_url", "")

    # Substituir variáveis de ambiente na senha
    password = db.get("password", "")
    if password.startswith("${") and password.endswith("}"):
        env_var = password[2:-1]
        password = os.environ.get(env_var, password)

    jdbc_properties = {
        "user": db.get("user", ""),
        "password": password,
        "driver": db.get("driver", "com.mysql.cj.jdbc.Driver"),
        "batchsize": str(db.get("batch_size", 5000)),
        "numPartitions": str(db.get("num_partitions", 4)),
    }

    write_mode = mode
    if mode == "truncate":
        write_mode = "overwrite"
        jdbc_properties["truncateTable"] = "true"

    df.write.jdbc(
        url=jdbc_url,
        table=table,
        mode=write_mode,
        properties=jdbc_properties,
    )


def load_parquet(df: DataFrame, path: str, partition_col: str = None) -> None:
    """Grava um DataFrame em formato Parquet."""
    writer = df.write.mode("overwrite")
    if partition_col and partition_col in df.columns:
        writer = writer.partitionBy(partition_col)
    writer.parquet(path)


def load_delta(df: DataFrame, path: str, partition_col: str = None) -> None:
    """Grava um DataFrame em formato Delta Lake."""
    writer = df.write.format("delta").mode("overwrite")
    if partition_col and partition_col in df.columns:
        writer = writer.partitionBy(partition_col)
    writer.save(path)


# ---------------------------------------------------------------------------
# Carga — Tabelas Dimensão
# ---------------------------------------------------------------------------

def load_dimensions(df: DataFrame, mapping: dict, config: dict) -> None:
    """Carrega tabelas dimensão a partir do DataFrame principal."""
    dimensions = mapping.get("dimensions", [])

    for dim in dimensions:
        dim_mapping = dim.get("mapping", {})
        dim_columns = dim_mapping.get("columns", {})
        dim_load = dim.get("load", {})
        table = dim_load.get("table", "")
        mode = dim_load.get("mode", "append")
        unique_key = dim_load.get("unique_key", [])

        # Selecionar e renomear colunas da dimensão
        available = set(df.columns)
        # As colunas de origem na dimensão podem estar com nomes já mapeados
        # (nomes de destino da tabela fato). Precisamos mapear os nomes
        # do DataFrame atual para os nomes de destino da dimensão.
        select_exprs = []
        found_any = False
        for source, target in dim_columns.items():
            # Tentar pelo nome original (maiúsculo)
            if source in available:
                select_exprs.append(F.col(source).alias(target))
                found_any = True
            else:
                # Tentar pelo nome já mapeado (minúsculo)
                source_lower = source.lower()
                if source_lower in available:
                    select_exprs.append(F.col(source_lower).alias(target))
                    found_any = True

        if not found_any:
            continue

        df_dim = df.select(select_exprs)

        # Coerção de tipos da dimensão
        dim_types = dim_mapping.get("types", {})
        for col_name, target_type in dim_types.items():
            if col_name not in df_dim.columns:
                continue
            if target_type == "int":
                df_dim = df_dim.withColumn(
                    col_name, F.col(col_name).cast(LongType()),
                )
            elif target_type == "datetime":
                df_dim = df_dim.withColumn(
                    col_name, F.col(col_name).cast(TimestampType()),
                )

        # Deduplicação pela chave única
        if unique_key:
            existing_keys = [k for k in unique_key if k in df_dim.columns]
            if existing_keys:
                # Filtrar linhas onde a chave é nula
                for key_col in existing_keys:
                    df_dim = df_dim.filter(F.col(key_col).isNotNull())
                df_dim = df_dim.dropDuplicates(existing_keys)

        load_jdbc(df_dim, config, table, mode)
        count = df_dim.count()
        print(f"  Dimensão {table}: {count} registros carregados")


# ---------------------------------------------------------------------------
# Pipeline Principal
# ---------------------------------------------------------------------------

def run_pipeline(config_path: str) -> None:
    """Executa o pipeline ETL completo."""
    print("=" * 60)
    print("ETL Pipeline Big Data — PySpark")
    print("=" * 60)

    # 1. Carregar configuração
    config = load_config(config_path)
    mapping_path = config.get("mapping", "mapping.json")
    mapping = load_mapping(mapping_path)

    # 2. Criar SparkSession
    spark = create_spark_session(config)
    print(f"\nSparkSession criada: {spark.sparkContext.appName}")

    try:
        # 3. Extração
        print("\n[1/5] Extração...")
        df_raw = extract(spark, config)
        raw_count = df_raw.count()
        print(f"  Registros lidos: {raw_count}")

        # 4. Mapeamento de colunas
        print("\n[2/5] Mapeamento de colunas...")
        df_mapped = apply_mapping(df_raw, mapping)

        # 5. Limpeza e normalização
        print("\n[3/5] Limpeza e normalização...")
        df_clean = clean(df_mapped, mapping)

        # 6. Coerção de tipos
        print("       Coerção de tipos...")
        df_typed = coerce_types(df_clean, mapping)

        # 7. Validação
        print("\n[4/5] Validação...")
        df_valid, df_rejected_report = validate(df_typed, config)
        valid_count = df_valid.count()
        rejected_count = df_rejected_report.count()
        print(f"  Válidos: {valid_count}")
        print(f"  Rejeitados: {rejected_count}")

        # Gravar relatório de rejeições
        if rejected_count > 0:
            df_rejected_report.write \
                .mode("overwrite") \
                .option("header", "true") \
                .csv("output/rejeicoes")
            print("  Relatório de rejeições gravado em output/rejeicoes/")

        # 8. Deduplicação
        df_dedup = deduplicate(df_valid, config)
        dedup_count = df_dedup.count()
        duplicates = valid_count - dedup_count
        print(f"  Deduplicados: {dedup_count} (duplicatas removidas: {duplicates})")

        # 9. Carga
        print("\n[5/5] Carga...")
        load_cfg = config.get("load", {})
        output_format = load_cfg.get("output_format", "jdbc")
        table = load_cfg.get("table", "tb_agendamentos")
        mode = load_cfg.get("mode", "append")

        if output_format == "jdbc":
            load_jdbc(df_dedup, config, table, mode)
            print(f"  Tabela {table}: {dedup_count} registros carregados via JDBC")
        elif output_format == "parquet":
            parquet_path = load_cfg.get(
                "delta_path", "output/agendamentos_parquet",
            )
            partition_col = config.get("source", {}).get("partition_column")
            load_parquet(df_dedup, parquet_path, partition_col)
            print(f"  Parquet gravado em {parquet_path}")
        elif output_format == "delta":
            delta_path = load_cfg.get(
                "delta_path", "output/agendamentos_delta",
            )
            partition_col = config.get("source", {}).get("partition_column")
            load_delta(df_dedup, delta_path, partition_col)
            print(f"  Delta Lake gravado em {delta_path}")

        # 10. Tabelas dimensão
        if mapping.get("dimensions"):
            print("\n  Carregando tabelas dimensão...")
            load_dimensions(df_dedup, mapping, config)

        # Resumo
        print("\n" + "=" * 60)
        print("Resumo do Pipeline")
        print("=" * 60)
        print(f"  Total lidos:       {raw_count}")
        print(f"  Válidos:           {valid_count}")
        print(f"  Rejeitados:        {rejected_count}")
        print(f"  Duplicatas:        {duplicates}")
        print(f"  Carregados:        {dedup_count}")
        print("=" * 60)

    finally:
        spark.stop()
        print("\nSparkSession encerrada.")


# ---------------------------------------------------------------------------
# Ponto de Entrada
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Uso: spark-submit etl_spark.py <config.json>")
        sys.exit(1)

    run_pipeline(sys.argv[1])
