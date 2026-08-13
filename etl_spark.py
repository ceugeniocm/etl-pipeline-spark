"""
Pipeline ETL Big Data com PySpark.

Versão distribuída do pipeline ETL de agendamentos médicos,
utilizando Apache Spark (PySpark) para processar grandes volumes de dados.

Uso:
    spark-submit --master "local[*]" \
        --driver-java-options "-Dlog4j.configurationFile=file:log4j2.properties" \
        etl_spark.py config_bigdata.json

A opção --driver-java-options aplica o log4j2.properties do projeto e
suprime as mensagens INFO de inicialização da JVM do Spark.
"""

import json
import logging
import os
import sys
import threading
import time
from urllib.parse import urlparse

import mysql.connector
from tqdm import tqdm

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


def setup_logging(config: dict) -> None:
    """Configura o sistema de logging do Python."""
    run_cfg = config.get("run", {})
    log_level_str = run_cfg.get("log_level", "INFO").upper()
    log_file = run_cfg.get("log_file")

    level = getattr(logging, log_level_str, logging.INFO)

    handlers = [logging.StreamHandler(sys.stdout)]
    if log_file:
        handlers.append(logging.FileHandler(log_file, encoding="utf-8"))

    logging.basicConfig(
        level=level,
        format="%(asctime)s - %(levelname)s - %(message)s",
        handlers=handlers,
        force=True,
    )


def create_spark_session(config: dict) -> SparkSession:
    """Cria e retorna uma SparkSession configurada."""
    spark_cfg = config.get("spark", {})
    run_cfg = config.get("run", {})
    spark_log_level = run_cfg.get("log_level", "ERROR").upper()

    # Silenciar loggers do Python que envolvem PySpark/Py4J
    logging.getLogger("pyspark").setLevel(getattr(logging, spark_log_level, logging.ERROR))
    logging.getLogger("py4j").setLevel(getattr(logging, spark_log_level, logging.ERROR))

    builder = SparkSession.builder \
        .appName(spark_cfg.get("app_name", "ETL Agendamentos - Big Data")) \
        .config(
            "spark.sql.shuffle.partitions",
            str(spark_cfg.get("shuffle_partitions", 200)),
        ) \
        .config(
            "spark.executor.memory",
            spark_cfg.get("executor_memory", "4g"),
        ) \
        .config("spark.jars.packages", "com.mysql:mysql-connector-j:8.3.0") \
        .config("spark.sql.debug.maxToStringFields", "1000") \
        .config("spark.ui.showConsoleProgress", "false")

    spark = builder.getOrCreate()

    # Configurar nível de log do SparkContext (método oficial)
    spark.sparkContext.setLogLevel(spark_log_level)

    # Tenta silenciar o Root Logger da JVM via Py4J para garantir silêncio total
    try:
        jvm = spark._jvm
        # Tenta Log4j 2 (Spark 3.x)
        try:
            level_obj = jvm.org.apache.logging.log4j.Level.toLevel(spark_log_level)
            jvm.org.apache.logging.log4j.core.config.Configurator.setAllLevels(
                jvm.org.apache.logging.log4j.LogManager.getRootLogger().getName(),
                level_obj
            )
        except Exception:
            # Fallback Log4j 1.2 (Ponte de compatibilidade ou versões antigas)
            log4j = jvm.org.apache.log4j
            log4j.LogManager.getRootLogger().setLevel(log4j.Level.toLevel(spark_log_level))
    except Exception:
        pass

    return spark


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
        logging.info(f"  Convertendo {excel_path} (aba: {sheet}) para Parquet...")
        # Lemos tudo como string para evitar OverflowError com números gigantes
        # (ex: CPF, CNS, Carteira) e garantir compatibilidade com PyArrow.
        df_pd = pd.read_excel(excel_path, sheet_name=sheet, dtype=str)
        df_pd.to_parquet(parquet_path, index=False)
        logging.info(f"  Parquet gerado: {parquet_path}")
    else:
        logging.info(f"  Cache Parquet encontrado: {parquet_path}")

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

    # 1. Trim e 2. Converter strings vazias para null
    # Usamos withColumns para aplicar em lote e evitar planos muito profundos
    string_updates = {}
    for col_name in string_cols:
        trimmed = F.trim(F.col(col_name))
        string_updates[col_name] = F.when(trimmed == "", None).otherwise(trimmed)
    
    if string_updates:
        df = df.withColumns(string_updates)

    # 3. Aplicar normalizers do mapping
    normalizers = mapping.get("normalizers", {})
    norm_updates = {}
    for col_name, norms in normalizers.items():
        if col_name not in df.columns:
            logging.warning(f"Coluna {col_name} não encontrada para normalização.")
            continue
        
        expr = F.col(col_name)
        for norm in norms:
            if norm == "upper":
                expr = F.upper(expr)
            elif norm == "strip_punctuation":
                expr = F.regexp_replace(expr, r"[.\-/]", "")
            elif norm == "collapse_spaces":
                expr = F.regexp_replace(expr, r"\s+", " ")
            elif norm == "trim":
                expr = F.trim(expr)
        
        norm_updates[col_name] = expr

    if norm_updates:
        df = df.withColumns(norm_updates)

    return df


# ---------------------------------------------------------------------------
# Transformação — Coerção de Tipos
# ---------------------------------------------------------------------------

def coerce_types(df: DataFrame, mapping: dict) -> DataFrame:
    """Converte colunas para os tipos esperados pelo banco de dados."""
    types_map = mapping.get("types", {})
    type_updates = {}
    for col_name, target_type in types_map.items():
        if col_name not in df.columns:
            continue
        if target_type == "int":
            type_updates[col_name] = F.expr(f"try_cast(`{col_name}` as BIGINT)")
        elif target_type == "bigint":
            type_updates[col_name] = F.expr(f"try_cast(`{col_name}` as BIGINT)")
        elif target_type == "decimal":
            type_updates[col_name] = F.expr(f"try_cast(`{col_name}` as DECIMAL(15,2))")
        elif target_type == "datetime":
            type_updates[col_name] = F.coalesce(
                F.expr(f"try_cast(`{col_name}` as TIMESTAMP)"),
                F.to_timestamp(F.col(col_name), "dd/MM/yyyy HH:mm:ss"),
                F.to_timestamp(F.col(col_name), "dd/MM/yyyy"),
            )
        elif target_type == "date":
            type_updates[col_name] = F.coalesce(
                F.to_date(F.col(col_name), "yyyy-MM-dd"),
                F.to_date(F.col(col_name), "dd/MM/yyyy"),
            )

    if type_updates:
        df = df.withColumns(type_updates)

    # Tratamento de nulos específicos para campos obrigatórios no banco que devem ter default 0
    if "TBL_IDCID" in df.columns:
        df = df.withColumn("TBL_IDCID", F.coalesce(F.col("TBL_IDCID"), F.lit(0)))

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
        else:
            logging.warning(f"Coluna obrigatória {col_name} não encontrada no DataFrame.")
            # Se a coluna não existe, a condição para essa linha falha
            required_condition = F.lit(False)
            break

    # Faixas de valores
    range_condition = F.lit(True)
    for col_name, rules in ranges.items():
        if col_name not in df.columns:
            logging.warning(f"Coluna {col_name} não encontrada para validação de faixa.")
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
            logging.warning(f"Coluna {col_name} não encontrada para validação de comprimento.")
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
        maximum = rules.get("maximum")
        if minimum is not None:
            reason_exprs.append(
                F.when(
                    F.col(col_name).isNotNull() & (F.col(col_name) < minimum),
                    F.lit(f"{col_name} fora do intervalo permitido"),
                )
            )
        if maximum is not None:
            reason_exprs.append(
                F.when(
                    F.col(col_name).isNotNull() & (F.col(col_name) > maximum),
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

def _resolve_env_password(password: str) -> str:
    """Resolve variáveis de ambiente na senha, ex: ${SENHA}."""
    if password.startswith("${") and password.endswith("}"):
        env_var = password[2:-1]
        return os.environ.get(env_var, password)
    return password


def build_jdbc_properties(db: dict) -> dict:
    """Monta as propriedades JDBC a partir da config 'database'."""
    password = _resolve_env_password(db.get("password", ""))
    return {
        "user": db.get("user", ""),
        "password": password,
        "driver": db.get("driver", "com.mysql.cj.jdbc.Driver"),
        "batchsize": str(db.get("batch_size", 1000)),
        "connectTimeout": str(db.get("connect_timeout", 30000)),
        # Cargas grandes em servidores remotos/compartilhados podem exceder
        # facilmente timeouts curtos; 0 = sem timeout de leitura.
        "socketTimeout": str(db.get("socket_timeout", 600000)),
        "rewriteBatchedStatements": "true",
        "tcpKeepAlive": "true",
    }


TRANSIENT_JDBC_ERRORS = (
    "communications link failure",
    "communications exception",
    "connection reset",
    "connection closed",
    "connection is closed",
    "read timed out",
    "sockettimeoutexception",
    "wait_timeout",
    "broken pipe",
)


def write_jdbc_with_retry(df: DataFrame, url: str, table: str, mode: str, properties: dict,
                          max_attempts: int = 3, backoff_seconds: int = 5) -> None:
    """Grava dados no JDBC com retry para falhas de conexão transitórias."""
    for attempt in range(1, max_attempts + 1):
        try:
            df.write.jdbc(url=url, table=table, mode=mode, properties=properties)
            return
        except Exception as exc:
            exc_str = str(exc).lower()
            transient = any(err in exc_str for err in TRANSIENT_JDBC_ERRORS)
            if not transient or attempt == max_attempts:
                raise
            wait = backoff_seconds * (2 ** (attempt - 1))
            first_line = str(exc).splitlines()[0]
            logging.warning(
                f"Falha transitória JDBC na tabela {table} "
                f"(tentativa {attempt}/{max_attempts}), aguardando {wait}s: {first_line}"
            )
            logging.debug(f"Detalhes da falha JDBC: {exc}")
            time.sleep(wait)


def load_jdbc(df: DataFrame, config: dict, table: str, mode: str) -> None:
    """Grava um DataFrame no MySQL via JDBC."""
    db = config.get("database", {})
    load_cfg = config.get("load", {})
    jdbc_url = db.get("jdbc_url", "")

    jdbc_properties = build_jdbc_properties(db)

    # Melhoria: Suportar flag truncate do JSON e mapeamento seguro de tipos para MySQL
    write_mode = mode
    if mode == "truncate" or load_cfg.get("truncate"):
        write_mode = "overwrite"
        # "truncate" é a opção correta do Spark JDBC writer: preserva o
        # schema/índices existentes em vez de DROP + CREATE da tabela.
        jdbc_properties["truncate"] = "true"

    # Melhoria: Se for MySQL, mapear TIMESTAMP para DATETIME via TIMESTAMP_NTZ para evitar
    # "Invalid default value" no CREATE TABLE automático do Spark, sem exigir SUPER privileges.
    # No Spark 3.4+, mapeamos para TIMESTAMP_NTZ que o dialeto MySQL traduz para DATETIME.
    if "mysql" in jdbc_url.lower():
        timestamp_cols = [
            f.name for f in df.schema.fields
            if isinstance(f.dataType, (TimestampType, DateType))
        ]
        bigint_cols = [
            f.name for f in df.schema.fields
            if isinstance(f.dataType, LongType)
        ]
        
        col_types = []
        if timestamp_cols:
            for col_name in timestamp_cols:
                field = df.schema[col_name]
                if isinstance(field.dataType, TimestampType):
                    col_types.append(f"`{col_name}` TIMESTAMP_NTZ")
                else:
                    col_types.append(f"`{col_name}` DATE")

        if bigint_cols:
            for col_name in bigint_cols:
                col_types.append(f"`{col_name}` BIGINT")

        if col_types:
            jdbc_properties["createTableColumnTypes"] = ", ".join(col_types)

    num_partitions = int(db.get("num_partitions", 2))
    df = df.coalesce(num_partitions)

    # Retries são seguros para modo overwrite/truncate.
    # Para dimensões em modo append, há um pequeno risco de duplicação,
    # aceito como trade-off para garantir a resiliência.
    write_jdbc_with_retry(df, jdbc_url, table, write_mode, jdbc_properties)


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

def load_dimensions(df: DataFrame, mapping: dict, config: dict, progress=None) -> None:
    """Carrega tabelas dimensão a partir do DataFrame principal."""
    dimensions = mapping.get("dimensions", [])
    total_dims = len(dimensions)

    for idx, dim in enumerate(dimensions, start=1):
        dim_mapping = dim.get("mapping", {})
        dim_columns = dim_mapping.get("columns", {})
        dim_load = dim.get("load", {})
        table = dim_load.get("table", "")
        mode = dim_load.get("mode", "append")
        unique_key = dim_load.get("unique_key", [])

        if progress:
            progress.start_step(
                f"Etapa 7/7: Carga da dimensão {table} ({idx}/{total_dims})"
            )

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
            if progress:
                progress.finish_step()
            continue

        df_dim = df.select(select_exprs)

        # Coerção de tipos da dimensão
        dim_types = dim_mapping.get("types", {})
        dim_type_updates = {}
        for col_name, target_type in dim_types.items():
            if col_name not in df_dim.columns:
                continue
            if target_type == "int":
                dim_type_updates[col_name] = F.col(col_name).cast(LongType())
            elif target_type == "datetime":
                dim_type_updates[col_name] = F.col(col_name).cast(TimestampType())
        
        if dim_type_updates:
            df_dim = df_dim.withColumns(dim_type_updates)

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
        logging.info(f"  Dimensão {table}: {count} registros carregados")
        if progress:
            progress.finish_step()


def execute_sql_script(config: dict, script_path: str) -> None:
    """Executa um script SQL no banco de dados usando mysql-connector."""
    db = config.get("database", {})
    jdbc_url = db.get("jdbc_url", "")
    user = db.get("user", "")
    password = _resolve_env_password(db.get("password", ""))

    if not os.path.exists(script_path):
        logging.error(f"Script SQL não encontrado: {script_path}")
        return

    # Extrair host e porta da JDBC URL
    # jdbc:mysql://hostname:port/database
    try:
        # Remover prefixo jdbc:mysql:// para usar urlparse
        url_to_parse = jdbc_url.replace("jdbc:mysql://", "")
        # Se não houver host (ex: jdbc:mysql:///db), urlparse falha ou retorna vazio
        parsed_url = urlparse("//" + url_to_parse)
        host = parsed_url.hostname or "localhost"
        port = parsed_url.port or 3306
        database = parsed_url.path.lstrip("/")
    except Exception as e:
        logging.error(f"Erro ao parsear JDBC URL {jdbc_url}: {e}")
        return

    logging.info(f"Executando script SQL: {script_path} no banco {database} ({host})")
    
    conn = None
    try:
        conn = mysql.connector.connect(
            host=host,
            port=port,
            user=user,
            password=password,
            database=database
        )
        cursor = conn.cursor()
        
        with open(script_path, "r", encoding="utf-8") as f:
            sql_script = f.read()
        
        # O mysql-connector permite executar múltiplos comandos se o script for
        # processado corretamente. Dividimos por ';' para evitar erros com
        # comandos vazios ou problemas de parsing em algumas versões do driver.
        statements = [s.strip() for s in sql_script.split(";") if s.strip()]
        
        for statement in statements:
            cursor.execute(statement)
            # Consumir resultados (se houver) para manter o cursor limpo
            while True:
                if cursor.with_rows:
                    cursor.fetchall()
                if not cursor.nextset():
                    break
        
        conn.commit()
        logging.info("Script SQL executado com sucesso.")
    except Exception as e:
        logging.error(f"Erro ao executar script SQL: {e}")
        if conn:
            conn.rollback()
    finally:
        if conn:
            conn.close()


# ---------------------------------------------------------------------------
# Pipeline Principal
# ---------------------------------------------------------------------------

class PipelineProgress:
    """Barra de progresso com heartbeat para etapas longas.

    Um thread em segundo plano atualiza o tqdm a cada segundo, exibindo
    há quanto tempo a etapa atual está em execução — assim etapas
    demoradas (ex.: carga JDBC) não aparentam travamento.

    Ao concluir cada etapa, uma linha permanente é impressa acima da
    barra (via ``tqdm.write``), preservando o histórico de progresso no
    console em vez de sobrescrever a mensagem anterior.
    """

    def __init__(self, total: int, refresh_interval: float = 1.0):
        self._pbar = tqdm(total=total, desc="Iniciando Pipeline ETL", unit="etapa")
        self._interval = refresh_interval
        self._lock = threading.Lock()
        self._step_start = time.time()
        self._description = ""
        self._stop = threading.Event()
        self._thread = threading.Thread(target=self._heartbeat, daemon=True)

    def _heartbeat(self) -> None:
        while not self._stop.wait(self._interval):
            with self._lock:
                elapsed = int(time.time() - self._step_start)
                minutes, seconds = divmod(elapsed, 60)
                self._pbar.set_postfix_str(f"{minutes:02d}:{seconds:02d} na etapa atual")

    def start_step(self, description: str) -> None:
        """Inicia uma nova etapa, atualizando a descrição e zerando o cronômetro."""
        with self._lock:
            self._step_start = time.time()
            self._description = description
            self._pbar.set_description(description)
            self._pbar.set_postfix_str("00:00 na etapa atual")

    def finish_step(self) -> None:
        """Marca a etapa atual como concluída, avançando a barra.

        Imprime uma linha permanente acima da barra com o tempo da etapa,
        para que o histórico de progresso não seja sobrescrito.
        """
        with self._lock:
            elapsed = int(time.time() - self._step_start)
            minutes, seconds = divmod(elapsed, 60)
            if self._description:
                self._pbar.write(
                    f"✔ {self._description} — concluída em {minutes:02d}:{seconds:02d}"
                )
            self._pbar.update(1)

    def __enter__(self) -> "PipelineProgress":
        self._thread.start()
        return self

    def __exit__(self, exc_type, exc, tb) -> None:
        self._stop.set()
        self._thread.join()
        with self._lock:
            if exc_type is None:
                self._pbar.set_description("Pipeline concluído")
            else:
                self._pbar.set_description("Pipeline interrompido")
            self._pbar.set_postfix_str("")
            self._pbar.refresh()
        self._pbar.close()


def _format_int_ptbr(value: int) -> str:
    """Formata um inteiro com separador de milhar em pt_BR (ex.: 1.234.567)."""
    return f"{value:,}".replace(",", ".")


def _format_duration_ptbr(seconds: float) -> str:
    """Formata uma duração em segundos de forma legível em pt_BR."""
    total = int(round(seconds))
    hours, remainder = divmod(total, 3600)
    minutes, secs = divmod(remainder, 60)
    if hours:
        return f"{hours}h {minutes:02d}min {secs:02d}s"
    if minutes:
        return f"{minutes}min {secs:02d}s"
    return f"{secs}s"


def print_summary(
    raw_count: int,
    loaded_count: int,
    rejected_count: int,
    duplicates: int,
    elapsed_seconds: float,
) -> None:
    """Imprime o resumo de execução em pt_BR.

    Usa ``print`` (e não ``logging``) para que o resumo apareça sempre,
    independentemente do nível de log configurado (ex.: WARN).
    """
    line = "=" * 60
    summary = [
        "",
        line,
        "Resumo da Execução",
        line,
        f"  Linhas lidas:           {_format_int_ptbr(raw_count)}",
        f"  Linhas carregadas:      {_format_int_ptbr(loaded_count)}",
        f"  Linhas rejeitadas:      {_format_int_ptbr(rejected_count)}",
        f"  Duplicatas descartadas: {_format_int_ptbr(duplicates)}",
        f"  Tempo total decorrido:  {_format_duration_ptbr(elapsed_seconds)}",
        line,
    ]
    print("\n".join(summary), flush=True)


def run_pipeline(config_path: str) -> None:
    """Executa o pipeline ETL completo."""
    pipeline_start = time.time()

    # 1. Carregar configuração
    config = load_config(config_path)
    setup_logging(config)

    logging.info("=" * 60)
    logging.info("ETL Pipeline Big Data — PySpark")
    logging.info("=" * 60)

    mapping_path = config.get("mapping", "mapping.json")
    mapping = load_mapping(mapping_path)

    # 2. Criar SparkSession
    spark = create_spark_session(config)
    logging.info(f"SparkSession criada: {spark.sparkContext.appName}")

    # Novo: Executar schema se solicitado
    run_cfg = config.get("run", {})
    if run_cfg.get("execute_schema"):
        logging.info("Execução de schema solicitada (execute_schema=true).")
        # Podemos permitir configurar o caminho do script SQL no config
        script_path = run_cfg.get("schema_script", "create_tables.sql")
        execute_sql_script(config, script_path)

    try:
        dimensions = mapping.get("dimensions", [])
        # 6 etapas de preparação + carga da tabela fato + uma etapa por dimensão
        total_steps = 7 + len(dimensions)

        with PipelineProgress(total=total_steps) as progress:
            # 3. Extração
            progress.start_step("Etapa 1/7: Extração")
            logging.info("[1/7] Extração...")
            df_raw = extract(spark, config)
            raw_count = df_raw.count()
            logging.info(f"  Registros lidos: {raw_count}")
            progress.finish_step()

            # 4. Mapeamento de colunas
            progress.start_step("Etapa 2/7: Mapeamento")
            logging.info("[2/7] Mapeamento de colunas...")
            df_mapped = apply_mapping(df_raw, mapping)
            progress.finish_step()

            # 5. Limpeza e normalização
            progress.start_step("Etapa 3/7: Limpeza e Normalização")
            logging.info("[3/7] Limpeza e normalização...")
            df_clean = clean(df_mapped, mapping)
            progress.finish_step()

            # 6. Coerção de tipos
            progress.start_step("Etapa 4/7: Coerção de Tipos")
            logging.info("[4/7] Coerção de tipos...")
            df_typed = coerce_types(df_clean, mapping).cache()
            progress.finish_step()

            # 7. Validação
            progress.start_step("Etapa 5/7: Validação")
            logging.info("[5/7] Validação...")
            df_valid, df_rejected_report = validate(df_typed, config)
            valid_count = df_valid.count()
            rejected_count = df_rejected_report.count()
            logging.info(f"  Válidos: {valid_count}")
            logging.info(f"  Rejeitados: {rejected_count}")

            # Gravar relatório de rejeições
            if rejected_count > 0:
                df_rejected_report.write \
                    .mode("overwrite") \
                    .option("header", "true") \
                    .csv("output/rejeicoes")
                logging.info("  Relatório de rejeições gravado em output/rejeicoes/")
            progress.finish_step()

            # 8. Deduplicação
            progress.start_step("Etapa 6/7: Deduplicação")
            logging.info("[6/7] Deduplicação...")
            df_dedup = deduplicate(df_valid, config)
            dedup_count = df_dedup.count()
            duplicates = valid_count - dedup_count
            logging.info(f"  Deduplicados: {dedup_count} (duplicatas removidas: {duplicates})")
            progress.finish_step()

            # 9. Tabelas dimensão
            load_cfg = config.get("load", {})
            table = load_cfg.get("table", "AGENDAMENTO")

            if dimensions:
                logging.info("  Carregando tabelas dimensão...")
                # Remover a tabela fato da lista de dimensões para carregar separadamente depois,
                # garantindo que todas as dimensões reais sejam carregadas primeiro.
                other_dims = [d for d in dimensions if d.get("load", {}).get("table") != table]
                load_dimensions(df_dedup, {"dimensions": other_dims}, config, progress=progress)

            # 10. Carga da Tabela Fato
            logging.info("[7/7] Carga...")
            output_format = load_cfg.get("output_format", "jdbc")
            mode = load_cfg.get("mode", "append")

            progress.start_step(f"Etapa 7/7: Carga da tabela fato {table}")

            # Filtrar apenas as colunas que pertencem à tabela fato (tb_agendamentos)
            # para evitar erro AnalysisException caso o DataFrame contenha colunas extras
            # que não existem na tabela de destino.
            fact_mapping = next((d for d in dimensions if d.get("load", {}).get("table") == table), None)
            df_fact = df_dedup
            if fact_mapping:
                fact_cols = list(fact_mapping.get("mapping", {}).get("columns", {}).values())
                available_cols = [c for c in fact_cols if c in df_fact.columns]
                if available_cols:
                    df_fact = df_fact.select(available_cols)
                    logging.info(f"  Colunas selecionadas for {table}: {available_cols}")

            if output_format == "jdbc":
                load_jdbc(df_fact, config, table, mode)
                logging.info(f"  Tabela {table}: {dedup_count} registros carregados via JDBC")
            elif output_format == "parquet":
                parquet_path = load_cfg.get(
                    "delta_path", "output/agendamentos_parquet",
                )
                partition_col = config.get("source", {}).get("partition_column")
                load_parquet(df_fact, parquet_path, partition_col)
                logging.info(f"  Parquet gravado em {parquet_path}")
            elif output_format == "delta":
                delta_path = load_cfg.get(
                    "delta_path", "output/agendamentos_delta",
                )
                partition_col = config.get("source", {}).get("partition_column")
                load_delta(df_fact, delta_path, partition_col)
                logging.info(f"  Delta Lake gravado em {delta_path}")
            progress.finish_step()

        # Resumo de execução (sempre visível, independente do log_level)
        elapsed_seconds = time.time() - pipeline_start
        print_summary(
            raw_count=raw_count,
            loaded_count=dedup_count,
            rejected_count=rejected_count,
            duplicates=duplicates,
            elapsed_seconds=elapsed_seconds,
        )

    finally:
        spark.stop()
        logging.info("SparkSession encerrada.")


# ---------------------------------------------------------------------------
# Ponto de Entrada
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    if len(sys.argv) < 2:
        logging.error("Uso: spark-submit etl_spark.py <config.json>")
        sys.exit(1)

    run_pipeline(sys.argv[1])
