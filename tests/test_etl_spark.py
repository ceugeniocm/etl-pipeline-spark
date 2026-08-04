"""Testes unitários para o pipeline ETL Big Data com PySpark."""

import json
import os
import tempfile
import unittest

try:
    from pyspark.sql import SparkSession
    from pyspark.sql import functions as F
    from pyspark.sql.types import (
        StructType,
        StructField,
        StringType,
        LongType,
        DoubleType,
    )

    HAS_PYSPARK = True
except ImportError:
    HAS_PYSPARK = False

if HAS_PYSPARK:
    from etl_spark import (
        load_config,
        load_mapping,
        apply_mapping,
        clean,
        coerce_types,
        validate,
        deduplicate,
        _excel_to_parquet,
        load_jdbc,
        build_jdbc_properties,
        write_jdbc_with_retry,
        _format_int_ptbr,
        _format_duration_ptbr,
        print_summary,
        PipelineProgress,
    )


@unittest.skipUnless(HAS_PYSPARK, "pyspark não está instalado")
class TestConfig(unittest.TestCase):
    """Testes para funções de configuração."""

    def test_load_config(self):
        config = {
            "source": {"path": "test.parquet", "format": "parquet"},
            "mapping": "mapping.json",
        }
        with tempfile.NamedTemporaryFile(
            mode="w", suffix=".json", delete=False,
        ) as f:
            json.dump(config, f)
            f.flush()
            result = load_config(f.name)
        os.unlink(f.name)
        self.assertEqual(result["source"]["path"], "test.parquet")

    def test_load_mapping(self):
        mapping = {"columns": {"A": "a"}, "types": {"a": "int"}}
        with tempfile.NamedTemporaryFile(
            mode="w", suffix=".json", delete=False,
        ) as f:
            json.dump(mapping, f)
            f.flush()
            result = load_mapping(f.name)
        os.unlink(f.name)
        self.assertEqual(result["columns"]["A"], "a")


@unittest.skipUnless(HAS_PYSPARK, "pyspark não está instalado")
class SparkTestBase(unittest.TestCase):
    """Classe base que fornece uma SparkSession para os testes."""

    spark = None

    @classmethod
    def setUpClass(cls):
        cls.spark = (
            SparkSession.builder
            .master("local[1]")
            .appName("ETL Spark Tests")
            .config("spark.sql.shuffle.partitions", "1")
            .config("spark.ui.enabled", "false")
            .config("spark.driver.host", "localhost")
            .getOrCreate()
        )

    @classmethod
    def tearDownClass(cls):
        if cls.spark is not None:
            cls.spark.stop()
            cls.spark = None


@unittest.skipUnless(HAS_PYSPARK, "pyspark não está instalado")
class TestApplyMapping(SparkTestBase):
    """Testes para a função apply_mapping."""

    def test_rename_columns(self):
        df = self.spark.createDataFrame(
            [("1", "João", "100")],
            ["AG_ID", "BENEF_NOME", "AGP_VALOR"],
        )
        mapping = {
            "columns": {
                "AG_ID": "id_agendamento",
                "BENEF_NOME": "paciente_nome",
                "AGP_VALOR": "valor",
            },
        }
        result = apply_mapping(df, mapping)
        self.assertEqual(
            sorted(result.columns),
            sorted(["id_agendamento", "paciente_nome", "valor"]),
        )
        row = result.collect()[0]
        self.assertEqual(row["id_agendamento"], "1")
        self.assertEqual(row["paciente_nome"], "João")

    def test_missing_source_columns_ignored(self):
        df = self.spark.createDataFrame([("1",)], ["AG_ID"])
        mapping = {
            "columns": {
                "AG_ID": "id_agendamento",
                "COLUNA_INEXISTENTE": "fantasma",
            },
        }
        result = apply_mapping(df, mapping)
        self.assertEqual(result.columns, ["id_agendamento"])

    def test_empty_mapping_returns_original(self):
        df = self.spark.createDataFrame([("1",)], ["AG_ID"])
        result = apply_mapping(df, {"columns": {}})
        self.assertEqual(result.columns, ["AG_ID"])


@unittest.skipUnless(HAS_PYSPARK, "pyspark não está instalado")
class TestClean(SparkTestBase):
    """Testes para a função clean."""

    def test_trim_whitespace(self):
        df = self.spark.createDataFrame(
            [("  João  ",)], ["nome"],
        )
        mapping = {"normalizers": {}}
        result = clean(df, mapping)
        row = result.collect()[0]
        self.assertEqual(row["nome"], "João")

    def test_empty_string_to_null(self):
        df = self.spark.createDataFrame([("",)], ["nome"])
        mapping = {"normalizers": {}}
        result = clean(df, mapping)
        row = result.collect()[0]
        self.assertIsNone(row["nome"])

    def test_upper_normalizer(self):
        df = self.spark.createDataFrame([("joão silva",)], ["nome"])
        mapping = {"normalizers": {"nome": ["upper"]}}
        result = clean(df, mapping)
        row = result.collect()[0]
        self.assertEqual(row["nome"], "JOÃO SILVA")

    def test_strip_punctuation_normalizer(self):
        df = self.spark.createDataFrame([("123.456.789-00",)], ["cpf"])
        mapping = {"normalizers": {"cpf": ["strip_punctuation"]}}
        result = clean(df, mapping)
        row = result.collect()[0]
        self.assertEqual(row["cpf"], "12345678900")

    def test_collapse_spaces_normalizer(self):
        df = self.spark.createDataFrame(
            [("João   da   Silva",)], ["nome"],
        )
        mapping = {"normalizers": {"nome": ["collapse_spaces"]}}
        result = clean(df, mapping)
        row = result.collect()[0]
        self.assertEqual(row["nome"], "João da Silva")


@unittest.skipUnless(HAS_PYSPARK, "pyspark não está instalado")
class TestCoerceTypes(SparkTestBase):
    """Testes para a função coerce_types."""

    def test_int_coercion(self):
        df = self.spark.createDataFrame([("42",)], ["id"])
        mapping = {"types": {"id": "int"}}
        result = coerce_types(df, mapping)
        row = result.collect()[0]
        self.assertEqual(row["id"], 42)

    def test_decimal_coercion(self):
        df = self.spark.createDataFrame([("99.50",)], ["valor"])
        mapping = {"types": {"valor": "decimal"}}
        result = coerce_types(df, mapping)
        row = result.collect()[0]
        from decimal import Decimal

        self.assertEqual(row["valor"], Decimal("99.50"))

    def test_invalid_int_becomes_null(self):
        df = self.spark.createDataFrame([("abc",)], ["id"])
        mapping = {"types": {"id": "int"}}
        result = coerce_types(df, mapping)
        row = result.collect()[0]
        self.assertIsNone(row["id"])

    def test_missing_column_ignored(self):
        df = self.spark.createDataFrame([("1",)], ["id"])
        mapping = {"types": {"coluna_inexistente": "int"}}
        result = coerce_types(df, mapping)
        self.assertEqual(result.columns, ["id"])
        row = result.collect()[0]
        self.assertEqual(row["id"], "1")


@unittest.skipUnless(HAS_PYSPARK, "pyspark não está instalado")
class TestValidate(SparkTestBase):
    """Testes para a função validate."""

    def test_required_field_valid(self):
        df = self.spark.createDataFrame(
            [(1, 100, "2024-01-01")],
            ["id_agendamento", "benef_id", "data"],
        )
        config = {
            "validation": {
                "required": ["id_agendamento", "benef_id", "data"],
                "ranges": {},
                "max_lengths": {},
            },
        }
        df_valid, df_rejected = validate(df, config)
        self.assertEqual(df_valid.count(), 1)
        self.assertEqual(df_rejected.count(), 0)

    def test_required_field_null_rejected(self):
        schema = StructType([
            StructField("id_agendamento", LongType(), True),
            StructField("benef_id", LongType(), True),
            StructField("data", StringType(), True),
        ])
        df = self.spark.createDataFrame(
            [(None, 100, "2024-01-01")], schema,
        )
        config = {
            "validation": {
                "required": ["id_agendamento"],
                "ranges": {},
                "max_lengths": {},
            },
        }
        df_valid, df_rejected = validate(df, config)
        self.assertEqual(df_valid.count(), 0)
        self.assertEqual(df_rejected.count(), 1)

    def test_rejection_reason_populated(self):
        schema = StructType([
            StructField("id_agendamento", LongType(), True),
            StructField("benef_id", LongType(), True),
            StructField("data", StringType(), True),
        ])
        df = self.spark.createDataFrame(
            [(None, 100, "2024-01-01")], schema,
        )
        config = {
            "validation": {
                "required": ["id_agendamento"],
                "ranges": {},
                "max_lengths": {},
            },
        }
        _, df_rejected = validate(df, config)
        row = df_rejected.collect()[0]
        self.assertIn("id_agendamento é obrigatório", row["motivo_rejeicao"])

    def test_range_violation_rejected(self):
        schema = StructType([
            StructField("id_agendamento", LongType(), True),
            StructField("valor", DoubleType(), True),
        ])
        df = self.spark.createDataFrame([(1, -5.0)], schema)
        config = {
            "validation": {
                "required": [],
                "ranges": {"valor": {"minimum": 0}},
                "max_lengths": {},
            },
        }
        df_valid, df_rejected = validate(df, config)
        self.assertEqual(df_valid.count(), 0)
        self.assertEqual(df_rejected.count(), 1)

    def test_max_length_violation_rejected(self):
        df = self.spark.createDataFrame(
            [(1, "A" * 300)], ["id", "paciente_nome"],
        )
        config = {
            "validation": {
                "required": [],
                "ranges": {},
                "max_lengths": {"paciente_nome": 255},
            },
        }
        df_valid, df_rejected = validate(df, config)
        self.assertEqual(df_valid.count(), 0)
        self.assertEqual(df_rejected.count(), 1)

    def test_all_rules_pass(self):
        schema = StructType([
            StructField("id_agendamento", LongType(), True),
            StructField("valor", DoubleType(), True),
            StructField("paciente_nome", StringType(), True),
        ])
        df = self.spark.createDataFrame(
            [(1, 50.0, "João")], schema,
        )
        config = {
            "validation": {
                "required": ["id_agendamento"],
                "ranges": {"valor": {"minimum": 0}},
                "max_lengths": {"paciente_nome": 255},
            },
        }
        df_valid, df_rejected = validate(df, config)
        self.assertEqual(df_valid.count(), 1)
        self.assertEqual(df_rejected.count(), 0)


@unittest.skipUnless(HAS_PYSPARK, "pyspark não está instalado")
class TestDeduplicate(SparkTestBase):
    """Testes para a função deduplicate."""

    def test_remove_duplicates(self):
        df = self.spark.createDataFrame(
            [(1, 100, "2024-01-01"), (1, 100, "2024-01-01")],
            ["id_agendamento", "benef_id", "data_hora"],
        )
        config = {
            "validation": {
                "business_key": ["id_agendamento", "benef_id", "data_hora"],
                "on_duplicate": "discard",
            },
        }
        result = deduplicate(df, config)
        self.assertEqual(result.count(), 1)

    def test_keep_distinct_records(self):
        df = self.spark.createDataFrame(
            [(1, 100, "2024-01-01"), (2, 200, "2024-01-02")],
            ["id_agendamento", "benef_id", "data_hora"],
        )
        config = {
            "validation": {
                "business_key": ["id_agendamento", "benef_id", "data_hora"],
                "on_duplicate": "discard",
            },
        }
        result = deduplicate(df, config)
        self.assertEqual(result.count(), 2)

    def test_no_business_key_returns_all(self):
        df = self.spark.createDataFrame(
            [(1, 100), (1, 100)],
            ["id_agendamento", "benef_id"],
        )
        config = {"validation": {"business_key": []}}
        result = deduplicate(df, config)
        self.assertEqual(result.count(), 2)

    def test_report_mode_deduplicates(self):
        df = self.spark.createDataFrame(
            [(1, 100), (1, 100), (2, 200)],
            ["id_agendamento", "benef_id"],
        )
        config = {
            "validation": {
                "business_key": ["id_agendamento", "benef_id"],
                "on_duplicate": "report",
            },
        }
        result = deduplicate(df, config)
        self.assertEqual(result.count(), 2)


@unittest.skipUnless(HAS_PYSPARK, "pyspark não está instalado")
class TestExcelToParquet(unittest.TestCase):
    """Testes para a conversão Excel → Parquet."""

    def _create_excel(self, path, sheet="AGENDA_1"):
        """Cria um arquivo Excel simples para testes."""
        import pandas as pd

        df = pd.DataFrame({
            "AG_ID": [1, 2, 3],
            "BENEF_NOME": ["João", "Maria", "Pedro"],
            "AGP_VALOR": [100.50, 200.00, 50.25],
        })
        df.to_excel(path, sheet_name=sheet, index=False)

    def test_converts_excel_to_parquet(self):
        """Deve criar arquivo Parquet a partir do Excel."""
        import pandas as pd

        with tempfile.TemporaryDirectory() as tmpdir:
            excel_path = os.path.join(tmpdir, "dados.xlsx")
            parquet_path = os.path.join(tmpdir, "dados.parquet")
            self._create_excel(excel_path)

            result = _excel_to_parquet(excel_path, "AGENDA_1", parquet_path)

            self.assertEqual(result, parquet_path)
            self.assertTrue(os.path.exists(parquet_path))
            df = pd.read_parquet(parquet_path)
            self.assertEqual(len(df), 3)
            self.assertIn("AG_ID", df.columns)

    def test_uses_cache_when_parquet_is_newer(self):
        """Deve reutilizar o Parquet existente se for mais recente."""
        import pandas as pd

        with tempfile.TemporaryDirectory() as tmpdir:
            excel_path = os.path.join(tmpdir, "dados.xlsx")
            parquet_path = os.path.join(tmpdir, "dados.parquet")
            self._create_excel(excel_path)

            # Primeira conversão
            _excel_to_parquet(excel_path, "AGENDA_1", parquet_path)
            first_mtime = os.path.getmtime(parquet_path)

            # Pequena pausa para garantir diferença de mtime
            import time
            time.sleep(0.05)

            # Segunda chamada — deve usar cache (não reconverter)
            _excel_to_parquet(excel_path, "AGENDA_1", parquet_path)
            second_mtime = os.path.getmtime(parquet_path)

            self.assertEqual(first_mtime, second_mtime)

    def test_reconverts_when_excel_is_newer(self):
        """Deve reconverter se o Excel for modificado após o Parquet."""
        import pandas as pd
        import time

        with tempfile.TemporaryDirectory() as tmpdir:
            excel_path = os.path.join(tmpdir, "dados.xlsx")
            parquet_path = os.path.join(tmpdir, "dados.parquet")
            self._create_excel(excel_path)

            # Primeira conversão
            _excel_to_parquet(excel_path, "AGENDA_1", parquet_path)
            first_mtime = os.path.getmtime(parquet_path)

            # Recriar o Excel (simula modificação)
            time.sleep(0.05)
            df_new = pd.DataFrame({
                "AG_ID": [10, 20],
                "BENEF_NOME": ["Ana", "Carlos"],
                "AGP_VALOR": [300.00, 400.00],
            })
            df_new.to_excel(excel_path, sheet_name="AGENDA_1", index=False)

            # Segunda chamada — deve reconverter
            _excel_to_parquet(excel_path, "AGENDA_1", parquet_path)

            df_result = pd.read_parquet(parquet_path)
            self.assertEqual(len(df_result), 2)
            # Como agora lemos como string, verificamos por "10"
            self.assertIn("10", df_result["AG_ID"].values)

    def test_custom_sheet_name(self):
        """Deve ler a aba correta do Excel."""
        import pandas as pd

        with tempfile.TemporaryDirectory() as tmpdir:
            excel_path = os.path.join(tmpdir, "dados.xlsx")
            parquet_path = os.path.join(tmpdir, "dados.parquet")
            self._create_excel(excel_path, sheet="Minha_Aba")

            _excel_to_parquet(excel_path, "Minha_Aba", parquet_path)

            df = pd.read_parquet(parquet_path)
            self.assertEqual(len(df), 3)

    def test_large_integers_in_excel(self):
        """Deve lidar com números gigantes no Excel sem OverflowError."""
        import pandas as pd

        with tempfile.TemporaryDirectory() as tmpdir:
            excel_path = os.path.join(tmpdir, "large_ints.xlsx")
            parquet_path = os.path.join(tmpdir, "large_ints.parquet")

            # Valor maior que 2^63 - 1 (simulado como string no Excel para garantir)
            large_val_str = "55788888479303660000"
            df_pd = pd.DataFrame({
                "BIG_ID": [large_val_str],
                "NORMAL_ID": [123]
            })
            df_pd.to_excel(excel_path, index=False)

            # Não deve lançar OverflowError
            result = _excel_to_parquet(excel_path, "Sheet1", parquet_path)

            self.assertTrue(os.path.exists(parquet_path))
            df_result = pd.read_parquet(parquet_path)
            self.assertEqual(df_result["BIG_ID"].iloc[0], large_val_str)


@unittest.skipUnless(HAS_PYSPARK, "pyspark não está instalado")
class TestExtractExcel(SparkTestBase):
    """Testes para extração de Excel (com conversão para Parquet)."""

    def test_extract_excel_creates_parquet_and_reads(self):
        """extract() com formato excel deve converter e ler via Parquet."""
        import pandas as pd
        from etl_spark import extract

        with tempfile.TemporaryDirectory() as tmpdir:
            excel_path = os.path.join(tmpdir, "test.xlsx")
            df_pd = pd.DataFrame({
                "AG_ID": [1, 2],
                "BENEF_NOME": ["João", "Maria"],
            })
            df_pd.to_excel(excel_path, sheet_name="AGENDA_1", index=False)

            config = {
                "source": {
                    "path": excel_path,
                    "format": "excel",
                    "sheet": "AGENDA_1",
                },
            }
            df = extract(self.spark, config)
            self.assertEqual(df.count(), 2)
            self.assertIn("AG_ID", df.columns)

            # Verifica que o Parquet foi criado
            parquet_path = os.path.join(tmpdir, "test.parquet")
            self.assertTrue(os.path.exists(parquet_path))


@unittest.skipUnless(HAS_PYSPARK, "pyspark não está instalado")
class TestExtractCSV(SparkTestBase):
    """Testes para extração de CSV."""

    def test_extract_csv(self):
        from etl_spark import extract

        with tempfile.TemporaryDirectory() as tmpdir:
            csv_path = os.path.join(tmpdir, "test.csv")
            with open(csv_path, "w", encoding="utf-8") as f:
                f.write("AG_ID;BENEF_NOME;AGP_VALOR\n")
                f.write("1;João;100.50\n")
                f.write("2;Maria;200.00\n")

            config = {
                "source": {
                    "path": csv_path,
                    "format": "csv",
                    "delimiter": ";",
                },
            }
            df = extract(self.spark, config)
            self.assertEqual(df.count(), 2)
            self.assertIn("AG_ID", df.columns)


@unittest.skipUnless(HAS_PYSPARK, "pyspark não está instalado")
class TestExtractParquet(SparkTestBase):
    """Testes para extração de Parquet."""

    def test_extract_parquet(self):
        from etl_spark import extract

        with tempfile.TemporaryDirectory() as tmpdir:
            parquet_path = os.path.join(tmpdir, "test.parquet")
            df_orig = self.spark.createDataFrame(
                [(1, "João"), (2, "Maria")], ["id", "nome"],
            )
            df_orig.write.parquet(parquet_path)

            config = {
                "source": {"path": parquet_path, "format": "parquet"},
            }
            df = extract(self.spark, config)
            self.assertEqual(df.count(), 2)


@unittest.skipUnless(HAS_PYSPARK, "pyspark não está instalado")
class TestExtractInvalidFormat(SparkTestBase):
    """Teste para formato de entrada inválido."""

    def test_invalid_format_raises(self):
        from etl_spark import extract

        config = {
            "source": {"path": "dummy.txt", "format": "xml"},
        }
        with self.assertRaises(ValueError):
            extract(self.spark, config)


@unittest.skipUnless(HAS_PYSPARK, "pyspark não está instalado")
class TestIntegrationPipeline(SparkTestBase):
    """Teste de integração: mapeamento → limpeza → tipos → validação → dedup."""

    def test_full_transform_pipeline(self):
        df = self.spark.createDataFrame(
            [
                ("1", "  joão silva  ", "100.50", "2024-01-15"),
                ("2", "  maria  ", "-5.00", "2024-01-16"),
                ("1", "  joão silva  ", "100.50", "2024-01-15"),
            ],
            ["AG_ID", "BENEF_NOME", "AGP_VALOR", "DATA"],
        )

        mapping = {
            "columns": {
                "AG_ID": "id_agendamento",
                "BENEF_NOME": "paciente_nome",
                "AGP_VALOR": "valor",
                "DATA": "data",
            },
            "types": {
                "id_agendamento": "int",
                "valor": "decimal",
            },
            "normalizers": {
                "paciente_nome": ["trim", "upper"],
            },
        }

        config = {
            "validation": {
                "required": ["id_agendamento"],
                "ranges": {"valor": {"minimum": 0}},
                "max_lengths": {"paciente_nome": 255},
                "business_key": ["id_agendamento", "data"],
                "on_duplicate": "discard",
            },
        }

        # Mapeamento
        df_mapped = apply_mapping(df, mapping)
        self.assertIn("id_agendamento", df_mapped.columns)

        # Limpeza
        df_clean = clean(df_mapped, mapping)

        # Coerção de tipos
        df_typed = coerce_types(df_clean, mapping)

        # Validação
        df_valid, df_rejected = validate(df_typed, config)
        # Linha 2 (valor=-5) deve ser rejeitada
        self.assertEqual(df_rejected.count(), 1)

        # Deduplicação — linhas 1 e 3 são duplicatas
        df_dedup = deduplicate(df_valid, config)
        self.assertEqual(df_dedup.count(), 1)

        # Verificar dados finais
        row = df_dedup.collect()[0]
        self.assertEqual(row["id_agendamento"], 1)
        self.assertEqual(row["paciente_nome"], "JOÃO SILVA")


@unittest.skipUnless(HAS_PYSPARK, "pyspark não está instalado")
class TestLoadJDBC(SparkTestBase):
    """Testes para a função load_jdbc (usando mocks)."""

    def test_load_jdbc_properties_mysql_type_mapping(self):
        """Deve configurar TIMESTAMP_NTZ para MySQL para evitar erro de default inválido."""
        from unittest.mock import patch
        from pyspark.sql.types import StructType, StructField, TimestampType, DateType

        schema = StructType([
            StructField("data", TimestampType(), True),
            StructField("dt_nasc", DateType(), True),
        ])
        df = self.spark.createDataFrame(self.spark.sparkContext.emptyRDD(), schema)

        config = {
            "database": {
                "jdbc_url": "jdbc:mysql://localhost:3306/test",
                "user": "user",
                "password": "pass",
                "num_partitions": 2
            },
            "load": {"truncate": True}
        }

        with patch("pyspark.sql.readwriter.DataFrameWriter.jdbc") as mock_jdbc:
            load_jdbc(df, config, "tb_test", "overwrite")
            kwargs = mock_jdbc.call_args[1]
            properties = kwargs.get('properties', {})

            # Verifica flag truncate (opção oficial do Spark JDBC writer)
            self.assertEqual(properties.get('truncate'), "true")

            # Verifica mapeamento de tipos MySQL (TIMESTAMP_NTZ para DATETIME no MySQL)
            col_types = properties.get('createTableColumnTypes', "")
            self.assertIn("`data` TIMESTAMP_NTZ", col_types)
            self.assertIn("`dt_nasc` DATE", col_types)

            # Verifica que sessionVariables NÃO está presente (para evitar erro de SUPER privilege)
            self.assertNotIn('sessionVariables', properties)

    def test_load_jdbc_respects_truncate_flag(self):
        """Deve respeitar o flag truncate da configuração mesmo em modo overwrite."""
        from unittest.mock import patch
        df = self.spark.createDataFrame([("1",)], ["id"])
        config = {
            "database": {"jdbc_url": "jdbc:postgresql://localhost/db"},
            "load": {"truncate": True}
        }

        with patch("pyspark.sql.readwriter.DataFrameWriter.jdbc") as mock_jdbc:
            load_jdbc(df, config, "tb_test", "overwrite")
            kwargs = mock_jdbc.call_args[1]
            properties = kwargs.get('properties', {})
            self.assertEqual(properties.get('truncate'), "true")

    def test_load_jdbc_coalesce(self):
        """Deve chamar coalesce com o num_partitions configurado."""
        from unittest.mock import patch
        df = self.spark.createDataFrame([("1",)], ["id"])
        config = {
            "database": {"jdbc_url": "jdbc:mysql://localhost/db", "num_partitions": 3},
            "load": {}
        }
        
        with patch.object(df, "coalesce", return_value=df) as mock_coalesce:
            with patch("pyspark.sql.readwriter.DataFrameWriter.jdbc"):
                load_jdbc(df, config, "tb_test", "overwrite")
                mock_coalesce.assert_called_once_with(3)


@unittest.skipUnless(HAS_PYSPARK, "pyspark não está instalado")
class TestBuildJdbcProperties(unittest.TestCase):
    """Testes para a criação de propriedades JDBC."""

    def test_build_jdbc_properties_defaults(self):
        """Deve definir defaults corretos incluindo rewriteBatchedStatements."""
        db = {}
        props = build_jdbc_properties(db)
        self.assertEqual(props["user"], "")
        self.assertEqual(props["password"], "")
        self.assertEqual(props["driver"], "com.mysql.cj.jdbc.Driver")
        self.assertEqual(props["batchsize"], "1000")
        self.assertEqual(props["connectTimeout"], "30000")
        self.assertEqual(props["socketTimeout"], "600000")
        self.assertEqual(props["rewriteBatchedStatements"], "true")
        self.assertEqual(props["tcpKeepAlive"], "true")

    def test_build_jdbc_properties_env_password(self):
        """Deve resolver variável de ambiente na senha."""
        import os
        os.environ["TEST_DB_PASS"] = "secret123"
        db = {"password": "${TEST_DB_PASS}"}
        props = build_jdbc_properties(db)
        self.assertEqual(props["password"], "secret123")
        del os.environ["TEST_DB_PASS"]


@unittest.skipUnless(HAS_PYSPARK, "pyspark não está instalado")
class TestWriteJdbcWithRetry(unittest.TestCase):
    """Testes para o wrapper de retry JDBC."""

    def test_write_success_first_try(self):
        from unittest.mock import MagicMock, patch
        mock_df = MagicMock()
        
        write_jdbc_with_retry(mock_df, "url", "tb", "overwrite", {})
        mock_df.write.jdbc.assert_called_once_with(url="url", table="tb", mode="overwrite", properties={})

    def test_write_transient_failure_then_success(self):
        from unittest.mock import MagicMock, patch
        from py4j.protocol import Py4JJavaError
        mock_df = MagicMock()
        
        # O mock lança erro transient 2 vezes, depois sucesso
        mock_df.write.jdbc.side_effect = [
            Exception("Communications link failure..."),
            Exception("Connection reset..."),
            None
        ]
        
        with patch("time.sleep") as mock_sleep:
            write_jdbc_with_retry(mock_df, "url", "tb", "overwrite", {})
            
            self.assertEqual(mock_df.write.jdbc.call_count, 3)
            self.assertEqual(mock_sleep.call_count, 2)
            mock_sleep.assert_any_call(5) # wait backoff_seconds * 1
            mock_sleep.assert_any_call(10) # wait backoff_seconds * 2

    def test_write_transient_failure_exhausted(self):
        from unittest.mock import MagicMock, patch
        mock_df = MagicMock()
        
        # O mock sempre lança erro
        mock_df.write.jdbc.side_effect = Exception("Communications link failure...")
        
        with patch("time.sleep") as mock_sleep:
            with self.assertRaises(Exception):
                write_jdbc_with_retry(mock_df, "url", "tb", "overwrite", {}, max_attempts=2)
                
            self.assertEqual(mock_df.write.jdbc.call_count, 2)
            self.assertEqual(mock_sleep.call_count, 1)

    def test_write_non_transient_failure(self):
        from unittest.mock import MagicMock
        mock_df = MagicMock()
        
        # Erro não-transiente falha imediatamente
        mock_df.write.jdbc.side_effect = Exception("Access denied for user...")
        
        with self.assertRaises(Exception):
            write_jdbc_with_retry(mock_df, "url", "tb", "overwrite", {})
            
        self.assertEqual(mock_df.write.jdbc.call_count, 1)


@unittest.skipUnless(HAS_PYSPARK, "pyspark não está instalado")
class TestPipelineProgress(unittest.TestCase):
    """Testes para o indicador de progresso do pipeline."""

    def test_finish_step_preserves_previous_lines(self):
        """Cada etapa concluída deve gerar uma linha permanente própria,
        sem sobrescrever a mensagem da etapa anterior."""
        import io
        from contextlib import redirect_stdout

        buffer = io.StringIO()
        with redirect_stdout(buffer):
            with PipelineProgress(total=2, refresh_interval=60) as progress:
                progress.start_step("Etapa 1/7: Extração")
                progress.finish_step()
                progress.start_step("Etapa 2/7: Mapeamento")
                progress.finish_step()
        output = buffer.getvalue()

        self.assertIn("✔ Etapa 1/7: Extração — concluída em", output)
        self.assertIn("✔ Etapa 2/7: Mapeamento — concluída em", output)
        # Cada etapa em sua própria linha (histórico preservado)
        lines = [l for l in output.splitlines() if l.startswith("✔")]
        self.assertEqual(len(lines), 2)


@unittest.skipUnless(HAS_PYSPARK, "pyspark não está instalado")
class TestExecutionSummary(unittest.TestCase):
    """Testes para o resumo de execução em pt_BR."""

    def test_format_int_ptbr(self):
        """Deve usar ponto como separador de milhar (padrão pt_BR)."""
        self.assertEqual(_format_int_ptbr(0), "0")
        self.assertEqual(_format_int_ptbr(999), "999")
        self.assertEqual(_format_int_ptbr(1000), "1.000")
        self.assertEqual(_format_int_ptbr(1234567), "1.234.567")

    def test_format_duration_seconds_only(self):
        """Durações curtas devem exibir apenas segundos."""
        self.assertEqual(_format_duration_ptbr(0), "0s")
        self.assertEqual(_format_duration_ptbr(45.4), "45s")

    def test_format_duration_minutes(self):
        """Durações de minutos devem exibir min e s."""
        self.assertEqual(_format_duration_ptbr(316), "5min 16s")
        self.assertEqual(_format_duration_ptbr(60), "1min 00s")

    def test_format_duration_hours(self):
        """Durações longas devem exibir h, min e s."""
        self.assertEqual(_format_duration_ptbr(3661), "1h 01min 01s")

    def test_print_summary_output(self):
        """O resumo deve conter linhas lidas, carregadas, rejeitadas e tempo."""
        import io
        from contextlib import redirect_stdout

        buffer = io.StringIO()
        with redirect_stdout(buffer):
            print_summary(
                raw_count=125000,
                loaded_count=118500,
                rejected_count=6000,
                duplicates=500,
                elapsed_seconds=316,
            )
        output = buffer.getvalue()

        self.assertIn("Resumo da Execução", output)
        self.assertIn("Linhas lidas:           125.000", output)
        self.assertIn("Linhas carregadas:      118.500", output)
        self.assertIn("Linhas rejeitadas:      6.000", output)
        self.assertIn("Duplicatas descartadas: 500", output)
        self.assertIn("Tempo total decorrido:  5min 16s", output)

    def test_print_summary_ignores_log_level(self):
        """O resumo deve aparecer mesmo com logging em nível WARN."""
        import io
        import logging
        from contextlib import redirect_stdout

        logging.getLogger().setLevel(logging.WARNING)
        buffer = io.StringIO()
        with redirect_stdout(buffer):
            print_summary(1, 1, 0, 0, 1.0)

        self.assertIn("Resumo da Execução", buffer.getvalue())


@unittest.skipUnless(HAS_PYSPARK, "pyspark não está instalado")
class TestWriteJdbcRetryTimeout(unittest.TestCase):
    """Teste de retry para timeout de socket (erro real observado)."""

    def test_write_socket_timeout_is_transient(self):
        """Erros de timeout de socket / conexão fechada devem acionar o retry."""
        from unittest.mock import MagicMock, patch
        mock_df = MagicMock()

        # Reproduz o erro real: conexão fechada após SocketTimeoutException
        mock_df.write.jdbc.side_effect = [
            Exception("No operations allowed after connection closed."),
            Exception("java.net.SocketTimeoutException: Read timed out"),
            None,
        ]

        with patch("time.sleep") as mock_sleep:
            write_jdbc_with_retry(mock_df, "url", "tb", "overwrite", {})

            self.assertEqual(mock_df.write.jdbc.call_count, 3)
            self.assertEqual(mock_sleep.call_count, 2)


if __name__ == "__main__":
    unittest.main()
