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
            self.assertIn(10, df_result["AG_ID"].values)

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


if __name__ == "__main__":
    unittest.main()
