import unittest

from migrate import statements


class ClassifyTest(unittest.TestCase):
    def test_source_types(self):
        cases = {
            "SELECT": "SELECT", "CREATE_TABLE_AS_SELECT": "CTAS", "INSERT": "INSERT", "MULTI_TABLE_INSERT": "INSERT",
            "MERGE": "MERGE", "UPDATE": "UPDATE", "DELETE": "DELETE", "CREATE_TABLE": "DDL", "ALTER_TABLE": "DDL",
            "DROP": "DDL", "TRUNCATE_TABLE": "DDL", "SHOW": "other", "ALTER_SESSION": "other", "GRANT": "other",
            "CALL": "other", "ALTER_WAREHOUSE_SUSPEND": "other", "USE": "other",
        }
        for source_type, cls in cases.items():
            with self.subTest(source_type):
                self.assertEqual(statements.classify(source_type, "SELECT 1", "snowflake"), cls)

    def test_ambiguous_types_are_read_from_the_text(self):
        self.assertEqual(statements.classify("CREATE", "CREATE TABLE a.b AS SELECT * FROM c", "databricks"), "CTAS")
        self.assertEqual(statements.classify("CREATE", "CREATE TABLE a.b (x INT)", "databricks"), "DDL")
        self.assertEqual(statements.classify("CREATE", "CREATE VIEW v AS SELECT 1", "databricks"), "DDL")
        self.assertEqual(statements.classify(None, "WITH x AS (SELECT 1) SELECT * FROM x", "snowflake"), "SELECT")
        self.assertEqual(statements.classify("", "-- hi\nmerge into t using u on t.a = u.a when matched then delete",
                                             "snowflake"), "MERGE")

    def test_unparseable_text_falls_back_to_words(self):
        self.assertEqual(statements.classify(None, "SELECT FROM WHERE (", "snowflake"), "SELECT")
        self.assertEqual(statements.classify(None, "", "snowflake"), "other")


class TablesTest(unittest.TestCase):
    def refs(self, sql, dialect="snowflake", db="SHOP", schema="SALES"):
        return statements.tables(statements.parse(sql, dialect), dialect, db, schema)

    def test_resolution_against_the_session(self):
        t = self.refs('SELECT * FROM orders o JOIN ref.regions r ON 1 = 1 JOIN other.ref."Mixed" m ON 1 = 1')
        self.assertEqual([r.name for r in t.reads], ["SHOP.SALES.ORDERS", "SHOP.REF.REGIONS", "OTHER.REF.Mixed"])
        self.assertEqual(t.reads[0].written, ("orders",))
        self.assertEqual(t.reads[0].key, ("shop", "sales", "orders"))
        self.assertTrue(all(r.resolved for r in t.reads))

    def test_ctes_are_not_tables(self):
        t = self.refs("WITH recent AS (SELECT * FROM orders) SELECT * FROM recent")
        self.assertEqual([r.name for r in t.reads], ["SHOP.SALES.ORDERS"])

    def test_write_targets(self):
        cases = [
            ("INSERT INTO orders SELECT * FROM staging.orders", True),
            ("MERGE INTO orders o USING staging.orders s ON o.id = s.id WHEN MATCHED THEN DELETE", True),
            ("UPDATE orders SET x = 1", True),
            ("DELETE FROM orders WHERE x = 1", True),
            ("CREATE OR REPLACE TABLE orders AS SELECT * FROM staging.orders", False),
        ]
        for sql, must_exist in cases:
            with self.subTest(sql):
                t = self.refs(sql)
                self.assertEqual([w.name for w in t.writes], ["SHOP.SALES.ORDERS"])
                self.assertEqual(t.target_must_exist, must_exist)
                self.assertNotIn("SHOP.SALES.ORDERS", [r.name for r in t.reads if r.written == ("orders",)])

    def test_unknown_session_leaves_names_unresolved(self):
        t = self.refs("SELECT * FROM orders", db=None, schema=None)
        self.assertFalse(t.reads[0].resolved)

    def test_stages_and_metadata(self):
        t = self.refs("SELECT $1 FROM @landing/2026")
        self.assertEqual(t.reads[0].kind, "stage")
        t = self.refs("SELECT * FROM TABLE(INFORMATION_SCHEMA.QUERY_HISTORY())")
        self.assertEqual([r.kind for r in t.reads], ["system"])
        t = self.refs("SELECT * FROM SNOWFLAKE.ACCOUNT_USAGE.QUERY_HISTORY")
        self.assertEqual([r.kind for r in t.reads], ["system"])

    def test_databricks_names_are_case_insensitive(self):
        t = self.refs("SELECT * FROM Media.Impressions", "databricks", "main", "default")
        self.assertEqual(t.reads[0].name, "main.media.impressions")
        t = self.refs("SELECT * FROM system.billing.usage", "databricks", "main", "default")
        self.assertEqual(t.reads[0].kind, "system")


class ReadOnlyTest(unittest.TestCase):
    def test_queries_pass(self):
        for sql in ("SELECT 1", "WITH a AS (SELECT 1) SELECT * FROM a", "SELECT 1 UNION ALL SELECT 2",
                    "(SELECT a FROM t) EXCEPT (SELECT a FROM u)"):
            with self.subTest(sql):
                self.assertIsNone(statements.read_only_problem(sql, "snowflake"))

    def test_anything_else_is_refused(self):
        for sql in ("INSERT INTO t SELECT 1", "DELETE FROM t", "CREATE TABLE t AS SELECT 1", "SELECT 1; DROP TABLE t",
                    "SELECT a INTO b FROM c", "SELECT SYSTEM$CANCEL_ALL_QUERIES(1)", "SELECT seq.nextval",
                    "CALL refresh()", "ALTER SESSION SET QUERY_TAG = 'x'", "GRANT SELECT ON t TO ROLE r",
                    "USE SCHEMA s", "SELECT FROM (", ""):
            with self.subTest(sql):
                self.assertIsNotNone(statements.read_only_problem(sql, "snowflake"))
        self.assertIsNotNone(statements.read_only_problem("SELECT http_request('x')", "databricks"))
        self.assertIsNotNone(statements.read_only_problem("SELECT ai_query('m', 'p')", "databricks"))

    def test_words_check_for_text_sqlglot_cannot_parse(self):
        self.assertIsNone(statements.read_only_words("SELECT a FROM t WHERE x = 'INSERT'", "snowflake"))
        self.assertIsNotNone(statements.read_only_words("SELECT 1 INTO t", "snowflake"))
        self.assertIsNotNone(statements.read_only_words("DELETE FROM t", "snowflake"))
        self.assertIsNotNone(statements.read_only_words("SELECT SYSTEM$WAIT(1)", "snowflake"))

    def test_writes_stay_in_the_scratch_schema(self):
        scratch = ("lake", "scratch")
        self.assertIsNone(statements.write_target_problem("INSERT INTO lake.scratch.t SELECT * FROM lake.s.u",
                                                          "duckdb", scratch))
        self.assertIsNone(statements.write_target_problem(
            "MERGE INTO lake.scratch.t x USING lake.s.u y ON x.a = y.a WHEN MATCHED THEN UPDATE SET b = y.b "
            "WHEN NOT MATCHED THEN INSERT VALUES (y.a, y.b)", "duckdb", scratch))
        self.assertIsNotNone(statements.write_target_problem("INSERT INTO lake.s.u SELECT 1", "duckdb", scratch))
        self.assertIsNotNone(statements.write_target_problem("INSERT INTO u SELECT 1", "duckdb", scratch))
        self.assertIsNotNone(statements.write_target_problem("SELECT 1", "duckdb", scratch))


class TranslateTest(unittest.TestCase):
    def test_snowflake_to_duckdb(self):
        t = statements.translate("SELECT IFF(a > 1, 'x', 'y'), DATE_PART(YEAR, d) FROM t", "snowflake")
        self.assertIsNone(t.error)
        self.assertIn("CASE WHEN", t.sql)
        self.assertEqual(t.warnings, [])

    def test_imperfect_translations_say_so(self):
        t = statements.translate("SELECT TO_NUMBER(SUBSTR(MD5(a), 1, 15), 'XXXXXXXXXXXXXXX') FROM t", "snowflake")
        self.assertIsNotNone(t.sql)
        self.assertTrue(t.warnings)

    def test_what_sqlglot_cannot_read(self):
        self.assertIsNotNone(statements.translate("SELEC 1 FROM", "snowflake").error)
        self.assertIsNotNone(statements.translate("ALTER WAREHOUSE w SUSPEND", "snowflake").error)

    def test_databricks_to_duckdb(self):
        t = statements.translate("SELECT nvl(a, 0), a RLIKE '^x' FROM `c`.`s`.`t`", "databricks")
        self.assertIn("COALESCE", t.sql)
        self.assertIn('"c"."s"."t"', t.sql)


if __name__ == "__main__":
    unittest.main()
