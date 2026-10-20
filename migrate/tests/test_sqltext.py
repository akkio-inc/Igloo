import unittest

from migrate import sqltext


class NormalizeTest(unittest.TestCase):
    def test_literals_case_and_comments_do_not_matter(self):
        a = sqltext.normalize("SELECT region, SUM(amount) FROM orders WHERE d >= '2026-01-01' /* x */ GROUP BY 1",
                              "snowflake")
        b = sqltext.normalize("select REGION, sum(AMOUNT)\n  from ORDERS where D >= '2025-06-30' group by 7;",
                              "snowflake")
        self.assertEqual(a, b)
        self.assertNotIn("2026", a)

    def test_structure_matters(self):
        a = sqltext.normalize("SELECT a FROM t WHERE x = 1", "snowflake")
        b = sqltext.normalize("SELECT a FROM t WHERE y = 1", "snowflake")
        self.assertNotEqual(a, b)

    def test_quoted_names_keep_their_case(self):
        self.assertNotEqual(sqltext.normalize('SELECT "Amount" FROM t', "snowflake"),
                            sqltext.normalize('SELECT "AMOUNT" FROM t', "snowflake"))

    def test_lists_of_values_collapse(self):
        self.assertEqual(sqltext.normalize("SELECT * FROM t WHERE id IN (1, 2, 3)", "snowflake"),
                         sqltext.normalize("SELECT * FROM t WHERE id IN (4)", "snowflake"))
        self.assertEqual(sqltext.normalize("INSERT INTO t VALUES (1, 'a'), (2, 'b'), (3, 'c')", "snowflake"),
                         sqltext.normalize("INSERT INTO t VALUES (9, 'z'), (8, 'y')", "snowflake"))

    def test_untokenizable_text_still_has_a_shape(self):
        a = sqltext.normalize("SELECT 'unterminated FROM t WHERE x = 5", "snowflake")
        self.assertTrue(a)
        self.assertNotIn("5", a)

    def test_hash_is_stable(self):
        self.assertEqual(sqltext.shape_hash("select ?"), sqltext.shape_hash("select ?"))
        self.assertEqual(len(sqltext.shape_hash("select ?")), 16)


class RedactTest(unittest.TestCase):
    def test_literals_and_comments_go(self):
        sql = "SELECT email, 42 /* client acme */ FROM users WHERE email = 'ann@example.com' -- note\n AND id > 1001"
        out = sqltext.redact(sql, "snowflake")
        for secret in ("ann@example.com", "42", "1001", "acme", "note"):
            self.assertNotIn(secret, out)
        self.assertIn("SELECT email, ?", out)
        self.assertIn("FROM users WHERE email = ?", out)

    def test_dollar_strings_and_national_strings(self):
        out = sqltext.redact("SELECT $$secret$$, N'other' FROM t", "snowflake")
        self.assertNotIn("secret", out)
        self.assertNotIn("other", out)

    def test_databricks_backticks_are_names_not_literals(self):
        out = sqltext.redact("SELECT `user id` FROM t WHERE name = 'bo'", "databricks")
        self.assertIn("`user id`", out)
        self.assertNotIn("bo", out.replace("`user id`", ""))

    def test_fallback_for_text_the_tokenizer_rejects(self):
        out = sqltext.redact("SELECT 'a''b' , 77 FROM t WHERE x = 'unterminated", "snowflake")
        self.assertNotIn("77", out)

    def test_error_messages(self):
        msg = ("Conversion Error: Could not convert string 'ann@example.com' to INT32\n\n"
               "LINE 1: SELECT CAST('ann@example.com' AS INTEGER)\n               ^")
        out = sqltext.redact_message(msg)
        self.assertNotIn("ann@example.com", out)
        self.assertNotIn("LINE 1", out)
        self.assertEqual(out, "Conversion Error: Could not convert string '?' to INT32")
        self.assertIsNone(sqltext.redact_message(None))


class VolatilityTest(unittest.TestCase):
    def test_flags(self):
        self.assertEqual(sqltext.volatility("SELECT * FROM t WHERE d > CURRENT_DATE - 7", "snowflake"), {"time"})
        self.assertEqual(sqltext.volatility("SELECT * FROM t ORDER BY RANDOM() LIMIT 5", "snowflake"), {"random"})
        self.assertEqual(sqltext.volatility("SELECT CURRENT_ROLE()", "snowflake"), {"session"})
        self.assertEqual(sqltext.volatility("SELECT 'CURRENT_DATE' FROM t", "snowflake"), set())
        self.assertEqual(sqltext.volatility("SELECT a FROM t", "snowflake"), set())


class RenameTest(unittest.TestCase):
    def test_qualified_names_and_their_column_references(self):
        sql = "SELECT shop.sales.orders.id, o.x FROM SHOP.SALES.ORDERS o JOIN shop.sales.orders USING (id)"
        out = sqltext.rename_tables(sql, "snowflake", {("shop", "sales", "orders"): "lake.sales.orders"})
        self.assertEqual(out, "SELECT lake.sales.orders.id, o.x FROM lake.sales.orders o JOIN lake.sales.orders "
                              "USING (id)")

    def test_one_part_names_but_not_function_calls(self):
        sql = "SELECT orders.id, orders(1) FROM orders"
        out = sqltext.rename_tables(sql, "snowflake", {("orders",): "lake.s.orders_v2"})
        self.assertEqual(out, "SELECT lake.s.orders_v2.id, orders(1) FROM lake.s.orders_v2")

    def test_insert_with_a_column_list(self):
        out = sqltext.rename_tables("INSERT INTO nation (a, b) SELECT 1, 2", "snowflake",
                                    {("nation",): "lake.scratch.nation__x"})
        self.assertEqual(out, "INSERT INTO lake.scratch.nation__x (a, b) SELECT 1, 2")

    def test_quoted_names_and_literals_are_left_alone(self):
        sql = """SELECT 'SALES.ORDERS' FROM "SALES"."ORDERS" -- SALES.ORDERS\n"""
        out = sqltext.rename_tables(sql, "snowflake", {("sales", "orders"): "lake.sales.orders"})
        self.assertEqual(out, "SELECT 'SALES.ORDERS' FROM lake.sales.orders -- SALES.ORDERS\n")

    def test_nothing_to_rename(self):
        self.assertEqual(sqltext.rename_tables("SELECT 1", "snowflake", {}), "SELECT 1")


if __name__ == "__main__":
    unittest.main()
