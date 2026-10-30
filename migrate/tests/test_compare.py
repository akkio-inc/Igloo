import datetime as dt
import decimal
import unittest

from migrate import compare
from migrate.igloo import error_class, error_pattern

D = decimal.Decimal


class CanonTest(unittest.TestCase):
    def test_numbers(self):
        self.assertEqual(compare.canon(D("5.00")), "5")
        self.assertEqual(compare.canon(5.0), "5")
        self.assertEqual(compare.canon(5), "5")
        self.assertEqual(compare.canon(D("0.050006574")), compare.canon(0.0500065740000001))
        self.assertEqual(compare.canon(True), "true")
        self.assertEqual(compare.canon(float("nan")), "nan")
        self.assertEqual(compare.canon(D("0.0000001")), compare.canon(1e-7))

    def test_integers_and_decimals_are_exact(self):
        for a, b in [(D("1000000000.01"), D("1000000000.02")), (D("123456789012.50"), D("123456789019.50")),
                     (12345678901, 12345678902)]:
            self.assertNotEqual(compare.canon(a), compare.canon(b))
        self.assertEqual(compare.canon(D("123456789012.50")), "123456789012.5")
        # A double's last digits vary with the order rows were added in.
        self.assertEqual(compare.canon(1000000000.0000002), compare.canon(1000000000))
        self.assertEqual(compare.canon(1234.567890123), compare.canon(1234.567890119))

    def test_times(self):
        aware = dt.datetime(2026, 1, 1, 12, 0, tzinfo=dt.timezone(dt.timedelta(hours=2)))
        self.assertEqual(compare.canon(aware), "2026-01-01T10:00:00Z")
        self.assertEqual(compare.canon(dt.datetime(2026, 1, 1, 10)), "2026-01-01T10:00:00")
        self.assertEqual(compare.canon(dt.date(2026, 1, 1)), "2026-01-01")

    def test_json(self):
        self.assertEqual(compare.canon('{"b": 1, "a": [1.50, "x"]}', as_json=True),
                         compare.canon({"a": [D("1.5"), "x"], "b": 1}))
        self.assertEqual(compare.canon(None), "\\N")


class DigestTest(unittest.TestCase):
    def test_order_does_not_matter_but_duplicates_do(self):
        a = compare.digest([(1, "x"), (2, "y"), (2, "y")], ["A", "B"])
        b = compare.digest([(2, "y"), (1, "x"), (2, "y")], ["a", "b"])
        c = compare.digest([(1, "x"), (2, "y")], ["a", "b"])
        self.assertEqual(a.checksum, b.checksum)
        self.assertEqual(a.columns, ["a", "b"])
        self.assertNotEqual(a.checksum, c.checksum)
        self.assertEqual(compare.verdict(a, 3, b.checksum), "match")
        self.assertEqual(compare.verdict(a, 2, c.checksum), "rows differ")
        self.assertEqual(compare.verdict(a, 3, "0" * 16), "values differ")
        self.assertEqual(compare.verdict(a, 3, None), "rows match")
        self.assertEqual(compare.verdict(None, 3, None), "not compared")

    def test_values_do_not_read_as_others(self):
        def same(a, b):
            return compare.digest([a], ["x"] * len(a)).checksum == compare.digest([b], ["x"] * len(b)).checksum

        self.assertFalse(same((None,), ("\\N",)))
        self.assertFalse(same(("a\x1fb", "c"), ("a", "b\x1fc")))
        self.assertFalse(same((5,), ("5",)))
        self.assertFalse(same((True,), ("true",)))
        self.assertFalse(same(({"a": 5},), ({"a": "5"},)))
        self.assertTrue(same((5, None), (D("5.00"), None)))

    def test_numbers_exact_unless_floating_point(self):
        def verdict(a, b):
            da, db = compare.digest([a], ["x"] * len(a)), compare.digest([b], ["x"] * len(b))
            return compare.verdict(da, 1, db.checksum, db.loose)

        self.assertEqual(verdict((D("1000000000.01"),), (D("1000000000.02"),)), "values differ")
        self.assertEqual(verdict((D("123456789012.50"),), (D("123456789019.50"),)), "values differ")
        self.assertEqual(verdict((1000000000,), (1000000001,)), "values differ")
        self.assertEqual(verdict((12345678901,), (12345678902,)), "values differ")
        self.assertEqual(verdict((D("123456789012.50"),), (D("123456789012.5"),)), "match")
        # Snowflake's AVG of a NUMBER(12,2) has 8 decimal places, DuckDB's is
        # a double: they agree to 10 significant digits. Two decimals in
        # another column must still be equal.
        self.assertEqual(verdict((1234.567890123, "a"), (D("1234.56789012"), "a")), "match")
        self.assertEqual(verdict((D("1234.56789012"), 1.5), (1234.567890123, D("1.50"))), "match")
        self.assertEqual(verdict((1234.567890123, D("12345678901.5")), (D("1234.56789012"), D("12345678901.6"))),
                         "values differ")

    def test_large_results_are_not_kept(self):
        d = compare.digest(((i,) for i in range(10)), ["n"], keep=5)
        self.assertEqual(d.rows, 10)
        self.assertIsNone(d.values)
        self.assertEqual(len(compare.digest([(1,)], ["n"]).values), 1)


class TolerantTest(unittest.TestCase):
    def test_precision(self):
        # Snowflake's AVG of a NUMBER(12,2) has 8 decimals; DuckDB's is a double.
        self.assertTrue(compare.close(D("0.05000657"), 0.0500065745))
        self.assertFalse(compare.close(D("0.05000657"), 0.0500075))
        self.assertTrue(compare.close(1e12 + 0.0001, 1e12))
        self.assertFalse(compare.close(D("10.25"), D("10.26")))
        self.assertFalse(compare.close(D("0.05"), 0.06))
        self.assertTrue(compare.close(D("0.05"), 0.0500065745))
        self.assertFalse(compare.close(2, 2.5), "integer division is a difference")
        self.assertTrue(compare.close(2, 2.0))
        self.assertTrue(compare.close("a", "a"))
        self.assertFalse(compare.close("a", "b"))

    def test_integers_and_decimals_exactly(self):
        self.assertFalse(compare.close(1000000000, 1000000001))
        self.assertFalse(compare.close(D("1000000000.01"), D("1000000000.02")))
        self.assertFalse(compare.close(D("123456789012.50"), D("123456789019.50")))
        self.assertTrue(compare.close(1000000000, 1000000000.0000002), "a double's last digits")
        self.assertFalse(compare.close(None, "\\N"))
        self.assertFalse(compare.tolerant([(1000000000,)], [(1000000001,)]).equal)

    def test_rows(self):
        a = [("N", "O", D("25.49807587")), ("A", "F", D("25.5009751"))]
        b = [("A", "F", 25.500975103), ("N", "O", 25.498075869)]
        self.assertTrue(compare.tolerant(a, b).equal)
        t = compare.tolerant([("s1", "addr 1"), ("s2", "addr 2")], [("s1", "other"), ("s2", "addr 2")])
        self.assertEqual((t.equal, t.differing_rows, t.differing_columns), (False, 1, [1]))
        self.assertFalse(compare.tolerant([(1,)], [(1,), (2,)]).equal)


class ErrorClassTest(unittest.TestCase):
    def test_classes(self):
        cases = [
            ("42P01", 'Catalog Error: Scalar Function with name iff does not exist!\nDid you mean "if"?',
             "unknown function"),
            ("42P01", 'Catalog Error: Table Function with name "flatten" is not in the catalog', "unknown function"),
            ("42P01", "Catalog Error: Table with name orders does not exist!", "unknown table"),
            ("42P01", "Catalog Error: Type with name NUMBER does not exist!", "unknown type"),
            ("42703", 'Binder Error: Referenced column "YEAR" not found in FROM clause!', "unknown column"),
            ("42601", 'Parser Error: syntax error at or near "QUALIFY"', "syntax"),
            ("22P02", "Conversion Error: Could not convert string 'x' to INT32", "conversion"),
            ("57014", "canceling statement due to statement timeout", "timeout"),
            ("42501", "permission denied for ana: SELECT on table public.orders", "permission denied"),
            ("XX000", "INTERNAL Error: Attempted to access index 3 within vector of size 3", "internal error"),
            (None, "Binder Error: No function matches the given name and argument types 'f(INTEGER)'",
             "type mismatch"),
            (None, "server closed the connection unexpectedly", "connection lost"),
            (None, "something new", "other"),
        ]
        for state, message, cls in cases:
            with self.subTest(message):
                self.assertEqual(error_class(state, message), cls)

    def test_patterns(self):
        self.assertEqual(error_pattern("unknown function", "Catalog Error: Scalar Function with name iff does "
                                                           "not exist!\nDid you mean \"if\"?"),
                         "function iff does not exist")
        self.assertEqual(error_pattern("unknown column", 'Binder Error: Referenced column "ACCT_42" not found '
                                                         "in FROM clause! Candidate bindings: \"a\""),
                         'Binder Error: Referenced column "…" not found in FROM clause! Candidate bindings: "…"')
        self.assertEqual(error_pattern("conversion", "Could not convert string 'ann@example.com' to INT32"),
                         "Could not convert string '?' to INT32")


if __name__ == "__main__":
    unittest.main()
