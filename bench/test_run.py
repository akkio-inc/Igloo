import contextlib
import io
import json
import os
import pathlib
import sys
import tempfile
import types
import unittest
from unittest import mock

import run


class FakeCursor:
    def __init__(self, conn):
        self.conn = conn

    def execute(self, sql, params=None):
        self.conn.log.append(sql)

    def fetchone(self):
        return (self.conn.result_cache,)

    def fetchall(self):
        return []

    def close(self):
        pass


class FakeConn:
    """An Igloo whose SHOW igloo_result_cache answers result_cache."""

    def __init__(self, result_cache):
        self.result_cache = result_cache
        self.log = []

    def cursor(self):
        return FakeCursor(self)

    def close(self):
        pass


class IglooTest(unittest.TestCase):
    def run_igloo(self, result_cache):
        conn = FakeConn(result_cache)
        psycopg = types.SimpleNamespace(connect=lambda dsn, autocommit: conn)
        with tempfile.TemporaryDirectory() as d, mock.patch.dict(sys.modules, {"psycopg": psycopg}), \
                mock.patch.dict(os.environ, {"IGLOO_DSN": "postgresql://igloo"}), \
                mock.patch.object(sys, "argv", ["run.py", "--platform", "igloo", "--size", "c7g.4xlarge",
                                                "--scale", "1", "--runs", "1", "--out", f"{d}/out.json"]), \
                contextlib.redirect_stdout(io.StringIO()):
            run.main()
            return conn.log, json.loads(pathlib.Path(d, "out.json").read_text())

    def test_result_cache_off(self):
        log, out = self.run_igloo("off")
        self.assertEqual(log[:2], ["SET igloo_result_cache = off", "SHOW igloo_result_cache"])
        self.assertEqual(out["result_cache"], "off")
        self.assertEqual(len(out["queries"]), 22)

    def test_refuses_to_time_the_result_cache(self):
        with self.assertRaises(SystemExit):
            self.run_igloo("on")


if __name__ == "__main__":
    unittest.main()
