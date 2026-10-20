"""Unit tests: python -m unittest discover -s migrate/tests -t . (from the
repository root). They need only sqlglot, and no warehouse or Igloo."""

import json
import pathlib

FIXTURES = pathlib.Path(__file__).parent / "fixtures"


def fixture(name: str):
    return json.loads((FIXTURES / name).read_text())
