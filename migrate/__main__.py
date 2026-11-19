import sys

# Checked here, before anything that needs it is imported: on an older
# Python the tool otherwise stops part way with an import error.
if sys.version_info < (3, 11):
    sys.exit(f"python -m migrate needs Python 3.11 or later, and this is Python {sys.version.split()[0]}. "
             "Make a virtual environment with a newer one, for example: python3.12 -m venv .venv && "
             ".venv/bin/pip install -r migrate/requirements.txt, then run .venv/bin/python -m migrate ...")

from .cli import main  # noqa: E402

main()
