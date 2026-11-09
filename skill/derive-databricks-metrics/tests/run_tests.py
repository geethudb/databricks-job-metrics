#!/usr/bin/env python3
"""Zero-dependency test runner (works without pytest).

    python3 tests/run_tests.py

Discovers every `test_*` function in `tests/test_*.py`, runs it, and exits
non-zero on any failure. If pytest is installed, `pytest tests/` also works.
"""
import importlib.util
import sys
import traceback
from pathlib import Path

HERE = Path(__file__).resolve().parent


def main():
    passed = failed = 0
    for test_file in sorted(HERE.glob("test_*.py")):
        spec = importlib.util.spec_from_file_location(test_file.stem, test_file)
        module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(module)
        for name in [n for n in dir(module) if n.startswith("test_")]:
            try:
                getattr(module, name)()
                print(f"PASS {test_file.name}::{name}")
                passed += 1
            except Exception:
                print(f"FAIL {test_file.name}::{name}")
                traceback.print_exc()
                failed += 1
    print(f"\n{passed} passed, {failed} failed")
    return 1 if failed else 0


if __name__ == "__main__":
    sys.exit(main())
