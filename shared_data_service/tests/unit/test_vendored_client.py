"""The vendored SimpleRabbit copy must never drift from the repo-root
canonical file, and the adapter must import successfully without the root
on sys.path (fresh checkout / CI / Docker)."""
from pathlib import Path

import pytest

SERVICE_DIR = Path(__file__).resolve().parents[2]
VENDORED = SERVICE_DIR / "app" / "messaging" / "_vendored_simple_rabbit.py"
CANONICAL = SERVICE_DIR.parent / "simple_rabbit.py"


def test_vendored_copy_matches_canonical():
    if not CANONICAL.exists():
        pytest.skip("standalone checkout: no canonical copy to compare")
    assert VENDORED.read_bytes() == CANONICAL.read_bytes(), (
        "app/messaging/_vendored_simple_rabbit.py has drifted from the repo-root "
        "simple_rabbit.py — re-copy it (cp ../simple_rabbit.py "
        "app/messaging/_vendored_simple_rabbit.py)"
    )


def test_vendored_copy_is_importable():
    from app.messaging._vendored_simple_rabbit import SimpleRabbit  # noqa: F401


def test_import_fallback_actually_works():
    # In the monorepo the try-branch always wins, so without this test a
    # typo in the fallback line (or a packaging omission) would pass the
    # whole suite and only explode at standalone startup. Poisoning
    # sys.modules makes `import simple_rabbit` raise ImportError, forcing
    # the except branch on reload.
    import importlib
    import sys

    import app.messaging.simple_client as sc
    from app.messaging import _vendored_simple_rabbit as vendored

    had_root = "simple_rabbit" in sys.modules
    original = sys.modules.get("simple_rabbit")
    sys.modules["simple_rabbit"] = None  # None entry → ImportError on import
    try:
        importlib.reload(sc)
        assert sc.SimpleRabbit is vendored.SimpleRabbit
        assert sc.ConsumerCancelledError is vendored.ConsumerCancelledError
    finally:
        if had_root and original is not None:
            sys.modules["simple_rabbit"] = original
        else:
            sys.modules.pop("simple_rabbit", None)
        importlib.reload(sc)  # restore the normal (root-preferring) state
