"""TLS plumbing: empty CA = plaintext exactly as before; bad CA fails fast."""
import ssl

import pytest

from app.database.engine import _ssl_context


def test_empty_ca_file_means_no_ssl_context():
    assert _ssl_context("") is None


def test_invalid_ca_bundle_fails_at_startup(tmp_path):
    bad = tmp_path / "ca.pem"
    bad.write_text("not a certificate")
    with pytest.raises(ssl.SSLError):
        _ssl_context(str(bad))
