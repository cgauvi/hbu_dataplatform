"""What every HTTP client in the platform sends and trusts.

Two things the source clients share and none of them owns: the identity they
announce, and the certificate bundle they verify against on a laptop that
sits behind a TLS-inspecting proxy.
"""

from __future__ import annotations

import os
from pathlib import Path

#: Sent by every client in the project. Not decoration: donnees.montreal.ca
#: answers 403 to the `python-requests/x.y` default that `requests` would
#: otherwise send, and naming the caller is the polite thing to do on public
#: servers that owe this pipeline nothing.
USER_AGENT = "urban-rag/0.1.0 (Dagster pipeline)"


def default_ca_bundle() -> str | None:
    """CA bundle to trust, for laptops behind a TLS-inspecting proxy.

    ``requests`` verifies against ``certifi`` and reads only
    ``REQUESTS_CA_BUNDLE``/``CURL_CA_BUNDLE``, while managed machines usually
    advertise their corporate root through ``SSL_CERT_FILE`` instead. Without
    this, every call fails with a certificate-verify error.
    """
    for variable in ("REQUESTS_CA_BUNDLE", "CURL_CA_BUNDLE", "SSL_CERT_FILE"):
        value = os.environ.get(variable)
        if value and Path(value).exists():
            return value
    return None
