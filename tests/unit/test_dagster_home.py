from urllib.parse import parse_qs, unquote, urlsplit

import urban_rag.dagster_home as dagster_home
from urban_rag.dagster_home import configure_dagster_home, main, postgres_url_from_env


def test_dagster_home_defaults_to_local_storage(tmp_path):
    env = {}

    path = configure_dagster_home(tmp_path, env)

    assert "DAGSTER_POSTGRES_URL" not in env
    assert path.read_text(encoding="utf-8").endswith("telemetry:\n  enabled: false\n")


def test_postgres_url_uses_dedicated_dagster_schema():
    env = {
        "URBAN_RAG_PG_HOST": "db.example.test",
        "URBAN_RAG_PG_DATABASE": "urban_rag",
        "URBAN_RAG_PG_USER": "urban_rag",
        "URBAN_RAG_PG_PASSWORD": "s/ec:ret",
        "URBAN_RAG_PG_SSLMODE": "require",
    }

    url = postgres_url_from_env(env)

    parts = urlsplit(url)
    query = parse_qs(parts.query)
    assert parts.scheme == "postgresql"
    assert parts.username == "urban_rag"
    assert unquote(parts.password or "") == "s/ec:ret"
    assert parts.hostname == "db.example.test"
    assert parts.path == "/urban_rag"
    assert query["sslmode"] == ["require"]
    assert query["options"] == ["-csearch_path=dagster,public"]


def test_postgres_url_can_use_a_tunnel_without_renaming_the_server():
    env = {
        "URBAN_RAG_PG_HOST": "hbu-dev.example.rds.amazonaws.com",
        "URBAN_RAG_PG_HOSTADDR": "127.0.0.1",
        "URBAN_RAG_PG_PORT": "5433",
        "URBAN_RAG_PG_DATABASE": "urban_rag",
        "URBAN_RAG_PG_USER": "urban_rag",
        "URBAN_RAG_PG_PASSWORD": "secret",
        "URBAN_RAG_PG_SSLMODE": "verify-full",
    }

    url = postgres_url_from_env(env)

    parts = urlsplit(url)
    query = parse_qs(parts.query)
    assert parts.hostname == "hbu-dev.example.rds.amazonaws.com"
    assert parts.port == 5433
    assert query["hostaddr"] == ["127.0.0.1"]
    assert query["sslmode"] == ["verify-full"]


def test_secret_does_not_override_the_configured_host(monkeypatch):
    """The tunnel address is configuration, not something the secret knows.

    The hbu-dev secret stores the address the database answers on in its own
    network. Reached through the SSM tunnel it is a local port instead, so a
    secret that wins over URBAN_RAG_PG_HOST points Dagster's instance storage
    at the wrong server - and at `localhost` specifically, which resolves to
    ::1 before the IPv4 port the tunnel binds.
    """

    def fake_load_secret(secret_id, env):
        return {
            "username": "urban_rag",
            "password": "secret-password",
            "host": "localhost",
            "port": 5432,
            "dbname": "somewhere_else",
        }

    monkeypatch.setattr(dagster_home, "_load_secret", fake_load_secret)

    env = {
        "URBAN_RAG_PG_HOST": "127.0.0.1",
        "URBAN_RAG_PG_PORT": "5433",
        "URBAN_RAG_PG_DATABASE": "urban_rag",
        "URBAN_RAG_PG_USER": "urban_rag",
        "URBAN_RAG_PG_SECRET_ID": "dagster-secret",
        "URBAN_RAG_PG_SSLMODE": "require",
    }

    parts = urlsplit(postgres_url_from_env(env))

    assert parts.hostname == "127.0.0.1"
    assert parts.port == 5433
    assert parts.path == "/urban_rag"
    assert unquote(parts.netloc.split("@")[0]) == "urban_rag:secret-password"


def test_secret_fills_in_what_the_environment_leaves_unset(monkeypatch):
    """Only the host is pinned by the tunnel; the rest still has a fallback."""

    def fake_load_secret(secret_id, env):
        return {
            "username": "from_secret",
            "password": "secret-password",
            "port": 6543,
            "dbname": "from_secret_db",
        }

    monkeypatch.setattr(dagster_home, "_load_secret", fake_load_secret)

    env = {
        "URBAN_RAG_PG_HOST": "127.0.0.1",
        "URBAN_RAG_PG_SECRET_ID": "dagster-secret",
    }

    parts = urlsplit(postgres_url_from_env(env))

    assert parts.port == 6543
    assert parts.path == "/from_secret_db"
    assert unquote(parts.netloc.split("@")[0]) == "from_secret:secret-password"


def test_existing_postgres_url_gets_search_path(tmp_path):
    env = {
        "DAGSTER_POSTGRES_URL": "postgresql://u:p@db.example.test/urban_rag?sslmode=require",
        "DAGSTER_POSTGRES_SCHEMA": "dagster_meta",
    }

    configure_dagster_home(tmp_path, env)

    query = parse_qs(urlsplit(env["DAGSTER_POSTGRES_URL"]).query)
    assert query["sslmode"] == ["require"]
    assert query["options"] == ["-csearch_path=dagster_meta,public"]


def test_main_loads_dotenv_before_loading_postgres_secret(tmp_path, monkeypatch):
    (tmp_path / ".env").write_text(
        "\n".join(
            [
                "AWS_PROFILE=profile-from-dotenv",
                "AWS_DEFAULT_REGION=us-east-1",
                "URBAN_RAG_PG_HOST=db.example.test",
                "URBAN_RAG_PG_SECRET_ID=dagster-secret",
            ]
        ),
        encoding="utf-8",
    )
    monkeypatch.chdir(tmp_path)
    monkeypatch.setenv("DAGSTER_HOME", str(tmp_path / "dagster_home"))
    for name in (
        "AWS_PROFILE",
        "AWS_DEFAULT_REGION",
        "URBAN_RAG_PG_HOST",
        "URBAN_RAG_PG_SECRET_ID",
        "URBAN_RAG_PG_PASSWORD",
        "DAGSTER_POSTGRES_URL",
    ):
        monkeypatch.delenv(name, raising=False)

    seen = {}

    def fake_load_secret(secret_id, env):
        seen["secret_id"] = secret_id
        seen["aws_profile"] = env.get("AWS_PROFILE")
        return {
            "username": "urban_rag",
            "password": "secret-password",
            "host": "db.example.test",
            "dbname": "urban_rag",
        }

    monkeypatch.setattr(dagster_home, "_load_secret", fake_load_secret)

    assert main([]) == 0

    assert seen == {
        "secret_id": "dagster-secret",
        "aws_profile": "profile-from-dotenv",
    }
