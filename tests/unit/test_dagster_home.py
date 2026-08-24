from urllib.parse import parse_qs, unquote, urlsplit

from urban_rag.dagster_home import configure_dagster_home, postgres_url_from_env


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


def test_existing_postgres_url_gets_search_path(tmp_path):
    env = {
        "DAGSTER_POSTGRES_URL": "postgresql://u:p@db.example.test/urban_rag?sslmode=require",
        "DAGSTER_POSTGRES_SCHEMA": "dagster_meta",
    }

    configure_dagster_home(tmp_path, env)

    query = parse_qs(urlsplit(env["DAGSTER_POSTGRES_URL"]).query)
    assert query["sslmode"] == ["require"]
    assert query["options"] == ["-csearch_path=dagster_meta,public"]
