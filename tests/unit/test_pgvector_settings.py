from urban_rag.rag.pgvector import PgSettings


def test_connection_kwargs_can_use_a_tunnel_without_renaming_the_server(tmp_path):
    bundle = tmp_path / "root.crt"
    bundle.write_text("-----BEGIN CERTIFICATE-----", encoding="utf-8")
    settings = PgSettings(
        host="hbu-dev.example.rds.amazonaws.com",
        hostaddr="127.0.0.1",
        port=5433,
        password="secret",
        sslmode="verify-full",
        sslrootcert=str(bundle),
    )

    kwargs = settings.connection_kwargs()

    assert kwargs["host"] == "hbu-dev.example.rds.amazonaws.com"
    assert kwargs["hostaddr"] == "127.0.0.1"
    assert kwargs["port"] == 5433
    assert kwargs["sslmode"] == "verify-full"


def test_from_env_reads_pg_hostaddr(monkeypatch, tmp_path):
    bundle = tmp_path / "root.crt"
    bundle.write_text("-----BEGIN CERTIFICATE-----", encoding="utf-8")
    monkeypatch.setenv("URBAN_RAG_PG_HOST", "hbu-dev.example.rds.amazonaws.com")
    monkeypatch.setenv("URBAN_RAG_PG_HOSTADDR", "127.0.0.1")
    monkeypatch.setenv("URBAN_RAG_PG_PORT", "5433")
    monkeypatch.setenv("URBAN_RAG_PG_PASSWORD", "secret")
    monkeypatch.setenv("URBAN_RAG_PG_SSLROOTCERT", str(bundle))

    settings = PgSettings.from_env()

    assert settings.host == "hbu-dev.example.rds.amazonaws.com"
    assert settings.hostaddr == "127.0.0.1"
    assert settings.port == 5433
