import os
from contextlib import contextmanager

import psycopg2
from psycopg2 import sql


PG_HOST = os.environ.get('PGHOST', 'localhost')
PG_SUPERUSER = os.environ.get('PGSUPERUSER', 'postgres')
PG_SUPERUESR_PASSWORD = 'Postgres@123456'
PG_SUPERUSER_DB = os.environ.get('PGSUPERDB', 'postgres')
APP_DB_USER = 'local_write'
APP_DB_PASSWORD = 'Abc@123456'


def _connect(dbname: str, user: str, password: str | None = None, autocommit: bool = True):
    conn = psycopg2.connect(
        dbname=dbname,
        user=user,
        password=password,
        host=PG_HOST,
        connect_timeout=10,
        sslmode='disable',
    )
    conn.autocommit = autocommit
    return conn


def create_connection(dbname: str):
    """Connect to the requested database as the application role."""
    conn = psycopg2.connect(
        dbname=dbname,
        user=APP_DB_USER,
        password=APP_DB_PASSWORD,
        host=PG_HOST,
        connect_timeout=10,
        sslmode='disable',
    )
    conn.autocommit = False
    return conn


@contextmanager
def admin_cursor(dbname: str = PG_SUPERUSER_DB):
    conn = _connect(dbname, PG_SUPERUSER, PG_SUPERUESR_PASSWORD)
    try:
        cur = conn.cursor()
        yield cur
        cur.close()
    finally:
        conn.close()


def ensure_role_exists():
    with admin_cursor() as cur:
        cur.execute("SELECT 1 FROM pg_roles WHERE rolname = %s;", (APP_DB_USER,))
        exists = cur.fetchone() is not None
        if not exists:
            cur.execute(
                sql.SQL("CREATE ROLE {} WITH LOGIN PASSWORD %s;").format(sql.Identifier(APP_DB_USER)),
                (APP_DB_PASSWORD,),
            )
        else:
            cur.execute(
                sql.SQL("ALTER ROLE {} WITH LOGIN PASSWORD %s;").format(sql.Identifier(APP_DB_USER)),
                (APP_DB_PASSWORD,),
            )


def ensure_database_exists(dbname: str):
    ensure_role_exists()
    with admin_cursor() as cur:
        cur.execute("SELECT 1 FROM pg_database WHERE datname = %s;", (dbname,))
        exists = cur.fetchone() is not None
        if not exists:
            cur.execute(
                sql.SQL("CREATE DATABASE {} OWNER {} TEMPLATE template1;" ).format(
                    sql.Identifier(dbname), sql.Identifier(APP_DB_USER)
                )
            )


def ensure_database_ready(dbname: str, install_timescaledb: bool = True):
    """Ensure role, database, schema privileges, and optional TimescaleDB extension are available."""
    ensure_database_exists(dbname)
    if install_timescaledb:
        with admin_cursor(dbname) as cur:
            cur.execute("SELECT 1 FROM pg_extension WHERE extname = 'timescaledb';")
            exists = cur.fetchone() is not None
            if not exists:
                try:
                    cur.execute("CREATE EXTENSION timescaledb;")
                except psycopg2.Error as exc:
                    raise RuntimeError(
                        "Failed to CREATE EXTENSION timescaledb. Ensure TimescaleDB is installed and "
                        "shared_preload_libraries includes 'timescaledb', then restart PostgreSQL."
                    ) from exc
    with admin_cursor() as cur:
        cur.execute(
            sql.SQL("GRANT ALL PRIVILEGES ON DATABASE {} TO {};" ).format(
                sql.Identifier(dbname), sql.Identifier(APP_DB_USER)
            )
        )
    ensure_schema_privileges(dbname)


def ensure_schema_privileges(dbname: str, schema: str = 'public'):
    with admin_cursor(dbname) as cur:
        cur.execute(
            sql.SQL("GRANT USAGE, CREATE ON SCHEMA {} TO {};").format(
                sql.Identifier(schema), sql.Identifier(APP_DB_USER)
            )
        )
        cur.execute(
            sql.SQL(
                "ALTER DEFAULT PRIVILEGES FOR ROLE {} IN SCHEMA {} GRANT SELECT, INSERT, UPDATE, DELETE ON TABLES TO {};"
            ).format(
                sql.Identifier(APP_DB_USER),
                sql.Identifier(schema),
                sql.Identifier(APP_DB_USER),
            )
        )
        cur.execute(
            sql.SQL(
                "ALTER DEFAULT PRIVILEGES FOR ROLE {} IN SCHEMA {} GRANT USAGE, SELECT ON SEQUENCES TO {};"
            ).format(
                sql.Identifier(APP_DB_USER),
                sql.Identifier(schema),
                sql.Identifier(APP_DB_USER),
            )
        )
