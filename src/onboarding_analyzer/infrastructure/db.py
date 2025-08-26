from __future__ import annotations
from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker, DeclarativeBase
from onboarding_analyzer.config import get_settings


class Base(DeclarativeBase):
    pass


def _dsn() -> str:
    s = get_settings()
    # Always use Supabase - require Supabase configuration
    if not getattr(s, "supabase_project_ref", None) or not getattr(s, "supabase_db_password", None):
        raise RuntimeError(
            "Supabase configuration required. Please set SUPABASE_PROJECT_REF and SUPABASE_DB_PASSWORD environment variables."
        )
    
    host = f"db.{s.supabase_project_ref}.supabase.co"
    return f"postgresql+psycopg2://{s.supabase_db_user}:{s.supabase_db_password}@{host}:5432/{s.supabase_db_name}?sslmode=require"


engine = create_engine(_dsn(), pool_pre_ping=True)
SessionLocal = sessionmaker(bind=engine, autoflush=False, expire_on_commit=False)

def override_engine(e):  # test helper
    global engine, SessionLocal
    engine = e
    SessionLocal = sessionmaker(bind=engine, autoflush=False, expire_on_commit=False)


def healthcheck() -> bool:
    with engine.connect() as conn:
        conn.execute(text("SELECT 1"))
        return True
