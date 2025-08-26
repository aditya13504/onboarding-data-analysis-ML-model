from alembic import op
import sqlalchemy as sa

revision = '0010_add_time_partitioning_raw_events'
down_revision = '0009_add_archival_and_insight_shipped'
branch_labels = None
depends_on = None

PARTITION_FN = """
CREATE OR REPLACE FUNCTION raw_events_ensure_partition(ts timestamptz)
RETURNS text AS $$
DECLARE
    part_suffix text := to_char(ts, 'YYYYMM');
    part_name text := 'raw_events_' || part_suffix;
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_class WHERE relname = part_name) THEN
        EXECUTE format('CREATE TABLE IF NOT EXISTS %I (LIKE raw_events INCLUDING ALL) INHERITS (raw_events);', part_name);
        EXECUTE format('CREATE INDEX IF NOT EXISTS %I_event_name_ts ON %I (event_name, ts);', part_name, part_name);
        EXECUTE format('CREATE INDEX IF NOT EXISTS %I_user_ts ON %I (user_id, ts);', part_name, part_name);
        EXECUTE format('CREATE INDEX IF NOT EXISTS %I_project_event_ts ON %I (project, event_name, ts);', part_name, part_name);
    END IF;
    RETURN part_name;
END;
$$ LANGUAGE plpgsql;
"""

TRIGGER_FN = """
CREATE OR REPLACE FUNCTION raw_events_insert_router()
RETURNS trigger AS $$
DECLARE
    part text;
BEGIN
    part := raw_events_ensure_partition(NEW.ts);
    EXECUTE format('INSERT INTO %I VALUES ($1.*)', part) USING NEW;
    RETURN NULL;
END;
$$ LANGUAGE plpgsql;
"""

def upgrade():
    conn = op.get_bind()
    if conn.dialect.name != 'postgresql':
        return
    # Ensure parent table exists (created earlier by ORM / migrations)
    # Create helper functions
    conn.execute(sa.text(PARTITION_FN))
    conn.execute(sa.text(TRIGGER_FN))
    # Drop existing trigger if any then add
    try:
        conn.execute(sa.text('DROP TRIGGER IF EXISTS raw_events_insert_router ON raw_events'))
    except Exception:
        pass
    conn.execute(sa.text('CREATE TRIGGER raw_events_insert_router BEFORE INSERT ON raw_events FOR EACH ROW EXECUTE FUNCTION raw_events_insert_router()'))


def downgrade():
    conn = op.get_bind()
    if conn.dialect.name != 'postgresql':
        return
    try:
        conn.execute(sa.text('DROP TRIGGER IF EXISTS raw_events_insert_router ON raw_events'))
    except Exception:
        pass
    try:
        conn.execute(sa.text('DROP FUNCTION IF EXISTS raw_events_insert_router()'))
    except Exception:
        pass
    try:
        conn.execute(sa.text('DROP FUNCTION IF EXISTS raw_events_ensure_partition(timestamptz)'))
    except Exception:
        pass