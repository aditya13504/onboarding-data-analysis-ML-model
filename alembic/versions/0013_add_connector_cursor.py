"""add connector cursor column

Revision ID: 0013_add_connector_cursor
Revises: 0012_add_event_schema_registry
Create Date: 2025-08-24 12:00:00.000000
"""
from alembic import op
import sqlalchemy as sa

revision = '0013_add_connector_cursor'
down_revision = '0012_add_event_schema_registry'
branch_labels = None
depends_on = None

def upgrade():
    # Defensive: only add if not already present (SQLite memory reruns in tests)
    conn = op.get_bind()
    insp = sa.inspect(conn)
    cols = [c['name'] for c in insp.get_columns('connector_state')]
    if 'cursor' not in cols:
        op.add_column('connector_state', sa.Column('cursor', sa.String(256), nullable=True))
        op.create_index('ix_connector_state_cursor', 'connector_state', ['cursor'])


def downgrade():
    conn = op.get_bind()
    insp = sa.inspect(conn)
    cols = [c['name'] for c in insp.get_columns('connector_state')]
    if 'cursor' in cols:
        try:
            op.drop_index('ix_connector_state_cursor', table_name='connector_state')
        except Exception:
            pass
        op.drop_column('connector_state', 'cursor')
