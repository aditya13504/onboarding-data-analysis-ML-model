"""add event schema registry

Revision ID: 0012_add_event_schema_registry
Revises: 0011_add_archive_compaction
Create Date: 2025-08-24 00:10:00.000000
"""

from alembic import op
import sqlalchemy as sa

revision = '0012_add_event_schema_registry'
down_revision = '0011_add_archive_compaction'
branch_labels = None
depends_on = None


def upgrade():
    op.create_table(
        'event_schemas',
        sa.Column('id', sa.Integer, primary_key=True),
        sa.Column('event_name', sa.String(128), nullable=False),
        sa.Column('required_props', sa.JSON, nullable=False, server_default='{}'),
        sa.Column('min_version', sa.String(16), nullable=False, server_default='v1'),
        sa.Column('active', sa.Integer, nullable=False, server_default='1'),
        sa.Column('created_at', sa.DateTime, server_default=sa.text('CURRENT_TIMESTAMP'), nullable=False),
    )
    op.create_index('ix_event_schemas_event_name', 'event_schemas', ['event_name'], unique=True)
    op.create_index('ix_event_schemas_created_at', 'event_schemas', ['created_at'])


def downgrade():
    op.drop_index('ix_event_schemas_event_name', table_name='event_schemas')
    op.drop_table('event_schemas')
