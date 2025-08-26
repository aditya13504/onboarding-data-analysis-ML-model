"""add archive compacted events table

Revision ID: 0011_add_archive_compaction
Revises: 0010_add_time_partitioning_raw_events
Create Date: 2025-08-24 00:00:00.000000
"""

from alembic import op
import sqlalchemy as sa

revision = '0011_add_archive_compaction'
down_revision = '0010_add_time_partitioning_raw_events'
branch_labels = None
depends_on = None


def upgrade():
    op.create_table(
        'archive_compacted_events',
        sa.Column('id', sa.Integer, primary_key=True),
        sa.Column('project', sa.String(64), index=True, nullable=True),
        sa.Column('event_name', sa.String(128), index=True, nullable=False),
        sa.Column('event_date', sa.DateTime, index=True, nullable=False),
        sa.Column('events_count', sa.Integer, nullable=False),
        sa.Column('users_count', sa.Integer, nullable=False),
        sa.Column('sessions_count', sa.Integer, nullable=False),
        sa.Column('sample_props', sa.JSON, nullable=True),
        sa.Column('first_ts', sa.DateTime, nullable=False),
        sa.Column('last_ts', sa.DateTime, nullable=False),
        sa.Column('compacted_at', sa.DateTime, nullable=False, server_default=sa.text('CURRENT_TIMESTAMP')),
    )
    op.create_index('ux_archive_compact_key', 'archive_compacted_events', ['project', 'event_name', 'event_date'], unique=True)


def downgrade():
    op.drop_index('ux_archive_compact_key', table_name='archive_compacted_events')
    op.drop_table('archive_compacted_events')
