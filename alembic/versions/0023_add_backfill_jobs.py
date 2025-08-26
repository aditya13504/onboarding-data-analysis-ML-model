"""add backfill_jobs table

Revision ID: 0023_add_backfill_jobs
Revises: 0022_add_schema_drift_alerts
Create Date: 2025-08-24
"""
from __future__ import annotations

from alembic import op
import sqlalchemy as sa

revision = '0023_add_backfill_jobs'
down_revision = '0022_add_schema_drift_alerts'
branch_labels = None
depends_on = None


def upgrade():
    op.create_table(
        'backfill_jobs',
        sa.Column('id', sa.Integer(), primary_key=True),
        sa.Column('connector_name', sa.String(length=64), index=True, nullable=False),
        sa.Column('start_ts', sa.DateTime(), nullable=False, index=True),
        sa.Column('end_ts', sa.DateTime(), nullable=False, index=True),
        sa.Column('current_ts', sa.DateTime(), nullable=True, index=True),
        sa.Column('status', sa.String(length=16), nullable=False, server_default='pending', index=True),
        sa.Column('total_events', sa.Integer(), nullable=False, server_default='0'),
        sa.Column('windows_completed', sa.Integer(), nullable=False, server_default='0'),
        sa.Column('window_minutes', sa.Integer(), nullable=False, server_default='60'),
        sa.Column('created_at', sa.DateTime(), nullable=False, server_default=sa.text('CURRENT_TIMESTAMP'), index=True),
        sa.Column('updated_at', sa.DateTime(), nullable=False, server_default=sa.text('CURRENT_TIMESTAMP'), index=True),
    )
    op.create_index('ix_backfill_connector_status', 'backfill_jobs', ['connector_name', 'status'])


def downgrade():
    op.drop_index('ix_backfill_connector_status', table_name='backfill_jobs')
    op.drop_table('backfill_jobs')
