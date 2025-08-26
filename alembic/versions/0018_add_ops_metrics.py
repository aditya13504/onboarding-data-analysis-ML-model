"""add ops metrics table

Revision ID: 0018_add_ops_metrics
Revises: 0017_add_report_log_and_insight_fields
Create Date: 2025-08-24 15:10:00.000000
"""
from alembic import op
import sqlalchemy as sa

revision = '0018_add_ops_metrics'
down_revision = '0017_add_report_log_and_insight_fields'
branch_labels = None
depends_on = None

def upgrade():
    op.create_table(
        'ops_metrics',
        sa.Column('id', sa.Integer, primary_key=True),
        sa.Column('metric_name', sa.String(128), nullable=False),
        sa.Column('metric_value', sa.Float, nullable=False),
        sa.Column('captured_at', sa.DateTime, server_default=sa.text('CURRENT_TIMESTAMP'), nullable=False),
        sa.Column('details', sa.JSON, nullable=True),
    )
    try:
        op.create_index('ix_ops_metric_name_time', 'ops_metrics', ['metric_name','captured_at'])
    except Exception:
        pass

def downgrade():
    try:
        op.drop_table('ops_metrics')
    except Exception:
        pass
