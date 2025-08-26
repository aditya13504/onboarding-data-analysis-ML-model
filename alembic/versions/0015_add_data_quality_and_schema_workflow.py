"""add data quality snapshot table and schema workflow fields

Revision ID: 0015_add_data_quality_and_schema_workflow
Revises: 0014_add_dlq_attempts
Create Date: 2025-08-24 12:40:00.000000
"""
from alembic import op
import sqlalchemy as sa

revision = '0015_add_data_quality_and_schema_workflow'
down_revision = '0014_add_dlq_attempts'
branch_labels = None
depends_on = None

def upgrade():
    # EventSchema workflow columns (defensive add)
    for col in [
        sa.Column('status', sa.String(16), nullable=False, server_default='approved'),
        sa.Column('proposed_at', sa.DateTime, nullable=True),
        sa.Column('approved_at', sa.DateTime, nullable=True),
    ]:
        try:
            op.add_column('event_schemas', col)
        except Exception:
            pass
    try:
        op.create_index('ix_event_schemas_status', 'event_schemas', ['status'])
    except Exception:
        pass
    # DataQualitySnapshot table
    op.create_table(
        'data_quality_snapshots',
        sa.Column('id', sa.Integer, primary_key=True),
        sa.Column('captured_at', sa.DateTime, server_default=sa.text('CURRENT_TIMESTAMP'), nullable=False),
        sa.Column('project', sa.String(64), nullable=True),
        sa.Column('event_name', sa.String(128), nullable=True),
        sa.Column('metric_type', sa.String(64), nullable=False),
        sa.Column('metric_value', sa.Float, nullable=False),
        sa.Column('details', sa.JSON, nullable=True),
    )
    op.create_index('ix_dq_captured_at', 'data_quality_snapshots', ['captured_at'])
    op.create_index('ix_dq_metric_type', 'data_quality_snapshots', ['metric_type'])
    op.create_index('ix_dq_unique', 'data_quality_snapshots', ['captured_at','project','event_name','metric_type'])

def downgrade():
    op.drop_table('data_quality_snapshots')
    for idx in ['ix_event_schemas_status']:
        try:
            op.drop_index(idx, table_name='event_schemas')
        except Exception:
            pass
    for col in ['status','proposed_at','approved_at']:
        try:
            op.drop_column('event_schemas', col)
        except Exception:
            pass
