"""enterprise expansion: anomaly, consent, cost usage, recommendation feedback

Revision ID: 0049_enterprise_expansion
Revises: 0048_slo_tables
Create Date: 2025-08-25
"""
from __future__ import annotations

from alembic import op
import sqlalchemy as sa

revision = '0049_enterprise_expansion'
down_revision = '0048_slo_tables'
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        'anomaly_events',
        sa.Column('id', sa.Integer(), primary_key=True),
        sa.Column('anomaly_type', sa.String(length=32), index=True, nullable=False),
        sa.Column('project', sa.String(length=64), index=True, nullable=True),
        sa.Column('fingerprint', sa.String(length=128), nullable=True, index=True),
        sa.Column('severity', sa.Float(), nullable=True, index=True),
        sa.Column('delta_pct', sa.Float(), nullable=True),
        sa.Column('details', sa.JSON(), nullable=True),
        sa.Column('status', sa.String(length=16), nullable=False, server_default='open', index=True),
        sa.Column('detected_at', sa.DateTime(), nullable=False, index=True),
        sa.Column('acknowledged_at', sa.DateTime(), nullable=True, index=True),
        sa.Column('closed_at', sa.DateTime(), nullable=True, index=True),
    )
    op.create_index('ix_anomaly_type_project', 'anomaly_events', ['anomaly_type','project','status'])
    op.create_index('ux_anomaly_fingerprint', 'anomaly_events', ['fingerprint'])

    op.create_table(
        'consent_records',
        sa.Column('id', sa.Integer(), primary_key=True),
        sa.Column('user_id', sa.String(length=64), index=True, nullable=False),
        sa.Column('project', sa.String(length=64), index=True, nullable=True),
        sa.Column('policy_version', sa.String(length=32), index=True, nullable=False),
        sa.Column('granted', sa.Integer(), server_default='1', index=True),
        sa.Column('updated_at', sa.DateTime(), nullable=False, index=True),
        sa.Column('source', sa.String(length=64), nullable=True),
    )
    op.create_index('ux_consent_user_policy', 'consent_records', ['user_id','policy_version','project'], unique=True)

    op.create_table(
        'cost_usage',
        sa.Column('id', sa.Integer(), primary_key=True),
        sa.Column('project', sa.String(length=64), index=True, nullable=True),
        sa.Column('window_start', sa.DateTime(), nullable=False, index=True),
        sa.Column('window_end', sa.DateTime(), nullable=False, index=True),
        sa.Column('event_count', sa.Integer(), server_default='0'),
        sa.Column('storage_bytes', sa.Integer(), server_default='0'),
        sa.Column('compute_seconds', sa.Float(), server_default='0'),
        sa.Column('cost_estimate', sa.Float(), nullable=True),
        sa.Column('breakdown', sa.JSON(), nullable=True),
        sa.Column('created_at', sa.DateTime(), nullable=False, index=True),
    )
    op.create_index('ix_cost_usage_window', 'cost_usage', ['project','window_start','window_end'], unique=True)

    op.create_table(
        'recommendation_feedback',
        sa.Column('id', sa.Integer(), primary_key=True),
        sa.Column('recommendation', sa.String(length=64), index=True, nullable=False),
        sa.Column('user_id', sa.String(length=64), index=True, nullable=False),
        sa.Column('project', sa.String(length=64), index=True, nullable=True),
        sa.Column('feedback', sa.String(length=16), index=True, nullable=False),
        sa.Column('created_at', sa.DateTime(), nullable=False, index=True),
        sa.Column('metadata', sa.JSON(), nullable=True),
    )
    op.create_index('ix_rec_feedback_user', 'recommendation_feedback', ['user_id','recommendation','created_at'])


def downgrade() -> None:
    op.drop_index('ix_rec_feedback_user', table_name='recommendation_feedback')
    op.drop_table('recommendation_feedback')
    op.drop_index('ix_cost_usage_window', table_name='cost_usage')
    op.drop_table('cost_usage')
    op.drop_index('ux_consent_user_policy', table_name='consent_records')
    op.drop_table('consent_records')
    op.drop_index('ux_anomaly_fingerprint', table_name='anomaly_events')
    op.drop_index('ix_anomaly_type_project', table_name='anomaly_events')
    op.drop_table('anomaly_events')