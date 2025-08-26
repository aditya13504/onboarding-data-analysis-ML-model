"""Add model drift thresholds and audit tables

Revision ID: 0033_model_drift_thresholds
Revises: 0032_personalization_rules
Create Date: 2025-08-25
"""
from alembic import op
import sqlalchemy as sa

revision = '0033_model_drift_thresholds'
down_revision = '0032_personalization_rules'
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        'model_drift_thresholds',
        sa.Column('id', sa.Integer(), primary_key=True),
        sa.Column('model_name', sa.String(length=64), nullable=False),
        sa.Column('metric_name', sa.String(length=64), nullable=False),
        sa.Column('comparison', sa.String(length=4), nullable=False, server_default='gt'),
        sa.Column('boundary', sa.Float(), nullable=False),
        sa.Column('action', sa.String(length=16), nullable=False, server_default='retrain'),
        sa.Column('cooldown_hours', sa.Integer(), nullable=False, server_default='24'),
        sa.Column('active', sa.Integer(), nullable=False, server_default='1'),
        sa.Column('created_at', sa.DateTime(), nullable=False),
        sa.Column('last_triggered_at', sa.DateTime(), nullable=True),
    )
    op.create_index('ux_drift_threshold_key', 'model_drift_thresholds', ['model_name','metric_name','comparison','boundary'], unique=True)
    op.create_index('ix_model_drift_threshold_active', 'model_drift_thresholds', ['active'])

    op.create_table(
        'drift_retrain_audit',
        sa.Column('id', sa.Integer(), primary_key=True),
        sa.Column('model_name', sa.String(length=64), nullable=False),
        sa.Column('metric_name', sa.String(length=64), nullable=False),
        sa.Column('metric_value', sa.Float(), nullable=False),
        sa.Column('action', sa.String(length=16), nullable=False),
        sa.Column('created_at', sa.DateTime(), nullable=False),
        sa.Column('notes', sa.String(length=256), nullable=True),
    )
    op.create_index('ix_drift_retrain_audit_model_metric', 'drift_retrain_audit', ['model_name','metric_name'])


def downgrade() -> None:
    op.drop_table('drift_retrain_audit')
    op.drop_table('model_drift_thresholds')
