"""add mlops hygiene tables

Revision ID: 0019_add_mlops_hygiene
Revises: 0018_add_ops_metrics
Create Date: 2025-08-24 16:00:00.000000
"""
from alembic import op
import sqlalchemy as sa

revision = '0019_add_mlops_hygiene'
down_revision = '0018_add_ops_metrics'
branch_labels = None
depends_on = None


def upgrade():
    # artifact hash column (nullable)
    try:
        with op.batch_alter_table('model_artifacts') as batch_op:
            batch_op.add_column(sa.Column('artifact_hash', sa.String(64), nullable=True))
            batch_op.create_index('ix_model_artifact_hash', ['artifact_hash'])
    except Exception:
        pass
    # training runs
    op.create_table(
        'model_training_runs',
        sa.Column('id', sa.Integer, primary_key=True),
        sa.Column('model_name', sa.String(64), index=True),
        sa.Column('version', sa.String(32), index=True),
        sa.Column('started_at', sa.DateTime, server_default=sa.text('CURRENT_TIMESTAMP'), index=True),
        sa.Column('completed_at', sa.DateTime, nullable=True, index=True),
        sa.Column('status', sa.String(16), index=True),
        sa.Column('params', sa.JSON, nullable=True),
        sa.Column('metrics', sa.JSON, nullable=True),
        sa.Column('data_fingerprint', sa.String(64), nullable=True, index=True),
        sa.Column('training_rows', sa.Integer, nullable=True),
        sa.Column('notes', sa.String(512), nullable=True),
    )
    try:
        op.create_index('ix_training_model_version', 'model_training_runs', ['model_name','version'])
    except Exception:
        pass
    # drift metrics
    op.create_table(
        'model_drift_metrics',
        sa.Column('id', sa.Integer, primary_key=True),
        sa.Column('model_name', sa.String(64), index=True),
        sa.Column('metric_name', sa.String(64), index=True),
        sa.Column('metric_value', sa.Float),
        sa.Column('captured_at', sa.DateTime, server_default=sa.text('CURRENT_TIMESTAMP'), index=True),
        sa.Column('window', sa.String(32), nullable=True),
        sa.Column('details', sa.JSON, nullable=True),
    )
    try:
        op.create_index('ix_drift_model_metric_time', 'model_drift_metrics', ['model_name','metric_name','captured_at'])
    except Exception:
        pass


def downgrade():
    try:
        op.drop_table('model_drift_metrics')
    except Exception:
        pass
    try:
        op.drop_table('model_training_runs')
    except Exception:
        pass
    try:
        with op.batch_alter_table('model_artifacts') as batch_op:
            batch_op.drop_index('ix_model_artifact_hash')
            batch_op.drop_column('artifact_hash')
    except Exception:
        pass
