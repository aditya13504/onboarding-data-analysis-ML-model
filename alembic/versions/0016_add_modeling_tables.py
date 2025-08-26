"""add advanced analytics & modeling tables

Revision ID: 0016_add_modeling_tables
Revises: 0015_add_data_quality_and_schema_workflow
Create Date: 2025-08-24 13:05:00.000000
"""
from alembic import op
import sqlalchemy as sa

revision = '0016_add_modeling_tables'
down_revision = '0015_add_data_quality_and_schema_workflow'
branch_labels = None
depends_on = None

def upgrade():
    op.create_table(
        'user_features',
        sa.Column('id', sa.Integer, primary_key=True),
        sa.Column('user_id', sa.String(64), nullable=False),
        sa.Column('project', sa.String(64), nullable=True),
        sa.Column('features', sa.JSON, nullable=False, server_default='{}'),
        sa.Column('updated_at', sa.DateTime, server_default=sa.text('CURRENT_TIMESTAMP'), nullable=False),
    )
    op.create_index('ux_user_feature_user_project', 'user_features', ['user_id','project'], unique=True)
    op.create_table(
        'user_churn_risk',
        sa.Column('id', sa.Integer, primary_key=True),
        sa.Column('user_id', sa.String(64), nullable=False),
        sa.Column('project', sa.String(64), nullable=True),
        sa.Column('risk_score', sa.Float, nullable=False),
        sa.Column('model_version', sa.String(32), nullable=False),
        sa.Column('computed_at', sa.DateTime, server_default=sa.text('CURRENT_TIMESTAMP'), nullable=False),
    )
    op.create_index('ix_churn_user_project', 'user_churn_risk', ['user_id','project'])
    op.create_table(
        'cohort_retention',
        sa.Column('id', sa.Integer, primary_key=True),
        sa.Column('cohort_date', sa.DateTime, nullable=False),
        sa.Column('day_number', sa.Integer, nullable=False),
        sa.Column('retained_users', sa.Integer, nullable=False),
        sa.Column('total_users', sa.Integer, nullable=False),
        sa.Column('project', sa.String(64), nullable=True),
        sa.Column('created_at', sa.DateTime, server_default=sa.text('CURRENT_TIMESTAMP'), nullable=False),
    )
    op.create_index('ix_retention_cohort_day_project', 'cohort_retention', ['cohort_date','day_number','project'], unique=True)
    op.create_table(
        'ab_test_metrics',
        sa.Column('id', sa.Integer, primary_key=True),
        sa.Column('experiment_name', sa.String(128), nullable=False),
        sa.Column('variant', sa.String(64), nullable=False),
        sa.Column('metric_name', sa.String(64), nullable=False),
        sa.Column('metric_value', sa.Float, nullable=False),
        sa.Column('users', sa.Integer, nullable=False),
        sa.Column('p_value', sa.Float, nullable=True),
        sa.Column('computed_at', sa.DateTime, server_default=sa.text('CURRENT_TIMESTAMP'), nullable=False),
        sa.Column('project', sa.String(64), nullable=True),
    )
    op.create_index('ix_abtest_exp_variant_metric', 'ab_test_metrics', ['experiment_name','variant','metric_name','project'], unique=True)

def downgrade():
    for t in ['ab_test_metrics','cohort_retention','user_churn_risk','user_features']:
        try:
            op.drop_table(t)
        except Exception:
            pass
