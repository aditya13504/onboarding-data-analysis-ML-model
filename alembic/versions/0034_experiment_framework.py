"""Add experiment definitions and assignments

Revision ID: 0034_experiment_framework
Revises: 0033_model_drift_thresholds
Create Date: 2025-08-25
"""
from alembic import op
import sqlalchemy as sa

revision = '0034_experiment_framework'
down_revision = '0033_model_drift_thresholds'
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        'experiment_definitions',
        sa.Column('id', sa.Integer(), primary_key=True),
        sa.Column('key', sa.String(length=128), nullable=False),
        sa.Column('assignment_prop', sa.String(length=128), nullable=False),
        sa.Column('variants', sa.JSON(), nullable=False),
        sa.Column('traffic_allocation', sa.JSON(), nullable=True),
        sa.Column('hash_salt', sa.String(length=32), nullable=False, server_default='exp'),
        sa.Column('active', sa.Integer(), nullable=False, server_default='1'),
        sa.Column('created_at', sa.DateTime(), nullable=False),
    )
    op.create_index('ix_experiment_active', 'experiment_definitions', ['active'])
    op.create_index(op.f('ix_experiment_definitions_key'), 'experiment_definitions', ['key'], unique=True)

    op.create_table(
        'experiment_assignments',
        sa.Column('id', sa.Integer(), primary_key=True),
        sa.Column('experiment_key', sa.String(length=128), nullable=False),
        sa.Column('user_id', sa.String(length=64), nullable=False),
        sa.Column('variant', sa.String(length=64), nullable=False),
        sa.Column('assigned_at', sa.DateTime(), nullable=False),
        sa.Column('project', sa.String(length=64), nullable=True),
    )
    op.create_index('ux_experiment_user', 'experiment_assignments', ['experiment_key','user_id'], unique=True)


def downgrade() -> None:
    op.drop_table('experiment_assignments')
    op.drop_table('experiment_definitions')
