"""add experiment composite scoring tables

Revision ID: 0039_experiment_composite_scoring
Revises: 0038_experiment_adaptive_fields
Create Date: 2025-08-25
"""
from alembic import op
import sqlalchemy as sa

revision = '0039_experiment_composite_scoring'
down_revision = '0038_experiment_adaptive_fields'
branch_labels = None
depends_on = None

def upgrade():
    op.create_table(
        'experiment_composite_configs',
        sa.Column('id', sa.Integer(), primary_key=True),
        sa.Column('experiment_prop', sa.String(length=128), nullable=False, unique=True, index=True),
        sa.Column('weights', sa.JSON(), nullable=False),
        sa.Column('guardrails', sa.JSON(), nullable=True),
        sa.Column('active', sa.Integer(), nullable=False, server_default='1', index=True),
        sa.Column('created_at', sa.DateTime(), server_default=sa.text('CURRENT_TIMESTAMP'), nullable=False, index=True),
        sa.Column('updated_at', sa.DateTime(), server_default=sa.text('CURRENT_TIMESTAMP'), nullable=False, index=True)
    )
    op.create_table(
        'experiment_composite_scores',
        sa.Column('id', sa.Integer(), primary_key=True),
        sa.Column('experiment_prop', sa.String(length=128), nullable=False, index=True),
        sa.Column('variant', sa.String(length=64), nullable=False, index=True),
        sa.Column('score', sa.Float(), nullable=True),
        sa.Column('disqualified', sa.Integer(), nullable=False, server_default='0', index=True),
        sa.Column('details', sa.JSON(), nullable=False),
        sa.Column('computed_at', sa.DateTime(), server_default=sa.text('CURRENT_TIMESTAMP'), nullable=False, index=True)
    )
    op.create_index('ix_exp_comp_score_variant', 'experiment_composite_scores', ['experiment_prop','variant','computed_at'])


def downgrade():
    op.drop_index('ix_exp_comp_score_variant', table_name='experiment_composite_scores')
    op.drop_table('experiment_composite_scores')
    op.drop_table('experiment_composite_configs')
