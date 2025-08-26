"""add experiment decision snapshots

Revision ID: 0036_experiment_decision_snapshots
Revises: 0035_abtest_bayesian_fields
Create Date: 2025-08-25
"""
from alembic import op
import sqlalchemy as sa

# revision identifiers, used by Alembic.
revision = '0036_experiment_decision_snapshots'
down_revision = '0035_abtest_bayesian_fields'
branch_labels = None
depends_on = None

def upgrade():
    op.create_table(
        'experiment_decision_snapshots',
        sa.Column('id', sa.Integer(), primary_key=True),
        sa.Column('experiment_prop', sa.String(length=128), index=True, nullable=False),
        sa.Column('experiment_key', sa.String(length=128), index=True, nullable=True),
        sa.Column('conversion_event', sa.String(length=128), index=True, nullable=False),
        sa.Column('decision', sa.String(length=32), index=True, nullable=False),
        sa.Column('baseline', sa.String(length=64), nullable=False),
        sa.Column('winner', sa.String(length=64), nullable=True),
        sa.Column('variant', sa.String(length=64), index=True, nullable=False),
        sa.Column('prob_beats_baseline', sa.Float(), nullable=True),
        sa.Column('prob_practical_lift', sa.Float(), nullable=True),
        sa.Column('mean_rate', sa.Float(), nullable=True),
        sa.Column('users', sa.Integer(), nullable=False, server_default='0'),
        sa.Column('conversions', sa.Integer(), nullable=False, server_default='0'),
        sa.Column('computed_at', sa.DateTime(), nullable=False, server_default=sa.text('CURRENT_TIMESTAMP')),
    )
    op.create_index('ix_exp_decision_prop_time', 'experiment_decision_snapshots', ['experiment_prop','computed_at'])
    op.create_index('ix_exp_decision_variant', 'experiment_decision_snapshots', ['experiment_prop','variant','computed_at'])


def downgrade():
    op.drop_index('ix_exp_decision_variant', table_name='experiment_decision_snapshots')
    op.drop_index('ix_exp_decision_prop_time', table_name='experiment_decision_snapshots')
    op.drop_table('experiment_decision_snapshots')
