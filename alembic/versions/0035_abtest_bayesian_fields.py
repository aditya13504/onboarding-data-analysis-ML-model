"""Add Bayesian fields to AB test metrics

Revision ID: 0035_abtest_bayesian_fields
Revises: 0034_experiment_framework
Create Date: 2025-08-25
"""
from alembic import op
import sqlalchemy as sa

revision = '0035_abtest_bayesian_fields'
down_revision = '0034_experiment_framework'
branch_labels = None
depends_on = None

def upgrade() -> None:
    with op.batch_alter_table('ab_test_metrics') as batch:
        batch.add_column(sa.Column('lift', sa.Float(), nullable=True))
        batch.add_column(sa.Column('prob_beats_baseline', sa.Float(), nullable=True))
        batch.add_column(sa.Column('hpdi_low', sa.Float(), nullable=True))
        batch.add_column(sa.Column('hpdi_high', sa.Float(), nullable=True))


def downgrade() -> None:
    with op.batch_alter_table('ab_test_metrics') as batch:
        batch.drop_column('hpdi_high')
        batch.drop_column('hpdi_low')
        batch.drop_column('prob_beats_baseline')
        batch.drop_column('lift')
