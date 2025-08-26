"""add experiment promotion audit table

Revision ID: 0040_experiment_promotion_audit
Revises: 0039_experiment_composite_scoring
Create Date: 2025-08-25 00:00:00.000000
"""
from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql

# revision identifiers, used by Alembic.
revision = '0040_experiment_promotion_audit'
down_revision = '0039_experiment_composite_scoring'
branch_labels = None
depends_on = None


def upgrade():
    op.create_table(
        'experiment_promotion_audit',
        sa.Column('id', sa.Integer(), primary_key=True),
        sa.Column('experiment_prop', sa.String(length=128), nullable=False, index=True),
        sa.Column('experiment_key', sa.String(length=128), nullable=True, index=True),
        sa.Column('winner_variant', sa.String(length=64), nullable=False, index=True),
        sa.Column('decision', sa.String(length=32), nullable=False, index=True),
        sa.Column('rationale', sa.String(length=256), nullable=True),
        sa.Column('details', postgresql.JSONB(astext_type=sa.Text()) if op.get_context().dialect.name == 'postgresql' else sa.JSON(), nullable=True),
        sa.Column('created_at', sa.DateTime(), nullable=False, index=True, server_default=sa.text('CURRENT_TIMESTAMP')),
    )
    op.create_index('ix_exp_promo_prop_time', 'experiment_promotion_audit', ['experiment_prop','created_at'])


def downgrade():
    op.drop_index('ix_exp_promo_prop_time', table_name='experiment_promotion_audit')
    op.drop_table('experiment_promotion_audit')
