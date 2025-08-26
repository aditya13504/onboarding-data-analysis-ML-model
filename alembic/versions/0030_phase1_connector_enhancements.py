"""phase 1 connector incremental enhancements

Revision ID: 0030_phase1_connector_enhancements
Revises: 0029_normalization_enrichment_rules
Create Date: 2025-08-25
"""
from alembic import op
import sqlalchemy as sa

revision = '0030_phase1_connector_enhancements'
down_revision = '0029_normalization_enrichment_rules'
branch_labels = None
depends_on = None

def upgrade() -> None:
    op.add_column('connector_state', sa.Column('failure_count', sa.Integer(), server_default='0', nullable=False))
    op.add_column('connector_state', sa.Column('last_error', sa.String(length=256), nullable=True))


def downgrade() -> None:
    op.drop_column('connector_state', 'last_error')
    op.drop_column('connector_state', 'failure_count')
