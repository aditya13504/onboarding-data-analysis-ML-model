"""add connector credentials table

Revision ID: 0030_add_connector_credentials
Revises: 0029_normalization_enrichment_rules
Create Date: 2025-08-25
"""
from alembic import op
import sqlalchemy as sa

revision = '0030_add_connector_credentials'
down_revision = '0029_normalization_enrichment_rules'
branch_labels = None
depends_on = None

def upgrade() -> None:
    op.create_table(
        'connector_credentials',
        sa.Column('id', sa.Integer, primary_key=True),
        sa.Column('connector_name', sa.String(64), nullable=False, index=True),
        sa.Column('secret_key', sa.String(128), nullable=False, index=True),
        sa.Column('encrypted_value', sa.String(1024), nullable=False),
        sa.Column('version', sa.Integer, server_default='1', nullable=False, index=True),
        sa.Column('active', sa.Integer, server_default='1', nullable=False, index=True),
        sa.Column('created_at', sa.DateTime, server_default=sa.func.now(), index=True),
        sa.Column('rotated_at', sa.DateTime, nullable=True, index=True),
    )
    op.create_index('ux_connector_secret_unique', 'connector_credentials', ['connector_name','secret_key'], unique=True)

def downgrade() -> None:
    op.drop_index('ux_connector_secret_unique', table_name='connector_credentials')
    op.drop_table('connector_credentials')
