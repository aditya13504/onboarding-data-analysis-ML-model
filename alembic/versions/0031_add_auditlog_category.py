"""add audit log category column

Revision ID: 0031_add_auditlog_category
Revises: 0030_add_connector_credentials
Create Date: 2025-08-25
"""
from alembic import op
import sqlalchemy as sa

revision = '0031_add_auditlog_category'
down_revision = '0030_add_connector_credentials'
branch_labels = None
depends_on = None

def upgrade() -> None:
    op.add_column('audit_log', sa.Column('category', sa.String(32), nullable=True))
    op.create_index('ix_audit_log_category', 'audit_log', ['category'])


def downgrade() -> None:
    op.drop_index('ix_audit_log_category', table_name='audit_log')
    op.drop_column('audit_log', 'category')
