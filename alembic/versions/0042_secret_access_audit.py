"""add secret access audit table

Revision ID: 0042_secret_access_audit
Revises: 0041_notification_templates
Create Date: 2025-08-25
"""
from alembic import op
import sqlalchemy as sa

revision = '0042_secret_access_audit'
down_revision = '0041_notification_templates'
branch_labels = None
depends_on = None


def upgrade():
    op.create_table(
        'secret_access_audit',
        sa.Column('id', sa.Integer(), primary_key=True),
        sa.Column('connector_name', sa.String(length=64), nullable=False, index=True),
        sa.Column('secret_key', sa.String(length=128), nullable=False, index=True),
        sa.Column('accessed_at', sa.DateTime(), nullable=False, server_default=sa.text('CURRENT_TIMESTAMP'), index=True),
    )
    op.create_index('ix_secret_access_key', 'secret_access_audit', ['connector_name','secret_key','accessed_at'])


def downgrade():
    op.drop_index('ix_secret_access_key', table_name='secret_access_audit')
    op.drop_table('secret_access_audit')
