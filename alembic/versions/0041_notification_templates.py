"""notification templates

Revision ID: 0041_notification_templates
Revises: 0040_experiment_promotion_audit
Create Date: 2025-08-25
"""
from alembic import op
import sqlalchemy as sa

revision = '0041_notification_templates'
down_revision = '0040_experiment_promotion_audit'
branch_labels = None
depends_on = None


def upgrade():
    op.create_table(
        'notification_templates',
        sa.Column('id', sa.Integer(), primary_key=True),
        sa.Column('name', sa.String(length=64), nullable=False, unique=True),
        sa.Column('channel', sa.String(length=16), nullable=False, index=True),
        sa.Column('subject', sa.String(length=128), nullable=True),
        sa.Column('body', sa.String(length=1024), nullable=False),
        sa.Column('active', sa.Integer(), nullable=False, server_default='1', index=True),
        sa.Column('created_at', sa.DateTime(), nullable=False, server_default=sa.text('CURRENT_TIMESTAMP'), index=True),
        sa.Column('updated_at', sa.DateTime(), nullable=False, server_default=sa.text('CURRENT_TIMESTAMP'), index=True),
    )
    op.create_index('ix_template_channel_active', 'notification_templates', ['channel','active'])
    op.create_index('ix_notification_template_name', 'notification_templates', ['name'])


def downgrade():
    op.drop_index('ix_template_channel_active', table_name='notification_templates')
    op.drop_index('ix_notification_template_name', table_name='notification_templates')
    op.drop_table('notification_templates')
