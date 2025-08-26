"""add privacy deletion audit

Revision ID: 0045_privacy_deletion_audit
Revises: 0044_maintenance_jobs
Create Date: 2025-08-25
"""
from alembic import op
import sqlalchemy as sa

revision = '0045_privacy_deletion_audit'
down_revision = '0044_maintenance_jobs'
branch_labels = None
depends_on = None

def upgrade():
    op.create_table(
        'privacy_deletion_audit',
        sa.Column('id', sa.Integer(), primary_key=True),
        sa.Column('user_id', sa.String(length=64), nullable=False, index=True),
        sa.Column('project', sa.String(length=64), nullable=True, index=True),
        sa.Column('deleted_at', sa.DateTime(), nullable=False, server_default=sa.text('CURRENT_TIMESTAMP'), index=True),
        sa.Column('raw_events', sa.Integer(), nullable=False, server_default='0'),
        sa.Column('session_summaries', sa.Integer(), nullable=False, server_default='0'),
        sa.Column('features', sa.Integer(), nullable=False, server_default='0'),
        sa.Column('recommendations', sa.Integer(), nullable=False, server_default='0'),
        sa.Column('insights', sa.Integer(), nullable=False, server_default='0'),
        sa.Column('notes', sa.String(length=256), nullable=True),
    )
    op.create_index('ix_privacy_deletion_user_project', 'privacy_deletion_audit', ['user_id','project'])

def downgrade():
    op.drop_index('ix_privacy_deletion_user_project', table_name='privacy_deletion_audit')
    op.drop_table('privacy_deletion_audit')
