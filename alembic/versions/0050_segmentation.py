"""segmentation tables

Revision ID: 0050_segmentation
Revises: 0049_enterprise_expansion
Create Date: 2025-08-25
"""
from __future__ import annotations

from alembic import op
import sqlalchemy as sa

revision = '0050_segmentation'
down_revision = '0049_enterprise_expansion'
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        'segment_definitions',
        sa.Column('id', sa.Integer(), primary_key=True),
        sa.Column('key', sa.String(length=128), nullable=False, index=True, unique=True),
        sa.Column('expression', sa.JSON(), nullable=False),
        sa.Column('description', sa.String(length=256), nullable=True),
        sa.Column('active', sa.Integer(), server_default='1', index=True),
        sa.Column('created_at', sa.DateTime(), nullable=False, index=True),
        sa.Column('updated_at', sa.DateTime(), nullable=False, index=True),
    )
    op.create_index('ix_segment_active', 'segment_definitions', ['active'])

    op.create_table(
        'user_segment_memberships',
        sa.Column('id', sa.Integer(), primary_key=True),
        sa.Column('segment_key', sa.String(length=128), nullable=False, index=True),
        sa.Column('user_id', sa.String(length=64), nullable=False, index=True),
        sa.Column('project', sa.String(length=64), nullable=True, index=True),
        sa.Column('computed_at', sa.DateTime(), nullable=False, index=True),
    )
    op.create_index('ux_segment_user', 'user_segment_memberships', ['segment_key','user_id'], unique=True)


def downgrade() -> None:
    op.drop_index('ux_segment_user', table_name='user_segment_memberships')
    op.drop_table('user_segment_memberships')
    op.drop_index('ix_segment_active', table_name='segment_definitions')
    op.drop_table('segment_definitions')