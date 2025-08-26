"""add feature store tables

Revision ID: 0024_add_feature_store
Revises: 0023_add_backfill_jobs
Create Date: 2025-08-24
"""
from __future__ import annotations

from alembic import op
import sqlalchemy as sa

revision = '0024_add_feature_store'
down_revision = '0023_add_backfill_jobs'
branch_labels = None
depends_on = None


def upgrade():
    op.create_table(
        'feature_definitions',
        sa.Column('id', sa.Integer(), primary_key=True),
        sa.Column('feature_key', sa.String(length=256), nullable=False),
        sa.Column('entity', sa.String(length=32), nullable=False),
        sa.Column('version', sa.String(length=32), nullable=False, server_default='v1'),
        sa.Column('expr', sa.String(length=2048), nullable=False),
        sa.Column('description', sa.String(length=512), nullable=True),
        sa.Column('created_at', sa.DateTime(), nullable=False, server_default=sa.text('CURRENT_TIMESTAMP')),
        sa.Column('updated_at', sa.DateTime(), nullable=False, server_default=sa.text('CURRENT_TIMESTAMP')),
        sa.Column('status', sa.String(length=16), nullable=False, server_default='active'),
    )
    op.create_index('ix_feature_key', 'feature_definitions', ['feature_key'], unique=True)
    op.create_index('ix_feature_def_entity', 'feature_definitions', ['entity'])
    op.create_index('ix_feature_def_status', 'feature_definitions', ['status'])

    op.create_table(
        'feature_views',
        sa.Column('id', sa.Integer(), primary_key=True),
        sa.Column('feature_key', sa.String(length=256), nullable=False),
        sa.Column('entity_id', sa.String(length=128), nullable=False),
        sa.Column('value', sa.JSON(), nullable=False),
        sa.Column('feature_version', sa.String(length=32), nullable=False),
        sa.Column('computed_at', sa.DateTime(), nullable=False, server_default=sa.text('CURRENT_TIMESTAMP')),
    )
    op.create_index('ux_feature_view_key_entity_version', 'feature_views', ['feature_key','entity_id','feature_version'], unique=True)


def downgrade():
    op.drop_index('ux_feature_view_key_entity_version', table_name='feature_views')
    op.drop_table('feature_views')
    op.drop_index('ix_feature_key', table_name='feature_definitions')
    op.drop_index('ix_feature_def_entity', table_name='feature_definitions')
    op.drop_index('ix_feature_def_status', table_name='feature_definitions')
    op.drop_table('feature_definitions')
