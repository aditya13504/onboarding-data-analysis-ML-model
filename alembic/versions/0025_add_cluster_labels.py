"""add cluster_labels table

Revision ID: 0025_add_cluster_labels
Revises: 0024_add_feature_store
Create Date: 2025-08-24
"""
from __future__ import annotations

from alembic import op
import sqlalchemy as sa

revision = '0025_add_cluster_labels'
down_revision = '0024_add_feature_store'
branch_labels = None
depends_on = None


def upgrade():
    op.create_table(
        'cluster_labels',
        sa.Column('id', sa.Integer(), primary_key=True),
        sa.Column('cluster_id', sa.Integer(), nullable=False, index=True),
        sa.Column('label', sa.String(length=256), nullable=False),
        sa.Column('rationale', sa.String(length=1024), nullable=True),
        sa.Column('created_at', sa.DateTime(), nullable=False, server_default=sa.text('CURRENT_TIMESTAMP')),
        sa.Column('author', sa.String(length=64), nullable=True),
        sa.Column('active', sa.Integer(), nullable=False, server_default='1'),
    )
    # Indexes for cluster_id and author are automatically created via index=True in column definitions above.
    op.create_index('ux_cluster_label_unique', 'cluster_labels', ['cluster_id','label'], unique=True)


def downgrade():
    op.drop_index('ux_cluster_label_unique', table_name='cluster_labels')
    # Automatic column indexes will be dropped with the table.
    op.drop_table('cluster_labels')
