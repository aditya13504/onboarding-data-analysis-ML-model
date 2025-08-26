"""add project scoping to lineage tables

Revision ID: 0043_lineage_project_scoping
Revises: 0042_secret_access_audit
Create Date: 2025-08-25
"""
from alembic import op
import sqlalchemy as sa

revision = '0043_lineage_project_scoping'
down_revision = '0042_secret_access_audit'
branch_labels = None
depends_on = None


def upgrade():
    with op.batch_alter_table('data_assets') as b:
        b.add_column(sa.Column('project', sa.String(length=64), nullable=True))
        b.create_index('ix_data_assets_project', ['project'])
    with op.batch_alter_table('data_lineage_edges') as b:
        b.add_column(sa.Column('project', sa.String(length=64), nullable=True))
        b.create_index('ix_lineage_edges_project', ['project'])


def downgrade():
    with op.batch_alter_table('data_lineage_edges') as b:
        b.drop_index('ix_lineage_edges_project')
        b.drop_column('project')
    with op.batch_alter_table('data_assets') as b:
        b.drop_index('ix_data_assets_project')
        b.drop_column('project')
