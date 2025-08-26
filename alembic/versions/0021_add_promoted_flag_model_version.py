"""add promoted flag to model_versions

Revision ID: 0021_add_promoted_flag_model_version
Revises: 0020_add_api_key_hash_and_observed_props
Create Date: 2025-08-24 17:30:00.000000
"""
from alembic import op
import sqlalchemy as sa

revision = '0021_add_promoted_flag_model_version'
down_revision = '0020_add_api_key_hash_and_observed_props'
branch_labels = None
depends_on = None


def upgrade():
    try:
        with op.batch_alter_table('model_versions') as batch_op:
            batch_op.add_column(sa.Column('promoted', sa.Integer, server_default='0'))
            batch_op.create_index('ix_model_versions_promoted', ['promoted'])
    except Exception:
        pass


def downgrade():
    try:
        with op.batch_alter_table('model_versions') as batch_op:
            batch_op.drop_index('ix_model_versions_promoted')
            batch_op.drop_column('promoted')
    except Exception:
        pass
