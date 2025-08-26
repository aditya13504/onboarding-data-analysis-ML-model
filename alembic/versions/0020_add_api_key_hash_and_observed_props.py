"""add api key hash and observed props

Revision ID: 0020_add_api_key_hash_and_observed_props
Revises: 0019_add_mlops_hygiene
Create Date: 2025-08-24 17:00:00.000000
"""
from alembic import op
import sqlalchemy as sa

revision = '0020_add_api_key_hash_and_observed_props'
down_revision = '0019_add_mlops_hygiene'
branch_labels = None
depends_on = None


def upgrade():
    try:
        with op.batch_alter_table('api_keys') as batch_op:
            batch_op.add_column(sa.Column('hashed_key', sa.String(128), nullable=True))
            batch_op.create_index('ix_api_keys_hashed_key', ['hashed_key'])
    except Exception:
        pass
    op.create_table(
        'observed_event_properties',
        sa.Column('id', sa.Integer, primary_key=True),
        sa.Column('event_name', sa.String(128), index=True),
        sa.Column('prop_name', sa.String(128), index=True),
        sa.Column('first_seen', sa.DateTime, server_default=sa.text('CURRENT_TIMESTAMP'), index=True),
        sa.Column('last_seen', sa.DateTime, server_default=sa.text('CURRENT_TIMESTAMP'), index=True),
        sa.Column('count', sa.Integer, server_default='0'),
    )
    try:
        op.create_index('ux_observed_event_prop', 'observed_event_properties', ['event_name','prop_name'], unique=True)
    except Exception:
        pass


def downgrade():
    try:
        op.drop_table('observed_event_properties')
    except Exception:
        pass
    try:
        with op.batch_alter_table('api_keys') as batch_op:
            batch_op.drop_index('ix_api_keys_hashed_key')
            batch_op.drop_column('hashed_key')
    except Exception:
        pass
