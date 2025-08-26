"""Add RBAC roles, role assignments, personalization recommendations, and lineage tables

Revision ID: 0031_rbac_personalization_lineage
Revises: 0030_phase1_connector_enhancements
Create Date: 2025-08-25
"""
from alembic import op
import sqlalchemy as sa

revision = '0031_rbac_personalization_lineage'
down_revision = '0030_phase1_connector_enhancements'
branch_labels = None
depends_on = None

def upgrade() -> None:
    # roles table
    op.create_table(
        'roles',
        sa.Column('id', sa.Integer(), primary_key=True),
        sa.Column('name', sa.String(length=64), nullable=False, unique=True),
        sa.Column('scopes', sa.String(length=512), nullable=False, server_default='read'),
        sa.Column('description', sa.String(length=256), nullable=True),
        sa.Column('created_at', sa.DateTime(), nullable=False),
        sa.Column('active', sa.Integer(), nullable=False, server_default='1'),
    )
    op.create_index('ix_role_active', 'roles', ['active'])
    op.create_index(op.f('ix_roles_name'), 'roles', ['name'], unique=True)

    # role assignments
    op.create_table(
        'role_assignments',
        sa.Column('id', sa.Integer(), primary_key=True),
        sa.Column('api_key_id', sa.Integer(), nullable=False),
        sa.Column('role_id', sa.Integer(), nullable=False),
        sa.Column('created_at', sa.DateTime(), nullable=False),
        sa.Column('active', sa.Integer(), nullable=False, server_default='1'),
        sa.ForeignKeyConstraint(['api_key_id'], ['api_keys.id']),
        sa.ForeignKeyConstraint(['role_id'], ['roles.id']),
    )
    op.create_index('ix_role_assignment_key_role', 'role_assignments', ['api_key_id','role_id'], unique=True)

    # user recommendations
    op.create_table(
        'user_recommendations',
        sa.Column('id', sa.Integer(), primary_key=True),
        sa.Column('user_id', sa.String(length=64), nullable=False),
        sa.Column('project', sa.String(length=64), nullable=True),
        sa.Column('recommendation', sa.String(length=64), nullable=False),
        sa.Column('rationale', sa.String(length=256), nullable=True),
        sa.Column('status', sa.String(length=16), nullable=False, server_default='new'),
        sa.Column('created_at', sa.DateTime(), nullable=False),
        sa.Column('expires_at', sa.DateTime(), nullable=True),
        sa.Column('acted_at', sa.DateTime(), nullable=True),
        sa.Column('action_event_name', sa.String(length=128), nullable=True),
        sa.Column('impressions', sa.Integer(), nullable=False, server_default='0'),
        sa.Column('clicks', sa.Integer(), nullable=False, server_default='0'),
    )
    op.create_index('ux_user_rec_unique', 'user_recommendations', ['user_id','recommendation'], unique=True)
    op.create_index('ix_rec_status', 'user_recommendations', ['status'])

    # lineage assets
    op.create_table(
        'data_assets',
        sa.Column('id', sa.Integer(), primary_key=True),
        sa.Column('asset_key', sa.String(length=128), nullable=False, unique=True),
        sa.Column('asset_type', sa.String(length=32), nullable=False),
        sa.Column('description', sa.String(length=256), nullable=True),
        sa.Column('created_at', sa.DateTime(), nullable=False),
    )
    op.create_index('ix_asset_type', 'data_assets', ['asset_type'])

    # lineage edges
    op.create_table(
        'data_lineage_edges',
        sa.Column('id', sa.Integer(), primary_key=True),
        sa.Column('upstream_key', sa.String(length=128), nullable=False),
        sa.Column('downstream_key', sa.String(length=128), nullable=False),
        sa.Column('transform', sa.String(length=256), nullable=True),
        sa.Column('created_at', sa.DateTime(), nullable=False),
    )
    op.create_index('ux_lineage_pair', 'data_lineage_edges', ['upstream_key','downstream_key'], unique=True)


def downgrade() -> None:
    op.drop_table('data_lineage_edges')
    op.drop_table('data_assets')
    op.drop_table('user_recommendations')
    op.drop_table('role_assignments')
    op.drop_table('roles')
