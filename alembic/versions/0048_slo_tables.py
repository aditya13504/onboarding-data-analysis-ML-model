"""slo tables

Revision ID: 0048_slo_tables
Revises: 0047_governance_quota_eval_pii
Create Date: 2025-08-25
"""
from alembic import op
import sqlalchemy as sa

revision = '0048_slo_tables'
down_revision = '0047_governance_quota_eval_pii'
branch_labels = None
depends_on = None

def upgrade():
    op.create_table(
        'slo_definitions',
        sa.Column('id', sa.Integer(), primary_key=True),
        sa.Column('name', sa.String(length=128), nullable=False, unique=True, index=True),
        sa.Column('metric', sa.String(length=64), nullable=False, index=True),
        sa.Column('target', sa.Float(), nullable=False),
        sa.Column('window_hours', sa.Integer(), nullable=False, server_default='24'),
        sa.Column('threshold', sa.Float(), nullable=True),
        sa.Column('active', sa.Integer(), nullable=False, server_default='1', index=True),
        sa.Column('created_at', sa.DateTime(), nullable=False, index=True),
        sa.Column('description', sa.String(length=256), nullable=True),
    )
    op.create_index('ix_slo_metric_active', 'slo_definitions', ['metric','active'])
    op.create_table(
        'slo_evaluations',
        sa.Column('id', sa.Integer(), primary_key=True),
        sa.Column('slo_id', sa.Integer(), nullable=False, index=True),
        sa.Column('window_start', sa.DateTime(), nullable=False, index=True),
        sa.Column('window_end', sa.DateTime(), nullable=False, index=True),
        sa.Column('attained', sa.Float(), nullable=False),
        sa.Column('target', sa.Float(), nullable=False),
        sa.Column('breach', sa.Integer(), nullable=False, server_default='0', index=True),
        sa.Column('details', sa.JSON(), nullable=True),
        sa.Column('created_at', sa.DateTime(), nullable=False, index=True),
    )
    op.create_index('ix_slo_eval_slo_window', 'slo_evaluations', ['slo_id','window_start','window_end'], unique=True)


def downgrade():
    op.drop_index('ix_slo_eval_slo_window', table_name='slo_evaluations')
    op.drop_table('slo_evaluations')
    op.drop_index('ix_slo_metric_active', table_name='slo_definitions')
    op.drop_table('slo_definitions')
