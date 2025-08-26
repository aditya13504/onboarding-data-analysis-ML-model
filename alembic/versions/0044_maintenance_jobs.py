"""add maintenance jobs table

Revision ID: 0044_maintenance_jobs
Revises: 0043_lineage_project_scoping
Create Date: 2025-08-25
"""
from alembic import op
import sqlalchemy as sa

revision = '0044_maintenance_jobs'
down_revision = '0043_lineage_project_scoping'
branch_labels = None
depends_on = None

def upgrade():
    op.create_table(
        'maintenance_jobs',
        sa.Column('id', sa.Integer(), primary_key=True),
        sa.Column('job_type', sa.String(length=64), nullable=False, index=True),
        sa.Column('status', sa.String(length=16), nullable=False, server_default='running', index=True),
        sa.Column('started_at', sa.DateTime(), nullable=False, server_default=sa.text('CURRENT_TIMESTAMP'), index=True),
        sa.Column('completed_at', sa.DateTime(), nullable=True, index=True),
        sa.Column('params', sa.JSON(), nullable=True),
        sa.Column('result', sa.JSON(), nullable=True),
        sa.Column('error', sa.String(length=512), nullable=True),
    )
    op.create_index('ix_maintenance_type_status', 'maintenance_jobs', ['job_type','status'])

def downgrade():
    op.drop_index('ix_maintenance_type_status', table_name='maintenance_jobs')
    op.drop_table('maintenance_jobs')
