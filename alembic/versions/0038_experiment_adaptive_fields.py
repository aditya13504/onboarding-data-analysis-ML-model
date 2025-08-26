"""add adaptive fields to experiment definitions

Revision ID: 0038_experiment_adaptive_fields
Revises: 0037_alert_rules
Create Date: 2025-08-25
"""
from alembic import op
import sqlalchemy as sa

revision = '0038_experiment_adaptive_fields'
down_revision = '0037_alert_rules'
branch_labels = None
depends_on = None

def upgrade():
    op.add_column('experiment_definitions', sa.Column('adaptive', sa.Integer(), nullable=False, server_default='0'))
    op.add_column('experiment_definitions', sa.Column('conversion_event', sa.String(length=128), nullable=True))
    op.create_index('ix_experiment_def_conversion_event', 'experiment_definitions', ['conversion_event'])

def downgrade():
    op.drop_index('ix_experiment_def_conversion_event', table_name='experiment_definitions')
    op.drop_column('experiment_definitions', 'conversion_event')
    op.drop_column('experiment_definitions', 'adaptive')
