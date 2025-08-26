"""add alert rules and logs

Revision ID: 0037_alert_rules
Revises: 0036_experiment_decision_snapshots
Create Date: 2025-08-25
"""
from alembic import op
import sqlalchemy as sa

revision = '0037_alert_rules'
down_revision = '0036_experiment_decision_snapshots'
branch_labels = None
depends_on = None

def upgrade():
    op.create_table(
        'alert_rules',
        sa.Column('id', sa.Integer(), primary_key=True),
        sa.Column('rule_type', sa.String(length=32), index=True, nullable=False),
        sa.Column('condition', sa.JSON(), nullable=False),
        sa.Column('channels', sa.JSON(), nullable=False, server_default='{}'),
        sa.Column('cooldown_minutes', sa.Integer(), nullable=False, server_default='60'),
        sa.Column('active', sa.Integer(), nullable=False, server_default='1', index=True),
        sa.Column('last_fired_at', sa.DateTime(), nullable=True, index=True),
        sa.Column('created_at', sa.DateTime(), server_default=sa.text('CURRENT_TIMESTAMP'), nullable=False, index=True)
    )
    op.create_index('ix_alert_rule_type_active', 'alert_rules', ['rule_type','active'])
    op.create_table(
        'alert_logs',
        sa.Column('id', sa.Integer(), primary_key=True),
        sa.Column('rule_id', sa.Integer(), index=True, nullable=False),
        sa.Column('rule_type', sa.String(length=32), index=True, nullable=False),
        sa.Column('message', sa.String(length=512), nullable=False),
        sa.Column('context', sa.JSON(), nullable=True),
        sa.Column('created_at', sa.DateTime(), server_default=sa.text('CURRENT_TIMESTAMP'), nullable=False, index=True)
    )
    op.create_index('ix_alert_log_rule', 'alert_logs', ['rule_id','created_at'])


def downgrade():
    op.drop_index('ix_alert_log_rule', table_name='alert_logs')
    op.drop_table('alert_logs')
    op.drop_index('ix_alert_rule_type_active', table_name='alert_rules')
    op.drop_table('alert_rules')
