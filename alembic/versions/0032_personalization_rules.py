"""Add personalization rules table

Revision ID: 0032_personalization_rules
Revises: 0031_rbac_personalization_lineage
Create Date: 2025-08-25
"""
from alembic import op
import sqlalchemy as sa

revision = '0032_personalization_rules'
down_revision = '0031_rbac_personalization_lineage'
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        'personalization_rules',
        sa.Column('id', sa.Integer(), primary_key=True),
        sa.Column('key', sa.String(length=128), nullable=False),
        sa.Column('project', sa.String(length=64), nullable=True),
        sa.Column('condition_expr', sa.String(length=512), nullable=False),
        sa.Column('recommendation', sa.String(length=64), nullable=False),
        sa.Column('rationale_template', sa.String(length=256), nullable=True),
        sa.Column('cooldown_hours', sa.Integer(), nullable=False, server_default='72'),
        sa.Column('active', sa.Integer(), nullable=False, server_default='1'),
        sa.Column('created_at', sa.DateTime(), nullable=False),
        sa.Column('last_triggered_at', sa.DateTime(), nullable=True),
    )
    op.create_index('ux_rule_key_project', 'personalization_rules', ['key','project'], unique=True)
    op.create_index('ix_rule_active', 'personalization_rules', ['active'])
    op.create_index('ix_rule_recommendation', 'personalization_rules', ['recommendation'])


def downgrade() -> None:
    op.drop_table('personalization_rules')
