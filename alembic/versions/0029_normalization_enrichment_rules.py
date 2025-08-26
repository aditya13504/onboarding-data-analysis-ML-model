"""normalization enrichment rules and event enrichment columns

Revision ID: 0029_normalization_enrichment_rules
Revises: 0028_ingestion_normalization_enhancements
Create Date: 2025-08-24
"""
from alembic import op
import sqlalchemy as sa

revision = '0029_normalization_enrichment_rules'
down_revision = '0028_ingestion_normalization_enhancements'
branch_labels = None
depends_on = None

def upgrade() -> None:
    op.add_column('raw_events', sa.Column('session_event_index', sa.Integer(), nullable=True))
    op.add_column('raw_events', sa.Column('step_index', sa.Integer(), nullable=True))
    op.create_index('ix_raw_events_session_event_index', 'raw_events', ['session_id','session_event_index'])
    op.create_index('ix_raw_events_step_index', 'raw_events', ['step_index'])
    op.create_table(
        'normalization_rules',
        sa.Column('id', sa.Integer, primary_key=True),
        sa.Column('rule_type', sa.String(32), index=True, nullable=False),
        sa.Column('payload', sa.JSON(), nullable=False),
        sa.Column('active', sa.Integer, index=True, server_default='1'),
        sa.Column('version', sa.Integer, index=True, server_default='1'),
        sa.Column('created_at', sa.DateTime, server_default=sa.func.now(), index=True),
        sa.Column('author', sa.String(64), index=True),
    )
    op.create_index('ix_norm_rule_type_active', 'normalization_rules', ['rule_type','active'])


def downgrade() -> None:
    op.drop_index('ix_norm_rule_type_active', table_name='normalization_rules')
    op.drop_table('normalization_rules')
    op.drop_index('ix_raw_events_step_index', table_name='raw_events')
    op.drop_index('ix_raw_events_session_event_index', table_name='raw_events')
    op.drop_column('raw_events', 'step_index')
    op.drop_column('raw_events', 'session_event_index')
