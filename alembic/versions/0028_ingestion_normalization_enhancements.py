"""ingestion normalization enhancements: add event identity mapping tables

Revision ID: 0028_ingestion_normalization_enhancements
Revises: 0027_add_report_log_and_insight_fields
Create Date: 2025-08-24
"""
from alembic import op
import sqlalchemy as sa

# revision identifiers, used by Alembic.
revision = '0028_ingestion_normalization_enhancements'
down_revision = '0027_add_report_log_and_insight_fields'
branch_labels = None
depends_on = None

def upgrade() -> None:
    op.create_table(
        'user_identity_map',
        sa.Column('id', sa.Integer, primary_key=True),
        sa.Column('primary_user_id', sa.String(64), index=True, nullable=False),
        sa.Column('alias_user_id', sa.String(64), index=True, nullable=False),
        sa.Column('source', sa.String(32), index=True),
        sa.Column('created_at', sa.DateTime, server_default=sa.func.now(), index=True),
    )
    op.create_index('ix_user_identity_unique', 'user_identity_map', ['primary_user_id','alias_user_id'], unique=True)
    op.create_table(
        'event_name_map',
        sa.Column('id', sa.Integer, primary_key=True),
        sa.Column('canonical_name', sa.String(128), index=True, nullable=False),
        sa.Column('variant_name', sa.String(128), index=True, nullable=False),
        sa.Column('source', sa.String(32), index=True),
        sa.Column('created_at', sa.DateTime, server_default=sa.func.now(), index=True),
    )
    op.create_index('ix_event_name_variant_unique', 'event_name_map', ['canonical_name','variant_name'], unique=True)


def downgrade() -> None:
    op.drop_index('ix_event_name_variant_unique', table_name='event_name_map')
    op.drop_table('event_name_map')
    op.drop_index('ix_user_identity_unique', table_name='user_identity_map')
    op.drop_table('user_identity_map')
