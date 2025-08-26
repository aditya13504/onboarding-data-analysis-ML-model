"""add attempts column to ingestion_dead_letter

Revision ID: 0014_add_dlq_attempts
Revises: 0013_add_connector_cursor
Create Date: 2025-08-24 12:10:00.000000
"""
from alembic import op
import sqlalchemy as sa

revision = '0014_add_dlq_attempts'
down_revision = '0013_add_connector_cursor'
branch_labels = None
depends_on = None


def upgrade():
    op.add_column('ingestion_dead_letter', sa.Column('attempts', sa.Integer(), nullable=False, server_default='0'))
    op.create_index('ix_ingestion_dead_letter_attempts', 'ingestion_dead_letter', ['attempts'])


def downgrade():
    op.drop_index('ix_ingestion_dead_letter_attempts', table_name='ingestion_dead_letter')
    op.drop_column('ingestion_dead_letter', 'attempts')
