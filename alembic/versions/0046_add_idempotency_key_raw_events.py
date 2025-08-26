"""add idempotency key to raw events

Revision ID: 0046_add_idempotency_key_raw_events
Revises: 0045_privacy_deletion_audit
Create Date: 2025-08-25
"""
from alembic import op
import sqlalchemy as sa

revision = '0046_add_idempotency_key_raw_events'
down_revision = '0045_privacy_deletion_audit'
branch_labels = None
depends_on = None

def upgrade():
    with op.batch_alter_table('raw_events') as b:
        b.add_column(sa.Column('idempotency_key', sa.String(length=128), nullable=True))
        b.create_index('ix_event_idempotency', ['idempotency_key'])

def downgrade():
    with op.batch_alter_table('raw_events') as b:
        b.drop_index('ix_event_idempotency')
        b.drop_column('idempotency_key')
