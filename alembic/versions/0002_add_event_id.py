from alembic import op
import sqlalchemy as sa

revision = '0002_add_event_id'
down_revision = '0001_initial'
branch_labels = None
depends_on = None


def upgrade():
    with op.batch_alter_table('raw_events') as batch_op:
        batch_op.add_column(sa.Column('event_id', sa.String(128)))
        batch_op.create_unique_constraint('uq_raw_events_event_id', ['event_id'])
        batch_op.create_index('ix_raw_events_event_id', ['event_id'])


def downgrade():
    with op.batch_alter_table('raw_events') as batch_op:
        batch_op.drop_index('ix_raw_events_event_id')
        batch_op.drop_constraint('uq_raw_events_event_id', type_='unique')
        batch_op.drop_column('event_id')