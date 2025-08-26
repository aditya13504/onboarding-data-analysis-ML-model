from alembic import op
import sqlalchemy as sa

revision = '0005_add_project_column'
down_revision = '0004_add_dead_letter_table'
branch_labels = None
depends_on = None


def upgrade():
    with op.batch_alter_table('raw_events') as batch_op:
        batch_op.add_column(sa.Column('project', sa.String(length=64)))
        batch_op.create_index('ix_raw_events_project', ['project'])
        batch_op.create_index('ix_project_event_ts', ['project', 'event_name', 'ts'])


def downgrade():
    with op.batch_alter_table('raw_events') as batch_op:
        batch_op.drop_index('ix_project_event_ts')
        batch_op.drop_index('ix_raw_events_project')
        batch_op.drop_column('project')
