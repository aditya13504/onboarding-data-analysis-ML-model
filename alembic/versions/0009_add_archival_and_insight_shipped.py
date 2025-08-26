from alembic import op
import sqlalchemy as sa

revision = '0009_add_archival_and_insight_shipped'
down_revision = '0008_add_insight_status'
branch_labels = None
depends_on = None

def upgrade():
    op.create_table(
        'raw_events_archive',
        sa.Column('id', sa.Integer, primary_key=True),
        sa.Column('event_id', sa.String(128), index=True),
        sa.Column('user_id', sa.String(64), index=True),
        sa.Column('session_id', sa.String(64), index=True),
        sa.Column('event_name', sa.String(128), index=True),
        sa.Column('ts', sa.DateTime, index=True),
        sa.Column('props', sa.JSON),
        sa.Column('schema_version', sa.String(16), index=True),
        sa.Column('project', sa.String(64), index=True),
        sa.Column('archived_at', sa.DateTime, index=True),
    )
    with op.batch_alter_table('insights') as batch_op:
        batch_op.add_column(sa.Column('shipped_at', sa.DateTime(), nullable=True))
        batch_op.create_index('ix_insights_shipped_at', ['shipped_at'])


def downgrade():
    with op.batch_alter_table('insights') as batch_op:
        batch_op.drop_index('ix_insights_shipped_at')
        batch_op.drop_column('shipped_at')
    op.drop_table('raw_events_archive')
