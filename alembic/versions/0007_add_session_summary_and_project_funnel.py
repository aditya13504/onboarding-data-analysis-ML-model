from alembic import op
import sqlalchemy as sa

revision = '0007_add_session_summary_and_project_funnel'
down_revision = '0006_add_operational_tables'
branch_labels = None
depends_on = None


def upgrade():
    with op.batch_alter_table('funnel_metrics') as batch_op:
        batch_op.add_column(sa.Column('project', sa.String(length=64)))
        batch_op.create_index('ix_funnel_metrics_project', ['project'])
    op.create_table(
        'session_summary',
        sa.Column('id', sa.Integer, primary_key=True),
        sa.Column('user_id', sa.String(64), index=True),
        sa.Column('session_id', sa.String(64), index=True),
        sa.Column('project', sa.String(64), index=True),
        sa.Column('start_ts', sa.DateTime, index=True),
        sa.Column('end_ts', sa.DateTime, index=True),
        sa.Column('duration_sec', sa.Float),
        sa.Column('steps_count', sa.Integer),
        sa.Column('completion_ratio', sa.Float),
        sa.Column('created_at', sa.DateTime, index=True),
    )


def downgrade():
    op.drop_table('session_summary')
    with op.batch_alter_table('funnel_metrics') as batch_op:
        batch_op.drop_index('ix_funnel_metrics_project')
        batch_op.drop_column('project')
