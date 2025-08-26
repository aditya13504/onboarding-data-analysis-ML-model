from alembic import op
import sqlalchemy as sa

revision = '0008_add_insight_status'
down_revision = '0007_add_session_summary_and_project_funnel'
branch_labels = None
depends_on = None

def upgrade():
    with op.batch_alter_table('insights') as batch_op:
        batch_op.add_column(sa.Column('status', sa.String(length=16), nullable=False, server_default='new'))
        batch_op.add_column(sa.Column('accepted_at', sa.DateTime(), nullable=True))
        batch_op.create_index('ix_insights_status', ['status'])
        batch_op.create_index('ix_insights_accepted_at', ['accepted_at'])


def downgrade():
    with op.batch_alter_table('insights') as batch_op:
        batch_op.drop_index('ix_insights_status')
        batch_op.drop_index('ix_insights_accepted_at')
        batch_op.drop_column('status')
        batch_op.drop_column('accepted_at')
