from __future__ import annotations
from alembic import op
import sqlalchemy as sa

revision = '0001_initial'
down_revision = None
branch_labels = None
depends_on = None


def upgrade():
    op.create_table(
        'raw_events',
        sa.Column('id', sa.Integer, primary_key=True),
        sa.Column('user_id', sa.String(64), index=True),
        sa.Column('session_id', sa.String(64), index=True),
        sa.Column('event_name', sa.String(128), index=True),
        sa.Column('ts', sa.DateTime, index=True),
        sa.Column('props', sa.JSON),
    )
    op.create_table(
        'funnel_metrics',
        sa.Column('id', sa.Integer, primary_key=True),
        sa.Column('step_name', sa.String(128), index=True),
        sa.Column('step_order', sa.Integer, index=True),
        sa.Column('users_entered', sa.Integer),
        sa.Column('users_converted', sa.Integer),
        sa.Column('drop_off', sa.Integer),
        sa.Column('conversion_rate', sa.Float),
        sa.Column('calc_ts', sa.DateTime, index=True),
    )
    op.create_table(
        'friction_clusters',
        sa.Column('id', sa.Integer, primary_key=True),
        sa.Column('label', sa.String(128)),
        sa.Column('size', sa.Integer),
        sa.Column('drop_off_users', sa.Integer),
        sa.Column('impact_score', sa.Float),
        sa.Column('features_summary', sa.JSON),
        sa.Column('model_version', sa.String(32)),
        sa.Column('created_at', sa.DateTime, index=True),
    )
    op.create_table(
        'insights',
        sa.Column('id', sa.Integer, primary_key=True),
        sa.Column('cluster_id', sa.Integer, sa.ForeignKey('friction_clusters.id')),
        sa.Column('title', sa.String(256)),
        sa.Column('recommendation', sa.String(1024)),
        sa.Column('priority', sa.Integer, index=True),
        sa.Column('impact_score', sa.Float),
        sa.Column('created_at', sa.DateTime, index=True),
    )


def downgrade():
    op.drop_table('insights')
    op.drop_table('friction_clusters')
    op.drop_table('funnel_metrics')
    op.drop_table('raw_events')
