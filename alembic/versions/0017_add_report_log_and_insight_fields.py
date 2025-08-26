"""add report log table and insight category/confidence

Revision ID: 0017_add_report_log_and_insight_fields
Revises: 0016_add_modeling_tables
Create Date: 2025-08-24 14:10:00.000000
"""
from alembic import op
import sqlalchemy as sa

revision = '0017_add_report_log_and_insight_fields'
down_revision = '0016_add_modeling_tables'
branch_labels = None
depends_on = None

def upgrade():
    with op.batch_alter_table('insights') as batch:
        try:
            batch.add_column(sa.Column('category', sa.String(64), nullable=True))
        except Exception:
            pass
        try:
            batch.add_column(sa.Column('confidence', sa.Float, nullable=True))
        except Exception:
            pass
        try:
            batch.create_index('ix_insights_category', ['category'])
        except Exception:
            pass
    op.create_table(
        'report_logs',
        sa.Column('id', sa.Integer, primary_key=True),
        sa.Column('report_type', sa.String(64), nullable=False),
        sa.Column('generated_at', sa.DateTime, server_default=sa.text('CURRENT_TIMESTAMP'), nullable=False),
        sa.Column('content', sa.String(8192), nullable=False),
        sa.Column('insights_count', sa.Integer, nullable=False, server_default='0'),
        sa.Column('funnel_steps', sa.Integer, nullable=False, server_default='0'),
        sa.Column('published_slack', sa.Integer, nullable=False, server_default='0'),
        sa.Column('published_notion', sa.Integer, nullable=False, server_default='0'),
    )
    try:
        op.create_index('ix_report_logs_type_time', 'report_logs', ['report_type','generated_at'])
    except Exception:
        pass
    # Performance index for archive compaction grouping
    try:
        op.create_index('ix_raw_events_archive_proj_event_ts', 'raw_events_archive', ['project','event_name','ts'])
    except Exception:
        pass

def downgrade():
    try:
        op.drop_table('report_logs')
    except Exception:
        pass
    try:
        op.drop_index('ix_raw_events_archive_proj_event_ts', table_name='raw_events_archive')
    except Exception:
        pass
    with op.batch_alter_table('insights') as batch:
        for col in ['category','confidence']:
            try:
                batch.drop_column(col)
            except Exception:
                pass
