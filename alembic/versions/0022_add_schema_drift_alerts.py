"""add schema drift alerts

Revision ID: 0022_add_schema_drift_alerts
Revises: 0021_add_promoted_flag_model_version
Create Date: 2025-08-24 18:00:00.000000
"""
from alembic import op
import sqlalchemy as sa

revision = '0022_add_schema_drift_alerts'
down_revision = '0021_add_promoted_flag_model_version'
branch_labels = None
depends_on = None


def upgrade():
    try:
        op.create_table(
            'schema_drift_alerts',
            sa.Column('id', sa.Integer, primary_key=True),
            sa.Column('event_name', sa.String(128), index=True),
            sa.Column('prop_name', sa.String(128), index=True, nullable=True),
            sa.Column('change_type', sa.String(32), index=True),
            sa.Column('occurrences', sa.Integer, server_default='1'),
            sa.Column('first_seen', sa.DateTime, server_default=sa.text('CURRENT_TIMESTAMP'), index=True),
            sa.Column('last_seen', sa.DateTime, server_default=sa.text('CURRENT_TIMESTAMP'), index=True),
            sa.Column('status', sa.String(16), server_default='open', index=True),
            sa.Column('details', sa.JSON, nullable=True),
        )
        op.create_index('ix_schema_drift_key', 'schema_drift_alerts', ['event_name','prop_name','change_type'], unique=True)
    except Exception:
        pass


def downgrade():
    try:
        op.drop_table('schema_drift_alerts')
    except Exception:
        pass
