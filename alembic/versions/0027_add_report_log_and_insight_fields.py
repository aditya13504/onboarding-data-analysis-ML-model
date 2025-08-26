"""add report log insight delta fields

Revision ID: 0027_add_report_log_and_insight_fields
Revises: 0026_recommendation_engine_expansion
Create Date: 2025-08-24
"""
from __future__ import annotations

from alembic import op
import sqlalchemy as sa

revision = '0027_add_report_log_and_insight_fields'
down_revision = '0026_recommendation_engine_expansion'
branch_labels = None
depends_on = None


def upgrade():
    with op.batch_alter_table('report_logs') as batch:
        batch.add_column(sa.Column('new_insights', sa.Integer(), server_default='0', nullable=False))
        batch.add_column(sa.Column('progressed_insights', sa.Integer(), server_default='0', nullable=False))
        batch.add_column(sa.Column('shipped_insights', sa.Integer(), server_default='0', nullable=False))
        batch.add_column(sa.Column('reopened_insights', sa.Integer(), server_default='0', nullable=False))


def downgrade():
    with op.batch_alter_table('report_logs') as batch:
        batch.drop_column('reopened_insights')
        batch.drop_column('shipped_insights')
        batch.drop_column('progressed_insights')
        batch.drop_column('new_insights')
