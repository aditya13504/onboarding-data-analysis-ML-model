"""recommendation engine expansion

Revision ID: 0026_recommendation_engine_expansion
Revises: 0025_add_cluster_labels
Create Date: 2025-08-24
"""
from __future__ import annotations

from alembic import op
import sqlalchemy as sa

revision = '0026_recommendation_engine_expansion'
down_revision = '0025_add_cluster_labels'
branch_labels = None
depends_on = None


def upgrade():
    # Add new columns to insights
    with op.batch_alter_table('insights') as batch:
        batch.add_column(sa.Column('rationale', sa.String(length=1024), nullable=True))
        batch.add_column(sa.Column('score', sa.Float(), nullable=True))
        batch.create_index('ix_insight_score', ['score'])

    op.create_table(
        'suggestion_patterns',
        sa.Column('id', sa.Integer(), primary_key=True),
        sa.Column('key', sa.String(length=128), nullable=False),
        sa.Column('description', sa.String(length=512), nullable=False),
        sa.Column('recommendation', sa.String(length=1024), nullable=False),
        sa.Column('category', sa.String(length=64), nullable=False),
        sa.Column('weight', sa.Float(), nullable=False, server_default='1.0'),
        sa.Column('created_at', sa.DateTime(), server_default=sa.text('CURRENT_TIMESTAMP'), nullable=False),
    )
    op.create_index('ix_suggestion_patterns_key', 'suggestion_patterns', ['key'], unique=True)
    op.create_index('ix_suggestion_patterns_category', 'suggestion_patterns', ['category'])

    op.create_table(
        'insight_suppressions',
        sa.Column('id', sa.Integer(), primary_key=True),
        sa.Column('cluster_id', sa.Integer(), nullable=True),
        sa.Column('pattern_key', sa.String(length=128), nullable=True),
        sa.Column('reason', sa.String(length=256), nullable=True),
        sa.Column('author', sa.String(length=64), nullable=True),
        sa.Column('expires_at', sa.DateTime(), nullable=True),
        sa.Column('created_at', sa.DateTime(), server_default=sa.text('CURRENT_TIMESTAMP'), nullable=False),
        sa.Column('active', sa.Integer(), server_default='1', nullable=False),
    )
    op.create_index('ix_suppression_cluster_pattern', 'insight_suppressions', ['cluster_id','pattern_key'])
    op.create_index('ix_insight_suppressions_active', 'insight_suppressions', ['active'])

    op.create_table(
        'insight_feedback',
        sa.Column('id', sa.Integer(), primary_key=True),
        sa.Column('insight_id', sa.Integer(), nullable=False),
        sa.Column('feedback', sa.String(length=8), nullable=False),
        sa.Column('author', sa.String(length=64), nullable=True),
        sa.Column('created_at', sa.DateTime(), server_default=sa.text('CURRENT_TIMESTAMP'), nullable=False),
        sa.Column('weight', sa.Float(), nullable=False, server_default='1.0'),
    )
    op.create_index('ix_insight_feedback_insight', 'insight_feedback', ['insight_id'])
    op.create_index('ix_insight_feedback_feedback', 'insight_feedback', ['feedback'])


def downgrade():
    op.drop_index('ix_insight_feedback_feedback', table_name='insight_feedback')
    op.drop_index('ix_insight_feedback_insight', table_name='insight_feedback')
    op.drop_table('insight_feedback')
    op.drop_index('ix_insight_suppressions_active', table_name='insight_suppressions')
    op.drop_index('ix_suppression_cluster_pattern', table_name='insight_suppressions')
    op.drop_table('insight_suppressions')
    op.drop_index('ix_suggestion_patterns_category', table_name='suggestion_patterns')
    op.drop_index('ix_suggestion_patterns_key', table_name='suggestion_patterns')
    op.drop_table('suggestion_patterns')
    with op.batch_alter_table('insights') as batch:
        batch.drop_index('ix_insight_score')
        batch.drop_column('score')
        batch.drop_column('rationale')
