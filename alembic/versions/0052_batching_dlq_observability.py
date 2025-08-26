"""Add high-volume batching and DLQ observability tasks.

Revision ID: 0052_batching_dlq_observability
Revises: 0051_operational_secret_kafka_learning
Create Date: 2025-08-25 10:30:00.000000

"""
from typing import Sequence, Union
from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql

# revision identifiers, used by Alembic.
revision: str = '0052_batching_dlq_observability'
down_revision: Union[str, None] = '0051_operational_secret_kafka_learning'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None

def upgrade() -> None:
    # Add batching configuration table
    op.create_table('batch_processing_config',
        sa.Column('id', sa.Integer(), nullable=False),
        sa.Column('pipeline_name', sa.String(100), nullable=False),
        sa.Column('max_batch_size', sa.Integer(), nullable=False),
        sa.Column('max_wait_seconds', sa.Integer(), nullable=False),
        sa.Column('parallel_workers', sa.Integer(), nullable=False),
        sa.Column('compression_enabled', sa.Boolean(), nullable=False),
        sa.Column('adaptive_enabled', sa.Boolean(), nullable=False),
        sa.Column('created_at', sa.DateTime(), nullable=False),
        sa.Column('updated_at', sa.DateTime(), nullable=False),
        sa.PrimaryKeyConstraint('id'),
    )
    op.create_index('ix_batch_config_pipeline', 'batch_processing_config', ['pipeline_name'], unique=True)

    # Add batch processing metrics table
    op.create_table('batch_processing_metrics',
        sa.Column('id', sa.Integer(), nullable=False),
        sa.Column('pipeline_name', sa.String(100), nullable=False),
        sa.Column('batch_size', sa.Integer(), nullable=False),
        sa.Column('processing_time_ms', sa.Integer(), nullable=False),
        sa.Column('throughput_eps', sa.Float(), nullable=False),
        sa.Column('compression_ratio', sa.Float(), nullable=True),
        sa.Column('created_at', sa.DateTime(), nullable=False),
        sa.PrimaryKeyConstraint('id'),
    )
    op.create_index('ix_batch_metrics_pipeline_time', 'batch_processing_metrics', ['pipeline_name', 'created_at'])

    # Add DLQ error patterns table
    op.create_table('dlq_error_patterns',
        sa.Column('id', sa.Integer(), nullable=False),
        sa.Column('error_signature', sa.String(255), nullable=False),
        sa.Column('error_category', sa.String(50), nullable=False),
        sa.Column('event_pattern', sa.String(255), nullable=True),
        sa.Column('occurrence_count', sa.Integer(), nullable=False),
        sa.Column('recovery_strategy', sa.String(50), nullable=False),
        sa.Column('auto_recoverable', sa.Boolean(), nullable=False),
        sa.Column('first_seen', sa.DateTime(), nullable=False),
        sa.Column('last_seen', sa.DateTime(), nullable=False),
        sa.PrimaryKeyConstraint('id'),
    )
    op.create_index('ix_dlq_patterns_signature', 'dlq_error_patterns', ['error_signature'], unique=True)
    op.create_index('ix_dlq_patterns_category', 'dlq_error_patterns', ['error_category'])

    # Add DLQ recovery attempts table
    op.create_table('dlq_recovery_attempts',
        sa.Column('id', sa.Integer(), nullable=False),
        sa.Column('dlq_event_id', sa.Integer(), nullable=False),
        sa.Column('recovery_strategy', sa.String(50), nullable=False),
        sa.Column('attempt_number', sa.Integer(), nullable=False),
        sa.Column('success', sa.Boolean(), nullable=False),
        sa.Column('error_message', sa.String(1000), nullable=True),
        sa.Column('attempted_at', sa.DateTime(), nullable=False),
        sa.PrimaryKeyConstraint('id'),
        sa.ForeignKeyConstraint(['dlq_event_id'], ['dead_letter_queue.id'], ),
    )
    op.create_index('ix_dlq_recovery_event_id', 'dlq_recovery_attempts', ['dlq_event_id'])
    op.create_index('ix_dlq_recovery_attempted_at', 'dlq_recovery_attempts', ['attempted_at'])

def downgrade() -> None:
    op.drop_index('ix_dlq_recovery_attempted_at', table_name='dlq_recovery_attempts')
    op.drop_index('ix_dlq_recovery_event_id', table_name='dlq_recovery_attempts')
    op.drop_table('dlq_recovery_attempts')
    op.drop_index('ix_dlq_patterns_category', table_name='dlq_error_patterns')
    op.drop_index('ix_dlq_patterns_signature', table_name='dlq_error_patterns')
    op.drop_table('dlq_error_patterns')
    op.drop_index('ix_batch_metrics_pipeline_time', table_name='batch_processing_metrics')
    op.drop_table('batch_processing_metrics')
    op.drop_index('ix_batch_config_pipeline', table_name='batch_processing_config')
    op.drop_table('batch_processing_config')
