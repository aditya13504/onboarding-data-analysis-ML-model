"""operational enhancements: kafka lag, suggestion pattern learning, secret rotation audit

Revision ID: 0051_operational_secret_kafka_learning
Revises: 0050_segmentation
Create Date: 2025-08-25
"""
from alembic import op
import sqlalchemy as sa

# revision identifiers, used by Alembic.
revision = '0051_operational_secret_kafka_learning'
down_revision = '0050_segmentation'
branch_labels = None
depends_on = None

def upgrade():
    # Kafka consumer lag table
    op.create_table(
        'kafka_consumer_lag',
        sa.Column('id', sa.Integer(), primary_key=True),
        sa.Column('group_id', sa.String(128), nullable=False, index=True),
        sa.Column('topic', sa.String(256), nullable=False, index=True),
        sa.Column('partition', sa.Integer(), nullable=False, index=True),
        sa.Column('end_offset', sa.Integer(), nullable=False),
        sa.Column('committed_offset', sa.Integer(), nullable=True),
        sa.Column('lag', sa.Integer(), nullable=True),
        sa.Column('captured_at', sa.DateTime(), nullable=False, index=True),
        sa.Column('details', sa.JSON(), nullable=True),
    )
    op.create_index('ix_kafka_lag_group_topic_part_time', 'kafka_consumer_lag', ['group_id','topic','partition','captured_at'])

    # Suggestion pattern learning columns
    with op.batch_alter_table('suggestion_patterns') as batch:
        batch.add_column(sa.Column('base_weight', sa.Float(), nullable=True))
        batch.add_column(sa.Column('dynamic_adjust', sa.Integer(), nullable=True, server_default='1'))

    # Secret rotation audit
    op.create_table(
        'secret_rotation_audit',
        sa.Column('id', sa.Integer(), primary_key=True),
        sa.Column('connector_name', sa.String(64), index=True),
        sa.Column('secret_key', sa.String(128), index=True),
        sa.Column('old_version', sa.Integer(), nullable=True),
        sa.Column('new_version', sa.Integer(), nullable=True),
        sa.Column('rotated_at', sa.DateTime(), nullable=False, index=True),
        sa.Column('status', sa.String(16), nullable=False, index=True),
        sa.Column('reason', sa.String(256), nullable=True),
    )
    op.create_index('ix_secret_rotation_key', 'secret_rotation_audit', ['connector_name','secret_key','rotated_at'])

    # Recommendation learning metrics
    op.create_table(
        'recommendation_learning_metrics',
        sa.Column('id', sa.Integer(), primary_key=True),
        sa.Column('recommendation', sa.String(64), index=True),
        sa.Column('window_start', sa.DateTime(), nullable=False, index=True),
        sa.Column('window_end', sa.DateTime(), nullable=False, index=True),
        sa.Column('impressions', sa.Integer(), nullable=False, server_default='0'),
        sa.Column('accepts', sa.Integer(), nullable=False, server_default='0'),
        sa.Column('dismisses', sa.Integer(), nullable=False, server_default='0'),
        sa.Column('converts', sa.Integer(), nullable=False, server_default='0'),
        sa.Column('ctr', sa.Float(), nullable=True),
        sa.Column('conversion_rate', sa.Float(), nullable=True),
        sa.Column('weight_adjustment', sa.Float(), nullable=True),
        sa.Column('computed_at', sa.DateTime(), nullable=False, index=True),
    )
    op.create_index('ux_rec_learning_window', 'recommendation_learning_metrics', ['recommendation','window_start','window_end'], unique=True)

    # Idempotency records table
    op.create_table(
        'idempotency_records',
        sa.Column('id', sa.Integer(), primary_key=True),
        sa.Column('operation_type', sa.String(64), nullable=False, index=True),
        sa.Column('idempotency_key', sa.String(128), nullable=False, index=True),
        sa.Column('request_hash', sa.String(64), nullable=False),
        sa.Column('result_data', sa.String(8192), nullable=True),
        sa.Column('status', sa.String(16), nullable=False, index=True),
        sa.Column('created_at', sa.DateTime(), nullable=False, index=True),
        sa.Column('expires_at', sa.DateTime(), nullable=False, index=True),
    )
    op.create_index('ux_idempotency_key', 'idempotency_records', ['operation_type','idempotency_key'], unique=True)

    # Create connector_state table for cursor tracking and failure auditing
    op.create_table('connector_state',
        sa.Column('connector_name', sa.String(100), nullable=False),
        sa.Column('cursor', sa.Text(), nullable=True),
        sa.Column('last_since_ts', sa.DateTime(), nullable=True),
        sa.Column('failure_count', sa.Integer(), nullable=False, index=True),
        sa.Column('last_error', sa.String(255), nullable=True),
        sa.Column('created_at', sa.DateTime(), nullable=False, index=True),
        sa.Column('updated_at', sa.DateTime(), nullable=False, index=True),
        sa.PrimaryKeyConstraint('connector_name'),
    )


def downgrade():
    op.drop_table('connector_state')
    op.drop_index('ux_idempotency_key', table_name='idempotency_records')
    op.drop_table('idempotency_records')
    op.drop_index('ux_rec_learning_window', table_name='recommendation_learning_metrics')
    op.drop_table('recommendation_learning_metrics')
    op.drop_index('ix_secret_rotation_key', table_name='secret_rotation_audit')
    op.drop_table('secret_rotation_audit')
    with op.batch_alter_table('suggestion_patterns') as batch:
        batch.drop_column('base_weight')
        batch.drop_column('dynamic_adjust')
    op.drop_index('ix_kafka_lag_group_topic_part_time', table_name='kafka_consumer_lag')
    op.drop_table('kafka_consumer_lag')
