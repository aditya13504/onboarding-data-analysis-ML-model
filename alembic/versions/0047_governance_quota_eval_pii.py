"""governance quota eval pii

Revision ID: 0047_governance_quota_eval_pii
Revises: 0046_add_idempotency_key_raw_events
Create Date: 2025-08-25
"""
from alembic import op
import sqlalchemy as sa

# revision identifiers, used by Alembic.
revision = '0047_governance_quota_eval_pii'
down_revision = '0046_add_idempotency_key_raw_events'
branch_labels = None
depends_on = None

def upgrade():
    # ApiKey quotas
    with op.batch_alter_table('api_keys') as b:
        b.add_column(sa.Column('daily_quota', sa.Integer(), nullable=True))
        b.add_column(sa.Column('monthly_quota', sa.Integer(), nullable=True))
        b.create_index('ix_api_keys_daily_quota', ['daily_quota'])
        b.create_index('ix_api_keys_monthly_quota', ['monthly_quota'])
    # Observed PII table
    op.create_table(
        'observed_pii_properties',
        sa.Column('id', sa.Integer(), primary_key=True),
        sa.Column('event_name', sa.String(length=128), nullable=True, index=True),
        sa.Column('prop_name', sa.String(length=128), nullable=False, index=True),
        sa.Column('detection_type', sa.String(length=32), nullable=False, index=True),
        sa.Column('sample_hash', sa.String(length=64), nullable=True),
        sa.Column('first_seen', sa.DateTime(), nullable=False, index=True),
        sa.Column('last_seen', sa.DateTime(), nullable=False, index=True),
        sa.Column('occurrences', sa.Integer(), nullable=False, server_default='1'),
        sa.Column('project', sa.String(length=64), nullable=True, index=True),
    )
    op.create_index('ux_pii_prop_key', 'observed_pii_properties', ['prop_name','project'], unique=True)
    # Project quotas
    op.create_table(
        'project_quotas',
        sa.Column('id', sa.Integer(), primary_key=True),
        sa.Column('project', sa.String(length=64), nullable=False, unique=True, index=True),
        sa.Column('daily_event_limit', sa.Integer(), nullable=True),
        sa.Column('monthly_event_limit', sa.Integer(), nullable=True),
        sa.Column('enforced', sa.Integer(), nullable=False, server_default='1', index=True),
        sa.Column('created_at', sa.DateTime(), nullable=False, index=True),
        sa.Column('updated_at', sa.DateTime(), nullable=False, index=True),
    )
    op.create_index('ix_quota_project_enforced', 'project_quotas', ['project','enforced'])
    # Model evaluations
    op.create_table(
        'model_evaluations',
        sa.Column('id', sa.Integer(), primary_key=True),
        sa.Column('model_name', sa.String(length=64), nullable=False, index=True),
        sa.Column('candidate_version', sa.String(length=32), nullable=False, index=True),
        sa.Column('baseline_version', sa.String(length=32), nullable=True, index=True),
        sa.Column('metric_name', sa.String(length=64), nullable=False, index=True),
        sa.Column('candidate_score', sa.Float(), nullable=False),
        sa.Column('baseline_score', sa.Float(), nullable=True),
        sa.Column('delta', sa.Float(), nullable=True, index=True),
        sa.Column('threshold', sa.Float(), nullable=True),
        sa.Column('decision', sa.String(length=16), nullable=True, index=True),
        sa.Column('decided_at', sa.DateTime(), nullable=True, index=True),
        sa.Column('created_at', sa.DateTime(), nullable=False, index=True),
        sa.Column('details', sa.JSON(), nullable=True),
    )
    op.create_index('ix_model_eval_key', 'model_evaluations', ['model_name','candidate_version','metric_name'], unique=True)


def downgrade():
    op.drop_index('ix_model_eval_key', table_name='model_evaluations')
    op.drop_table('model_evaluations')
    op.drop_index('ix_quota_project_enforced', table_name='project_quotas')
    op.drop_table('project_quotas')
    op.drop_index('ux_pii_prop_key', table_name='observed_pii_properties')
    op.drop_table('observed_pii_properties')
    with op.batch_alter_table('api_keys') as b:
        b.drop_index('ix_api_keys_daily_quota')
        b.drop_index('ix_api_keys_monthly_quota')
        b.drop_column('daily_quota')
        b.drop_column('monthly_quota')
