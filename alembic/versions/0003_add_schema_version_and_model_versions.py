from alembic import op
import sqlalchemy as sa

revision = '0003_add_schema_version_and_model_versions'
down_revision = '0002_add_event_id'
branch_labels = None
depends_on = None


def upgrade():
    # Add schema_version column if not exists (SQLite friendly batch op)
    with op.batch_alter_table('raw_events') as batch_op:
        batch_op.add_column(sa.Column('schema_version', sa.String(length=16)))
        # New composite indexes
        batch_op.create_index('ix_event_name_ts', ['event_name', 'ts'])
        batch_op.create_index('ix_user_ts', ['user_id', 'ts'])

    op.create_table(
        'model_versions',
        sa.Column('id', sa.Integer, primary_key=True),
        sa.Column('model_name', sa.String(64), index=True),
        sa.Column('version', sa.String(32), index=True),
        sa.Column('created_at', sa.DateTime, index=True),
        sa.Column('notes', sa.String(512), nullable=True),
        sa.Column('active', sa.Integer, default=1, index=True),
    )


def downgrade():
    op.drop_table('model_versions')
    with op.batch_alter_table('raw_events') as batch_op:
        batch_op.drop_index('ix_event_name_ts')
        batch_op.drop_index('ix_user_ts')
        batch_op.drop_column('schema_version')
