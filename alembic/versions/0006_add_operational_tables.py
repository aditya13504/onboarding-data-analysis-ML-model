from alembic import op
import sqlalchemy as sa

revision = '0006_add_operational_tables'
down_revision = '0005_add_project_column'
branch_labels = None
depends_on = None


def upgrade():
    op.create_table(
        'api_keys',
        sa.Column('id', sa.Integer, primary_key=True),
        sa.Column('key', sa.String(128), unique=True, index=True),
        sa.Column('project', sa.String(64), index=True),
        sa.Column('scopes', sa.String(256)),
        sa.Column('active', sa.Integer, index=True, server_default='1'),
        sa.Column('created_at', sa.DateTime, index=True),
    )
    op.create_table(
        'connector_state',
        sa.Column('id', sa.Integer, primary_key=True),
        sa.Column('connector_name', sa.String(64), unique=True, index=True),
        sa.Column('last_since_ts', sa.DateTime),
        sa.Column('last_until_ts', sa.DateTime),
        sa.Column('cursor', sa.String(256)),
        sa.Column('updated_at', sa.DateTime, index=True),
    )
    op.create_table(
        'audit_log',
        sa.Column('id', sa.Integer, primary_key=True),
        sa.Column('ts', sa.DateTime, index=True),
        sa.Column('endpoint', sa.String(256), index=True),
        sa.Column('method', sa.String(16)),
        sa.Column('status', sa.Integer, index=True),
        sa.Column('api_key', sa.String(128), index=True),
        sa.Column('project', sa.String(64), index=True),
        sa.Column('duration_ms', sa.Integer),
    )
    op.create_table(
        'model_artifacts',
        sa.Column('id', sa.Integer, primary_key=True),
        sa.Column('model_name', sa.String(64), index=True),
        sa.Column('version', sa.String(32), index=True),
        sa.Column('artifact', sa.JSON),
        sa.Column('created_at', sa.DateTime, index=True),
    )


def downgrade():
    op.drop_table('model_artifacts')
    op.drop_table('audit_log')
    op.drop_table('connector_state')
    op.drop_table('api_keys')
