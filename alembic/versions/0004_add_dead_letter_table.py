from alembic import op
import sqlalchemy as sa

revision = '0004_add_dead_letter_table'
down_revision = '0003_add_schema_version_and_model_versions'
branch_labels = None
depends_on = None


def upgrade():
    op.create_table(
        'ingestion_dead_letter',
        sa.Column('id', sa.Integer, primary_key=True),
        sa.Column('payload', sa.JSON),
        sa.Column('error', sa.String(512)),
        sa.Column('created_at', sa.DateTime, index=True),
    )


def downgrade():
    op.drop_table('ingestion_dead_letter')
