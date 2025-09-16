from alembic import op
import sqlalchemy as sa


revision = "0002_ai_extracts"
down_revision = "0001_init"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "page_ai_extracts",
        sa.Column("id", sa.Integer(), primary_key=True, autoincrement=True),
        sa.Column("page_id", sa.String(length=64), nullable=False),
        sa.Column("url", sa.Text(), nullable=False),
        sa.Column("created_at", sa.String(length=32), nullable=False),
        sa.Column("html_bytes", sa.Integer(), nullable=False, server_default=sa.text("0")),
        sa.Column("screenshot_bytes", sa.Integer(), nullable=False, server_default=sa.text("0")),
        sa.Column("data", sa.JSON(), nullable=False, server_default=sa.text("'{}'")),
    )
    op.create_index("idx_ai_extracts_page", "page_ai_extracts", ["page_id"]) 


def downgrade() -> None:
    op.drop_index("idx_ai_extracts_page", table_name="page_ai_extracts")
    op.drop_table("page_ai_extracts")




