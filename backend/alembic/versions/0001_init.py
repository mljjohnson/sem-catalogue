from alembic import op
import sqlalchemy as sa


revision = "0001_init"
down_revision = None
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "pages_sem_inventory",
        sa.Column("page_id", sa.String(length=64), primary_key=True),
        sa.Column("url", sa.Text(), nullable=False),
        sa.Column("canonical_url", sa.Text(), nullable=False),
        sa.Column("status_code", sa.Integer(), nullable=False),
        sa.Column("primary_category", sa.String(length=255)),
        sa.Column("vertical", sa.String(length=255)),
        sa.Column("template_type", sa.String(length=128)),
        sa.Column("has_coupons", sa.Boolean(), nullable=False, server_default=sa.text("0")),
        # MySQL does not allow defaults on JSON columns; enforce default in application layer
        sa.Column("brand_list", sa.JSON(), nullable=False),
        sa.Column("brand_positions", sa.Text()),
        sa.Column("first_seen", sa.Date(), nullable=False),
        sa.Column("last_seen", sa.Date(), nullable=False),
        sa.Column("ga_sessions_14d", sa.Integer()),
        sa.Column("ga_key_events_14d", sa.Integer()),
    )
    op.create_index("idx_pages_last_seen", "pages_sem_inventory", ["last_seen"]) 
    op.create_index("idx_pages_status", "pages_sem_inventory", ["status_code"]) 
    op.create_index("idx_pages_category", "pages_sem_inventory", ["primary_category"]) 

    op.create_table(
        "page_brands",
        sa.Column("id", sa.Integer(), primary_key=True, autoincrement=True),
        sa.Column("page_id", sa.String(length=64), nullable=False),
        sa.Column("brand_slug", sa.String(length=255), nullable=False),
        sa.Column("brand_name", sa.String(length=255), nullable=False),
        sa.Column("position", sa.String(length=8)),
        sa.Column("module_type", sa.String(length=64)),
    )
    op.create_index("idx_page_brands_page_id", "page_brands", ["page_id"]) 


def downgrade() -> None:
    op.drop_index("idx_page_brands_page_id", table_name="page_brands")
    op.drop_table("page_brands")
    op.drop_index("idx_pages_category", table_name="pages_sem_inventory")
    op.drop_index("idx_pages_status", table_name="pages_sem_inventory")
    op.drop_index("idx_pages_last_seen", table_name="pages_sem_inventory")
    op.drop_table("pages_sem_inventory")


