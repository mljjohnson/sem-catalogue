from alembic import op
import sqlalchemy as sa


revision = "0004_add_products"
down_revision = "0003_add_has_promotions"
branch_labels = None
depends_on = None


def upgrade() -> None:
    # Add product columns to pages_sem_inventory
    with op.batch_alter_table("pages_sem_inventory") as batch_op:
        # MySQL does not allow defaults on JSON columns; enforce default in application layer
        batch_op.add_column(sa.Column("product_list", sa.JSON(), nullable=False))
        batch_op.add_column(sa.Column("product_positions", sa.Text()))
    # Create page_products table
    op.create_table(
        "page_products",
        sa.Column("id", sa.Integer(), primary_key=True, autoincrement=True),
        sa.Column("page_id", sa.String(length=64), nullable=False),
        sa.Column("product_name", sa.String(length=255), nullable=False),
        sa.Column("position", sa.String(length=8)),
        sa.Column("module_type", sa.String(length=64)),
    )
    op.create_index("idx_page_products_page_id", "page_products", ["page_id"]) 


def downgrade() -> None:
    op.drop_index("idx_page_products_page_id", table_name="page_products")
    op.drop_table("page_products")
    with op.batch_alter_table("pages_sem_inventory") as batch_op:
        batch_op.drop_column("product_positions")
        batch_op.drop_column("product_list")



