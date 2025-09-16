from alembic import op
import sqlalchemy as sa


revision = "0003_add_has_promotions"
down_revision = "0002_ai_extracts"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column(
        "pages_sem_inventory",
        sa.Column("has_promotions", sa.Boolean(), nullable=False, server_default=sa.text("0")),
    )
    op.create_index("idx_pages_has_promotions", "pages_sem_inventory", ["has_promotions"]) 


def downgrade() -> None:
    op.drop_index("idx_pages_has_promotions", table_name="pages_sem_inventory")
    op.drop_column("pages_sem_inventory", "has_promotions")



