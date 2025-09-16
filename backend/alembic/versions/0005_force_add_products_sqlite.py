from alembic import op
import sqlalchemy as sa


revision = "0005_force_add_products_sqlite"
down_revision = "0004_add_products"
branch_labels = None
depends_on = None


def upgrade() -> None:
    bind = op.get_bind()
    dialect = bind.dialect.name if bind is not None else None
    if dialect != "sqlite":
        # No-op for MySQL; prior migration handled it
        return

    # Check existing columns via PRAGMA
    cols = [r[1] for r in bind.execute(sa.text("PRAGMA table_info(pages_sem_inventory)")).fetchall()]

    if "product_list" not in cols:
        # Use TEXT to maintain compatibility; app treats it as JSON-encoded text on SQLite
        bind.execute(sa.text("ALTER TABLE pages_sem_inventory ADD COLUMN product_list TEXT DEFAULT '[]'"))
    if "product_positions" not in cols:
        bind.execute(sa.text("ALTER TABLE pages_sem_inventory ADD COLUMN product_positions TEXT"))

    # Create child table if missing
    tables = [r[0] for r in bind.execute(sa.text("SELECT name FROM sqlite_master WHERE type='table'"))]
    if "page_products" not in tables:
        bind.execute(sa.text(
            """
            CREATE TABLE page_products (
              id INTEGER PRIMARY KEY AUTOINCREMENT,
              page_id VARCHAR(64) NOT NULL,
              product_name VARCHAR(255) NOT NULL,
              position VARCHAR(8),
              module_type VARCHAR(64)
            )
            """
        ))
        bind.execute(sa.text("CREATE INDEX idx_page_products_page_id ON page_products(page_id)"))


def downgrade() -> None:
    # SQLite cannot drop columns easily; leave as-is
    pass



