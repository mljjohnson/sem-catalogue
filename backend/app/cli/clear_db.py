import click
import sqlalchemy as sa
from app.core.config import settings


@click.command(help="Clear ACE-SEM tables: pages_sem_inventory, page_ai_extracts, page_brands, page_products")
@click.option("--yes", is_flag=True, help="Skip confirmation prompt")
def main(yes: bool):
    if not yes:
        click.confirm(
            "This will delete all rows from pages_sem_inventory, page_ai_extracts, page_brands, and page_products. Continue?",
            abort=True,
        )

    engine = sa.create_engine(settings.database_url)
    with engine.begin() as conn:
        # Child tables first
        try:
            conn.execute(sa.text("DELETE FROM page_products"))
        except Exception:
            pass
        try:
            conn.execute(sa.text("DELETE FROM page_brands"))
        except Exception:
            pass
        conn.execute(sa.text("DELETE FROM page_ai_extracts"))
        conn.execute(sa.text("DELETE FROM pages_sem_inventory"))
    click.echo("Tables cleared.")


if __name__ == "__main__":
    main()


