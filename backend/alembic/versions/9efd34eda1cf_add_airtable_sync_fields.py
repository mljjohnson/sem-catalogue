"""Add Airtable sync fields

Revision ID: 9efd34eda1cf
Revises: 0005_force_add_products_sqlite
Create Date: 2025-09-18 16:31:57.873207

"""
from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = '9efd34eda1cf'
down_revision = '0005_force_add_products_sqlite'
branch_labels = None
depends_on = None


def upgrade() -> None:
    # Add Airtable sync fields to pages_sem_inventory
    op.add_column('pages_sem_inventory', sa.Column('airtable_id', sa.String(length=255), nullable=True))
    op.add_column('pages_sem_inventory', sa.Column('channel', sa.String(length=255), nullable=True))
    op.add_column('pages_sem_inventory', sa.Column('team', sa.String(length=255), nullable=True))
    op.add_column('pages_sem_inventory', sa.Column('brand', sa.String(length=255), nullable=True))


def downgrade() -> None:
    op.drop_column('pages_sem_inventory', 'brand')
    op.drop_column('pages_sem_inventory', 'team')
    op.drop_column('pages_sem_inventory', 'channel')
    op.drop_column('pages_sem_inventory', 'airtable_id')
