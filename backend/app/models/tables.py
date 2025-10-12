from datetime import date, datetime

from sqlalchemy import JSON, Boolean, Column, Date, DateTime, Integer, String, Text
from sqlalchemy.orm import declarative_base


Base = declarative_base()


class PageSEMInventory(Base):
    __tablename__ = "pages_sem_inventory"

    id = Column(Integer, primary_key=True, autoincrement=True)  # Unique per record/version
    page_id = Column(String(64), nullable=False, index=True)  # Groups versions of same URL
    url = Column(Text, nullable=False)
    canonical_url = Column(Text, nullable=False)
    status_code = Column(Integer, nullable=False)
    primary_category = Column(String(255))
    vertical = Column(String(255))
    template_type = Column(String(128))
    has_coupons = Column(Boolean, nullable=False, default=False)
    has_promotions = Column(Boolean, nullable=False, default=False)
    brand_list = Column(JSON, nullable=False, default=list)
    brand_positions = Column(Text)
    product_list = Column(JSON, nullable=False, default=list)
    product_positions = Column(Text)
    first_seen = Column(Date, nullable=False)
    last_seen = Column(Date, nullable=False)
    sessions = Column(Integer, nullable=True)  # Sessions from BigQuery (NULL = not in BQ)
    ga_key_events_14d = Column(Integer)
    # Airtable sync fields
    airtable_id = Column(String(255), nullable=True)  # Airtable record ID
    channel = Column(String(255), nullable=True)      # DE Channel
    team = Column(String(255), nullable=True)         # DE Team
    brand = Column(String(255), nullable=True)        # DE Brand
    page_status = Column(String(50), nullable=True)   # Page Status (Active/Inactive)
    catalogued = Column(Integer, nullable=False, default=0)  # 0=not catalogued, 1=successfully catalogued


class PageBrand(Base):
    __tablename__ = "page_brands"

    id = Column(Integer, primary_key=True, autoincrement=True)
    page_id = Column(String(64), nullable=False, index=True)
    brand_slug = Column(String(255), nullable=False)
    brand_name = Column(String(255), nullable=False)
    position = Column(String(8))
    module_type = Column(String(64))


class PageProduct(Base):
    __tablename__ = "page_products"

    id = Column(Integer, primary_key=True, autoincrement=True)
    page_id = Column(String(64), nullable=False, index=True)
    product_name = Column(String(255), nullable=False)
    position = Column(String(8))
    module_type = Column(String(64))


class PageAIExtract(Base):
    __tablename__ = "page_ai_extracts"

    id = Column(Integer, primary_key=True, autoincrement=True)
    page_id = Column(String(64), nullable=False, index=True)
    url = Column(Text, nullable=False)
    created_at = Column(String(32), nullable=False)
    html_bytes = Column(Integer, nullable=False, default=0)
    screenshot_bytes = Column(Integer, nullable=False, default=0)
    data = Column(JSON, nullable=False, default=dict)


