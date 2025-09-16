from app.models.db import engine
from app.models.tables import Base
import sqlalchemy as sa

Base.metadata.create_all(bind=engine)
with engine.connect() as c:
    rows = c.execute(sa.text("SELECT name FROM sqlite_master WHERE type='table'")).fetchall()
    print("tables:", rows)
