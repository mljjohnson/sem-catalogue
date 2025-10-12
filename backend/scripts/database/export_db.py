import csv, sqlalchemy as sa
from app.core.config import settings
engine = sa.create_engine(settings.database_url)
with engine.begin() as c:
    rows = c.execute(sa.text("SELECT * FROM pages_sem_inventory")).mappings().all()
    with open("data/latest/pages_sem_inventory.csv","w",newline="",encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=rows[0].keys() if rows else [])
        w.writeheader(); [w.writerow(dict(r)) for r in rows]
    rows = c.execute(sa.text("SELECT id,page_id,url,created_at,html_bytes,screenshot_bytes FROM page_ai_extracts")).mappings().all()
    with open("data/latest/page_ai_extracts.csv","w",newline="",encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=rows[0].keys() if rows else [])
        w.writeheader(); [w.writerow(dict(r)) for r in rows]
print("Wrote CSVs to data/latest")
