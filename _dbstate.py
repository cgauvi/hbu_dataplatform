import os, time
from dotenv import load_dotenv
load_dotenv('.env')
import sys; sys.path.insert(0, 'src')
from urban_rag.postgis import connect
from urban_rag.rag.pgvector import PgSettings
t = time.time()
with connect(PgSettings.from_env()) as c:
    print("connected in %.2fs" % (time.time()-t))
    cur = c.cursor()
    cur.execute("""
      select table_schema || '.' || table_name, column_name
        from information_schema.columns
       where column_name in ('use_description','dominant_use_description',
                             'existing_dominant_use_description')
       order by 1, 2""")
    rows = cur.fetchall()
    print("description columns:")
    if rows:
        for r in rows: print("   ", r[0], "->", r[1])
    else:
        print("    NONE - the five ALTERs have not been applied")
    print()
    for t_ in ("silver.assessment_units","silver.lot_assessment_comparables",
               "gold.lot_profiles","gold.lot_redevelopment_gap",
               "gold.lot_investment_opportunities","silver.lot_buildable_setbacks",
               "silver.lot_development_programs","gold.lot_highest_best_use",
               "gold.lot_building_massing"):
        try:
            cur.execute("select scrape_date::text, count(*) from %s group by 1 order by 1" % t_)
            print(" ", t_, dict(cur.fetchall()))
        except Exception as e:
            c.rollback(); print(" ", t_, "ERR", str(e).splitlines()[0])
