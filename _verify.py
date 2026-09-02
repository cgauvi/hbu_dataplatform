from dotenv import load_dotenv; load_dotenv('.env')
import sys; sys.path.insert(0, 'src')
from urban_rag.postgis import connect
from urban_rag.rag.pgvector import PgSettings
with connect(PgSettings.from_env()) as c:
    cur = c.cursor()
    cur.execute("""
      select count(*), count(use_description),
             count(*) filter (where use_code is not null and use_description is null)
        from silver.assessment_units where scrape_date = '2026-09-01'""")
    n, described, undescribed = cur.fetchone()
    print(f"silver.assessment_units @2026-09-01: {n} rows, {described} described, "
          f"{undescribed} coded-but-undescribed")
    cur.execute("""
      select use_code, use_description, count(*) c
        from silver.assessment_units where scrape_date='2026-09-01'
       group by 1,2 order by c desc limit 6""")
    print("\n  most common uses in VSMPE:")
    for code, text, cnt in cur.fetchall():
        print(f"    {code}  {cnt:>6}  {text}")
    cur.execute("""
      select use_code, use_description from silver.assessment_units
       where scrape_date='2026-09-01' and use_code in ('1010','4611') 
       group by 1,2 order by 1""")
    print("\n  the two examples asked for:")
    for code, text in cur.fetchall():
        print(f"    {code}  {text}")
