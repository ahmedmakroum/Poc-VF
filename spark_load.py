import os
import logging
import pandas as pd
from datetime import datetime
from sqlalchemy import create_engine, text
from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from pyspark.sql.types import *

# ─── SETUP ─────────────────────────────────────────────────────────────
logging.basicConfig(level=logging.INFO, format='%(message)s')

USER, PWD, HOST, PORT = 'postgres','allianztest','localhost',5432
DB, SCHEMA            = 'dw_sinistres','sh_sinistres'
CSV_FILE              = r'C:\PoCv4\fake_data\sinistres.csv'  # your flat file path

engine_url = f'postgresql+psycopg2://{USER}:{PWD}@{HOST}:{PORT}/{DB}'
engine     = create_engine(engine_url, connect_args={'options': f'-csearch_path={SCHEMA}'})

UNIQUE_COLS = {
    'dim_date':          ['date_valeur'],
    'dim_assure':        ['nom_assure','immatriculation'],
    'dim_intermediaire': ['code_intermediaire'],
    'dim_lieu_surv':     ['code_lieusini'],
    'dim_police':        ['numero_police'],
    'dim_canal':         ['libelle_canal'],
    'dim_garantie':      ['code_garantie']
}

# ─── HELPERS ───────────────────────────────────────────────────────────
def upsert_date(conn, d):
    if d is None or pd.isna(d): return None
    date_val = pd.to_datetime(d).date()
    vals = {
        'date_valeur':  date_val,
        'annee':        date_val.year,
        'trimestre':    (date_val.month - 1)//3 + 1,
        'mois':         date_val.month,
        'jour':         date_val.day,
        'jour_semaine': date_val.weekday() + 1
    }
    sql = f"""
      INSERT INTO {SCHEMA}.dim_date
        (date_valeur, annee, trimestre, mois, jour, jour_semaine)
      VALUES (:date_valeur, :annee, :trimestre, :mois, :jour, :jour_semaine)
      ON CONFLICT (date_valeur) DO NOTHING
      RETURNING date_id;
    """
    res = conn.execute(text(sql), vals).fetchone()
    if res: return res[0]
    row = conn.execute(
        text(f"SELECT date_id FROM {SCHEMA}.dim_date WHERE date_valeur = :date_valeur"),
        {'date_valeur': date_val}
    ).fetchone()
    return row[0] if row else None

def upsert_dim(conn, table, row_vals):
    clean = {k:v for k,v in row_vals.items() if v and not pd.isna(v)}
    uniqs = UNIQUE_COLS[table]
    if any(k not in clean for k in uniqs): return None
    cols  = list(clean.keys())
    binds = [f":{c}" for c in cols]
    sql = f"""
      INSERT INTO {SCHEMA}.{table} ({','.join(cols)})
      VALUES ({','.join(binds)})
      ON CONFLICT ({','.join(uniqs)}) DO NOTHING
      RETURNING id;
    """
    res = conn.execute(text(sql), clean).fetchone()
    if res: return res[0]
    where = " AND ".join(f"{c}=:{c}" for c in uniqs)
    sel   = f"SELECT id FROM {SCHEMA}.{table} WHERE {where};"
    out   = conn.execute(text(sel), {c:clean[c] for c in uniqs}).fetchone()
    return out[0] if out else None

def process_partition(rows):
    from sqlalchemy import create_engine
    engine = create_engine(engine_url, connect_args={'options': f'-csearch_path={SCHEMA}'})
    with engine.begin() as conn:
        unknown_ids = {
            'dim_date':          upsert_date(conn, pd.to_datetime('1900-01-01')),
            'dim_assure':        upsert_dim(conn, 'dim_assure', {'nom_assure':'UNKNOWN','immatriculation':'UNK'}),
            'dim_intermediaire': upsert_dim(conn, 'dim_intermediaire', {'code_intermediaire':'UNKNOWN','nom_intermediaire':'UNK'}),
            'dim_lieu_surv':     upsert_dim(conn, 'dim_lieu_surv', {'code_lieusini':'UNKNOWN','libelle_lieu':'UNK'}),
            'dim_police':        upsert_dim(conn, 'dim_police', {'numero_police':'UNKNOWN'}),
            'dim_canal':         upsert_dim(conn, 'dim_canal', {'libelle_canal':'UNKNOWN'}),
            'dim_garantie':      upsert_dim(conn, 'dim_garantie', {'code_garantie':'UNKNOWN'})
        }
        for row in rows:
            r = row.asDict()
            ds = upsert_date(conn, r.get('DATESURV')) or unknown_ids['dim_date']
            do = upsert_date(conn, r.get('DATE_OUVERTURE')) or unknown_ids['dim_date']
            dc = upsert_date(conn, r.get('DATE_CLOTURE')) or unknown_ids['dim_date']
            a  = upsert_dim(conn, 'dim_assure', {
                  'nom_assure': r.get('ASSURE'),
                  'immatriculation': r.get('IMMATRICULATION_ASSURE')
                 }) or unknown_ids['dim_assure']
            i  = upsert_dim(conn, 'dim_intermediaire', {
                  'code_intermediaire': r.get('CODEINTE'),
                  'nom_intermediaire': r.get('INTERMEDIAIRE')
                 }) or unknown_ids['dim_intermediaire']
            l  = upsert_dim(conn, 'dim_lieu_surv', {
                  'code_lieusini': r.get('LIEUSINI'),
                  'libelle_lieu': r.get('LIEUSINI')
                 }) or unknown_ids['dim_lieu_surv']
            p  = upsert_dim(conn, 'dim_police', {'numero_police': r.get('NUMPOLICE')}) or unknown_ids['dim_police']
            c  = upsert_dim(conn, 'dim_canal', {'libelle_canal': r.get('CODNATSI')}) or unknown_ids['dim_canal']
            g  = upsert_dim(conn, 'dim_garantie', {'code_garantie': r.get('CODEGARA')}) or unknown_ids['dim_garantie']

            conn.execute(text(f"""
              INSERT INTO {SCHEMA}.fait_sinistre (
                fk_date_survenance,fk_date_ouverture,fk_date_cloture,
                fk_assure,fk_intermediaire,fk_lieu_survenance,
                fk_police,fk_canal,fk_garantie,
                montant_reserve,montant_reglement,
                motif_cloture,user_cloture
              ) VALUES (
                :ds,:do,:dc,:a,:i,:l,:p,:c,:g,:mr,:mreg,:motif,:uc
              ) ON CONFLICT DO NOTHING;
            """), {
                'ds': ds, 'do': do, 'dc': dc,
                'a':  a,  'i':  i,  'l':  l,
                'p':  p,  'c':  c,  'g':  g,
                'mr': r.get('MNT_RESERVE'),
                'mreg': r.get('MNT_REGLEMENT'),
                'motif': r.get('MOTIF'),
                'uc': r.get('USER_CLOTURE')
            })

# ─── SPARK JOB ────────────────────────────────────────────────────────
spark = SparkSession.builder \
    .appName("ETL Fait Sinistres") \
    .config("spark.jars", "C:/PoCv4/jars/postgresql-42.7.3.jar") \
    .getOrCreate()

df = spark.read.option("header", True).csv(CSV_FILE)
df.foreachPartition(process_partition)
logging.info("✅ Spark ETL terminé.")

