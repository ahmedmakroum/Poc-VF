import os
import pandas as pd
import logging
from sqlalchemy import create_engine, text

# ─── CONFIGURATION & LOGGING ─────────────────────────────────────────────────
logging.basicConfig(level=logging.INFO, format='%(message)s')

USER, PWD, HOST, PORT = 'postgres','allianztest','localhost',5432
DB, SCHEMA      = 'dw_sinistres','sh_sinistres'
CSV_DIR         = r'C:\PoCv4\fake_data'

engine = create_engine(
    f'postgresql+psycopg2://{USER}:{PWD}@{HOST}:{PORT}/{DB}',
    connect_args={'options': f'-csearch_path={SCHEMA}'}
)

# ─── MÉTA : clés uniques par dimension ────────────────────────────────────────
UNIQUE_COLS = {
    'dim_date':          ['date_valeur'],
    'dim_assure':        ['nom_assure','immatriculation'],
    'dim_intermediaire':['code_intermediaire'],
    'dim_lieu_surv':     ['code_lieusini'],
    'dim_police':        ['numero_police'],
    'dim_canal':         ['libelle_canal'],
    'dim_garantie':      ['code_garantie']
}

# ─── HELPERS ──────────────────────────────────────────────────────────────────
def load_and_clean_csv(path):
    """Charge tous les CSV du dossier, uniformise colonnes et convertit les dates."""
    dfs = []
    for fname in os.listdir(path):
        if not fname.lower().endswith('.csv'):
            continue
        df = pd.read_csv(os.path.join(path, fname), dtype=str)
        dfs.append(df)
    df = pd.concat(dfs, ignore_index=True)
    df.columns = df.columns.str.strip().str.upper().str.replace(' ', '_')
    for col in ('DATESURV','DATE_OUVERTURE','DATE_CLOTURE','DATE_REOUVERTURE'):
        if col in df:
            df[col] = pd.to_datetime(df[col], errors='coerce').dt.date
    return df

def upsert_date(conn, d):
    """Upsert dans dim_date, renvoie date_id ou None."""
    if d is None or pd.isna(d):
        return None
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
    if res:
        return res[0]
    row = conn.execute(
        text(f"SELECT date_id FROM {SCHEMA}.dim_date WHERE date_valeur = :date_valeur"),
        {'date_valeur': date_val}
    ).fetchone()
    return row[0] if row else None

def upsert_dim(conn, table, row_vals):
    """Upsert générique pour les dimensions non-date."""
    # retire valeurs vides ou NaN
    clean = {k:v for k,v in row_vals.items() if v and not pd.isna(v)}
    uniqs = UNIQUE_COLS[table]
    # si une des colonnes uniques manque, on renvoie None
    if any(k not in clean for k in uniqs):
        return None
    cols  = list(clean.keys())
    binds = [f":{c}" for c in cols]
    onconf = f"ON CONFLICT ({','.join(uniqs)}) DO NOTHING"
    sql = f"""
      INSERT INTO {SCHEMA}.{table} ({','.join(cols)})
      VALUES ({','.join(binds)})
      {onconf}
      RETURNING id;
    """
    res = conn.execute(text(sql), clean).fetchone()
    if res:
        return res[0]
    # fallback SELECT
    where = " AND ".join(f"{c}=:{c}" for c in uniqs)
    sel   = f"SELECT id FROM {SCHEMA}.{table} WHERE {where};"
    out   = conn.execute(text(sel), {c:clean[c] for c in uniqs}).fetchone()
    return out[0] if out else None

# ─── MAIN ETL ─────────────────────────────────────────────────────────────────
def main():
    df = load_and_clean_csv(CSV_DIR)
    logging.info(f"→ {len(df)} lignes chargées depuis les CSV.")

    # préparer IDs "UNKNOWN" pour remplacer les None
    unknown_ids = {}
    with engine.begin() as conn:
        unknown_ids['dim_date']          = upsert_date(conn, pd.to_datetime('1900-01-01').date())
        unknown_ids['dim_assure']        = upsert_dim(conn, 'dim_assure',
                                                 {'nom_assure':'UNKNOWN','immatriculation':'UNK'})
        unknown_ids['dim_intermediaire'] = upsert_dim(conn, 'dim_intermediaire',
                                                 {'code_intermediaire':'UNKNOWN','nom_intermediaire':'UNK'})
        unknown_ids['dim_lieu_surv']     = upsert_dim(conn, 'dim_lieu_surv',
                                                 {'code_lieusini':'UNKNOWN','libelle_lieu':'UNK'})
        unknown_ids['dim_police']        = upsert_dim(conn, 'dim_police',{'numero_police':'UNKNOWN'})
        unknown_ids['dim_canal']         = upsert_dim(conn, 'dim_canal',{'libelle_canal':'UNKNOWN'})
        unknown_ids['dim_garantie']      = upsert_dim(conn, 'dim_garantie',{'code_garantie':'UNKNOWN'})
    logging.info(f"→ IDs UNKNOWN initiaux : {unknown_ids}")

    inserted = 0
    with engine.begin() as conn:
        for _, row in df.iterrows():
            # upsert dimensions (ou UNKNOWN)
            ds = upsert_date(conn, row.get('DATESURV'))         or unknown_ids['dim_date']
            do = upsert_date(conn, row.get('DATE_OUVERTURE'))   or unknown_ids['dim_date']
            dc = upsert_date(conn, row.get('DATE_CLOTURE'))     or unknown_ids['dim_date']
            a  = upsert_dim(conn, 'dim_assure', {
                   'nom_assure':row.get('ASSURE'),
                   'immatriculation':row.get('IMMATRICULATION_ASSURE')
                 }) or unknown_ids['dim_assure']
            i  = upsert_dim(conn, 'dim_intermediaire', {
                   'code_intermediaire':row.get('CODEINTE'),
                   'nom_intermediaire':row.get('INTERMEDIAIRE')
                 }) or unknown_ids['dim_intermediaire']
            l  = upsert_dim(conn, 'dim_lieu_surv', {
                   'code_lieusini':row.get('LIEUSINI'),
                   'libelle_lieu':row.get('LIEUSINI')
                 }) or unknown_ids['dim_lieu_surv']
            p  = upsert_dim(conn, 'dim_police',    {'numero_police':row.get('NUMPOLICE')}) \
                 or unknown_ids['dim_police']
            c  = upsert_dim(conn, 'dim_canal',     {'libelle_canal':row.get('CODNATSI')}) \
                 or unknown_ids['dim_canal']
            g  = upsert_dim(conn, 'dim_garantie',  {'code_garantie':row.get('CODEGARA')}) \
                 or unknown_ids['dim_garantie']

            # insert dans la table de faits
            conn.execute(text(f"""
              INSERT INTO {SCHEMA}.fait_sinistre (
                fk_date_survenance,fk_date_ouverture,fk_date_cloture,
                fk_assure,fk_intermediaire,fk_lieu_survenance,
                fk_police,fk_canal,fk_garantie,
                montant_reserve,montant_reglement,
                motif_cloture,user_cloture
              ) VALUES (
                :ds,:do,:dc,:a,:i,:l,:p,:c,:g,
                :mr,:mreg,:motif,:uc
              ) ON CONFLICT DO NOTHING;
            """), {
                'ds': ds, 'do': do, 'dc': dc,
                'a':  a,  'i':  i,  'l':  l,
                'p':  p,  'c':  c,  'g':  g,
                'mr':   row.get('MNT_RESERVE'),
                'mreg': row.get('MNT_REGLEMENT'),
                'motif':row.get('MOTIF'),
                'uc':   row.get('USER_CLOTURE')
            })
            inserted += 1

    logging.info(f"✅ Faits insérés (tous) : {inserted}")

if __name__ == '__main__':
    main()
