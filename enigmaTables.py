import pandas as pd
from sqlalchemy import create_engine, text
import os
import glob

# --- Database credentials ---
db_host = 'pnl-x90-2-postgres-kcho.partners.org'
db_name = 'enigma_chr'
db_port = 5432
db_user = 'gb1009'
db_password = '*'
db_schema = 'enigma_data'
dwi_schema = 'enigma_data_dwi'

# --- Connect to Postgres ---
engine = create_engine(
    f'postgresql+psycopg2://{db_user}:{db_password}@{db_host}:{db_port}/{db_name}'
)

with engine.connect() as conn:
    # Drop and recreate core tables
    sql_statement = f"""
    DROP TABLE IF EXISTS
        {db_schema}.subject_medications,
        {db_schema}.clinical_variables,
        {db_schema}.cognitive_variables,
        {db_schema}.subject_demographics CASCADE;

    CREATE TABLE {db_schema}.subject_demographics (
        SubjID VARCHAR PRIMARY KEY,
        "Group" VARCHAR,
        Age INTEGER,
        Sex VARCHAR,
        "Site" VARCHAR
    );

    CREATE TABLE {db_schema}.cognitive_variables (
        SubjID VARCHAR PRIMARY KEY REFERENCES {db_schema}.subject_demographics(SubjID),
        HR_Group_Method VARCHAR,
        Raw_IQ FLOAT,
        Scaled_IQ FLOAT,
        IQ_Method VARCHAR,
        SIPS_POS INTEGER,
        SIPS_NEG INTEGER,
        SIPS_GEN INTEGER,
        IQ FLOAT,
        SIPS_Date DATE,
        SIPS_Version VARCHAR
    );

    CREATE TABLE {db_schema}.clinical_variables (
        SubjID VARCHAR PRIMARY KEY REFERENCES {db_schema}.subject_demographics(SubjID),
        Conv_stat VARCHAR,
        Follow_Up VARCHAR,
        Handedness VARCHAR,
        Scanner VARCHAR,
        Subgroup VARCHAR,
        APS BOOLEAN,
        BIPS BOOLEAN,
        GRD BOOLEAN,
        YST VARCHAR,
        MRI_Date DATE
    );

    CREATE TABLE {db_schema}.subject_medications (
        SubjID VARCHAR PRIMARY KEY REFERENCES {db_schema}.subject_demographics(SubjID),
        Current_Typ_AP BOOLEAN,
        Current_Atyp_AP BOOLEAN,
        Current_Li BOOLEAN,
        Current_AntiConv BOOLEAN,
        Current_Stim BOOLEAN,
        Current_Oth_Psy BOOLEAN,
        Current_Any_AP BOOLEAN,
        Current_AntiDep BOOLEAN
    );
    """
    conn.execute(text(sql_statement))
    conn.execute(text(f"CREATE SCHEMA IF NOT EXISTS {dwi_schema}"))
    conn.commit()
    print("Tables and schemas created successfully!")

# --- Load CSV with core demographics ---
df = pd.read_csv('/data/pnl/home/gb1009/enigma/merge_covariates.csv')

# --- Preprocessing ---
df['Group'] = df['Group'].map({0: 'HC', 1: 'CHR'}).fillna('Unknown')
df['Sex'] = df['Sex'].map({1: 'M', 2: 'F'}).fillna('Unknown')
df['Handedness'] = df['Handedness'].map({1: 'Right', 2: 'Left'}).fillna('Unknown')
df['Age'] = df['Age'].fillna(-1)

bool_columns = [
    'APS', 'BIPS', 'GRD', 'YST',
    'Current_Typ_AP', 'Current_Atyp_AP', 'Current_Li',
    'Current_AntiConv', 'Current_Stim', 'Current_Oth_Psy',
    'Current_Any_AP', 'Current_AntiDep'
]
for col in bool_columns:
    df[col] = df[col].map({0: False, 1: True}).fillna(False).astype(bool)

# --- Insert into core tables ---
df[['SubjID', 'Group', 'Age', 'Sex', 'Site']].to_sql('subject_demographics', engine, schema=db_schema, if_exists='append', index=False)

df[['SubjID', 'HR_Group_Method', 'Raw_IQ', 'Scaled_IQ', 'IQ_Method', 'SIPS_POS', 'SIPS_NEG',
    'SIPS_GEN', 'IQ', 'SIPS_Date', 'SIPS_Version']].to_sql('cognitive_variables', engine, schema=db_schema, if_exists='append', index=False)

df[['SubjID', 'Conv_stat', 'Follow_Up', 'Handedness', 'Scanner', 'Subgroup',
    'APS', 'BIPS', 'GRD', 'YST', 'MRI_Date']].to_sql('clinical_variables', engine, schema=db_schema, if_exists='append', index=False)

df[['SubjID', 'Current_Typ_AP', 'Current_Atyp_AP', 'Current_Li',
    'Current_AntiConv', 'Current_Stim', 'Current_Oth_Psy',
    'Current_Any_AP', 'Current_AntiDep']].to_sql('subject_medications', engine, schema=db_schema, if_exists='append', index=False)

print("Core data inserted successfully.")

# --- Process DWI Tables ---
dwi_root = '/data/pnl/home/gb1009/PNLProject/enigma'

for site in os.listdir(dwi_root):
    stats_path = os.path.join(dwi_root, site, 'stats')
    if not os.path.isdir(stats_path):
        continue

    dwi_files = glob.glob(os.path.join(stats_path, '*_combined_roi_avg.csv'))

    for dwi_file in dwi_files:
        try:
            df_dwi = pd.read_csv(dwi_file)
        except Exception as e:
            print(f"Error reading {dwi_file}: {e}")
            continue

        if 'Cases' not in df_dwi.columns:
            print(f"Skipping {dwi_file} (no 'Cases' column)")
            continue

        df_dwi.rename(columns={'Cases': 'SubjID'}, inplace=True)
        df_dwi.columns = df_dwi.columns.str.strip().str.replace(' ', '_').str.replace('-', '_')

        table_base = os.path.splitext(os.path.basename(dwi_file))[0].lower()
        table_name = f"{table_base}_{site.lower()}"

        # Create the DWI table
        with engine.connect() as conn:
            col_defs = ",\n    ".join(
                [f"{col} FLOAT" for col in df_dwi.columns if col != 'SubjID']
            )
            create_stmt = f"""
            CREATE TABLE IF NOT EXISTS {dwi_schema}.{table_name} (
                SubjID VARCHAR REFERENCES {db_schema}.subject_demographics(SubjID),
                {col_defs}
            );
            """
            conn.execute(text(create_stmt))

        # Insert the data
        df_dwi.to_sql(table_name, engine, schema=dwi_schema, if_exists='append', index=False)
        print(f"Inserted: {dwi_schema}.{table_name}")

print("DWI data inserted successfully.")

