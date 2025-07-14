import pandas as pd
from sqlalchemy import create_engine, text

# Connect to Postgres
engine = create_engine('postgresql+psycopg2://gb1009:GrcnPyM7W5K!2$@pnl-x90-2-postgres-kcho.partners.org/enigma_chr')

with engine.connect() as conn:
    # Drop & recreate tables with correct types & constraints
    conn.execute(text("""
    DROP TABLE IF EXISTS Subject_Medications, Clinical_Variables, Cognitive_Variables, Subject_Demographics CASCADE;

    CREATE TABLE Subject_Demographics (
        SubjID VARCHAR PRIMARY KEY,
        "Group" VARCHAR,
        Age INTEGER,
        Sex VARCHAR,
        Site VARCHAR
    );

    CREATE TABLE Cognitive_Variables (
        SubjID VARCHAR PRIMARY KEY REFERENCES Subject_Demographics(SubjID),
        HR_Group_Method VARCHAR,
        Raw_IQ FLOAT,
        Scaled_IQ FLOAT,
        IQ_Method VARCHAR,
        SIPSPOS INTEGER,
        SIPSNEG INTEGER,
        SIPSGEN INTEGER,
        IQ FLOAT,
        SIPS_Date DATE,
        SIPS_Version VARCHAR
    );

    CREATE TABLE Clinical_Variables (
        SubjID VARCHAR PRIMARY KEY REFERENCES Subject_Demographics(SubjID),
        Conv_stat VARCHAR,
        Follow_Up VARCHAR,
        Handedness VARCHAR,
        Scanner VARCHAR,
        Subgroup VARCHAR,
        APS BOOLEAN,
        BIPS BOOLEAN,
        GRD BOOLEAN,
        YST BOOLEAN,
        MRI_Date DATE
    );

    CREATE TABLE Subject_Medications (
        SubjID VARCHAR PRIMARY KEY REFERENCES Subject_Demographics(SubjID),
        Current_Typ_AP BOOLEAN,
        Current_Atyp_AP BOOLEAN,
        Current_Li BOOLEAN,
        Current_AntiConv BOOLEAN,
        Current_Stim BOOLEAN,
        Current_Oth_Psyc BOOLEAN,
        Current_Any_AP BOOLEAN,
        Current_AntiDep BOOLEAN
    );
    """))
    print("Tables created successfully!")

# Load CSV
df = pd.read_csv('merge_covariates.csv')

# Map numeric codes to text
df['Group'] = df['Group'].map({0: 'HC', 1: 'CHR'}).fillna('Unknown')
df['Sex'] = df['Sex'].map({1: 'M', 2: 'F'}).fillna('Unknown')
df['Handedness'] = df['Handedness'].map({1: 'Right', 2: 'Left'}).fillna('Unknown')

# Fill missing for Age
df['Age'] = df['Age'].fillna(-1)

# Map 0/1 to booleans
bool_columns = ['APS','BIPS','GRD','YST',
                'Current_Typ_AP','Current_Atyp_AP','Current_Li','Current_AntiConv',
                'Current_Stim','Current_Oth_Psyc','Current_Any_AP','Current_AntiDep']

for col in bool_columns:
    df[col] = df[col].map({0: False, 1: True}).fillna(False).astype(bool)

# Insert data into tables
df[['SubjID', 'Group', 'Age', 'Sex', 'Site']].to_sql('Subject_Demographics', engine, if_exists='append', index=False)

df[['SubjID','HR_Group_Method', 'Raw_IQ', 'Scaled_IQ', 'IQ_Method', 
    'SIPSPOS', 'SIPSNEG', 'SIPSGEN', 'IQ', 'SIPS_Date', 'SIPS_Version']
  ].to_sql('Cognitive_Variables', engine, if_exists='append', index=False)

df[['SubjID','Conv_stat', 'Follow_Up', 'Handedness', 'Scanner', 'Subgroup', 
    'APS', 'BIPS', 'GRD', 'YST', 'MRI_Date']
  ].to_sql('Clinical_Variables', engine, if_exists='append', index=False)

df[['SubjID','Current_Typ_AP', 'Current_Atyp_AP', 'Current_Li', 'Current_AntiConv',
    'Current_Stim','Current_Oth_Psyc','Current_Any_AP','Current_AntiDep']
  ].to_sql('Subject_Medications', engine, if_exists='append', index=False)

print("Data inserted successfully!")
