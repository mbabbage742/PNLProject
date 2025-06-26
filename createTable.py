import os
import json
import psycopg2
from bs4 import BeautifulSoup

# DB connection parameters
db_user = "user id here"
db_password = "password here"
db_host = "dna007.partners.org"
db_port = 58999
db_name = "mri_db"
db_schema = "mri_qc"
table_name = "mriqc_results"

# SQL to create schema and table
CREATE_TABLE_SQL = f"""
CREATE SCHEMA IF NOT EXISTS {db_schema};

CREATE TABLE IF NOT EXISTS {db_schema}.{table_name} (
    image_type TEXT,
    id SERIAL PRIMARY KEY,
    subject_id TEXT,
    session_number TEXT,
    scan_date DATE,
    filename TEXT,
    cjv DOUBLE PRECISION,
    cnr DOUBLE PRECISION,
    efc DOUBLE PRECISION,
    fber DOUBLE PRECISION,
    fwhm_avg DOUBLE PRECISION,
    fwhm_x DOUBLE PRECISION,
    fwhm_y DOUBLE PRECISION,
    fwhm_z DOUBLE PRECISION,
    icvs_csf DOUBLE PRECISION,
    icvs_gm DOUBLE PRECISION,
    icvs_wm DOUBLE PRECISION,
    inu_med DOUBLE PRECISION,
    inu_range DOUBLE PRECISION,
    provenance_md5sum TEXT,
    qi_1 DOUBLE PRECISION,
    qi_2 DOUBLE PRECISION,
    rpve_csf DOUBLE PRECISION,
    rpve_gm DOUBLE PRECISION,
    rpve_wm DOUBLE PRECISION,
    size_x INTEGER,
    size_y INTEGER,
    size_z INTEGER,
    snr_csf DOUBLE PRECISION,
    snr_gm DOUBLE PRECISION,
    snr_total DOUBLE PRECISION,
    snr_wm DOUBLE PRECISION,
    snrd_csf DOUBLE PRECISION,
    snrd_gm DOUBLE PRECISION,
    snrd_total DOUBLE PRECISION,
    snrd_wm DOUBLE PRECISION,
    spacing_x DOUBLE PRECISION,
    spacing_y DOUBLE PRECISION,
    spacing_z DOUBLE PRECISION,
    summary_bg_k DOUBLE PRECISION,
    summary_bg_mad DOUBLE PRECISION,
    summary_bg_mean DOUBLE PRECISION,
    summary_bg_median DOUBLE PRECISION,
    summary_bg_n BIGINT,
    summary_bg_p05 DOUBLE PRECISION,
    summary_bg_p95 DOUBLE PRECISION,
    summary_bg_stdv DOUBLE PRECISION,
    summary_csf_k DOUBLE PRECISION,
    summary_csf_mad DOUBLE PRECISION,
    summary_csf_mean DOUBLE PRECISION,
    summary_csf_median DOUBLE PRECISION,
    summary_csf_n BIGINT,
    summary_csf_p05 DOUBLE PRECISION,
    summary_csf_p95 DOUBLE PRECISION,
    summary_csf_stdv DOUBLE PRECISION,
    summary_gm_k DOUBLE PRECISION,
    summary_gm_mad DOUBLE PRECISION,
    summary_gm_mean DOUBLE PRECISION,
    summary_gm_median DOUBLE PRECISION,
    summary_gm_n BIGINT,
    summary_gm_p05 DOUBLE PRECISION,
    summary_gm_p95 DOUBLE PRECISION,
    summary_gm_stdv DOUBLE PRECISION,
    summary_wm_k DOUBLE PRECISION,
    summary_wm_mad DOUBLE PRECISION,
    summary_wm_mean DOUBLE PRECISION,
    summary_wm_median DOUBLE PRECISION,
    summary_wm_n BIGINT,
    summary_wm_p05 DOUBLE PRECISION,
    summary_wm_p95 DOUBLE PRECISION,
    summary_wm_stdv DOUBLE PRECISION,
    tpm_overlap_csf DOUBLE PRECISION,
    tpm_overlap_gm DOUBLE PRECISION,
    tpm_overlap_wm DOUBLE PRECISION,
    wm2max DOUBLE PRECISION
);
"""

# Creates table in DB
def create_table(conn):
    with conn.cursor() as cur:
        cur.execute(CREATE_TABLE_SQL)
    conn.commit()

# Loads JSON file
def load_json(file_path):
    with open(file_path, "r") as f:
        return json.load(f)

# Detects image type from file
def detect_image_type(filename):
    fname = filename.lower()
    if "t1w" in fname:
        return "T1"
    elif "t2w" in fname:
        return "T2"
    elif "bold" in fname:
        return "fMRI"
    elif "dwi" in fname or "dti" in fname:
        return "dMRI"
    else:
        return "Unknown"

# Extracts metadata from matching HTML file 
def extract_metadata_from_html(html_file):
    with open(html_file, "r") as f:
        soup = BeautifulSoup(f, "html.parser")
    text = soup.get_text()

    lines = text.splitlines()
    summary_lines = [line.strip() for line in lines if "BIDS filename:" in line or "Date and time:" in line]

    filename_line = next((line for line in summary_lines if "BIDS filename:" in line), "")
    date_line = next((line for line in summary_lines if "Date and time:" in line), "")

    filename = filename_line.replace("BIDS filename:", "").strip().rstrip(".")
    scan_date = date_line.replace("Date and time:", "").strip().split(",")[0]

    subject_id = session_number = None
    parts = filename.split("_")
    for p in parts:
        if p.startswith("sub-"):
            subject_id = p.replace("sub-", "")
        elif p.startswith("ses-"):
            session_number = p.replace("ses-", "")

    image_type = detect_image_type(filename)

    return {
        "subject_id": subject_id,
        "session_number": session_number,
        "scan_date": scan_date,
        "filename": filename,
        "image_type": image_type
    }

# Inserts all data into DB table 
def insert_data(conn, data, metadata):
    provenance_md5 = data.get("provenance", {}).get("md5sum")

    keys = ["subject_id", "session_number", "scan_date", "filename", "image_type"]
    values = [metadata["subject_id"], metadata["session_number"], metadata["scan_date"], metadata["filename"], metadata["image_type"]]

    for k, v in data.items():
        if k in ["provenance", "bids_meta"]:
            continue
        keys.append(k)
        values.append(v)

    keys.append("provenance_md5sum")
    values.append(provenance_md5)

    columns = ", ".join(keys)
    placeholders = ", ".join(["%s"] * len(values))

    sql = f"""
        INSERT INTO {db_schema}.{table_name} ({columns})
        VALUES ({placeholders})
        ON CONFLICT DO NOTHING
    """
    with conn.cursor() as cur:
        cur.execute(sql, values)
    conn.commit()

# Processes individual JSON file
def process_file(json_file, conn):
    base_name = os.path.splitext(json_file)[0]
    html_file = base_name + ".html"

    if not os.path.exists(html_file):
        print(f"Warning: No HTML file found for {json_file}")
        return

    data = load_json(json_file)
    metadata = extract_metadata_from_html(html_file)
    insert_data(conn, data, metadata)

def main(json_dir):
    conn = psycopg2.connect(
        dbname=db_name,
        user=db_user,
        password=db_password,
        host=db_host,
        port=db_port
    )
    try:
        print("Creating table...")
        create_table(conn)

        for file in os.listdir(json_dir):
            if file.endswith(".json"):
                print(f"Processing {file}")
                process_file(os.path.join(json_dir, file), conn)

        print("All files processed.")
    except Exception as e:
        print("Error:", e)
    finally:
        conn.close()

if __name__ == "__main__":
    json_dir = "file directory here"  
    main(json_dir)

