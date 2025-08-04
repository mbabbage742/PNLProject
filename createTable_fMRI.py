import os
from pathlib import Path
import json
import psycopg2
import psycopg2.errors
from bs4 import BeautifulSoup
from datetime import datetime
import logging

logging.basicConfig(level=logging.INFO)

# DB connection parameters
db_user = "pipeline"
db_password = "*"
db_host = "pnl-postgres-1.partners.org"
db_port = 5432
db_name = "ampscz_db"
db_schema = "mri_qc"
table_name = "mri_qc_results_fmri"

# SQL to create schema
CREATE_SCHEMA_SQL = f"""
CREATE SCHEMA IF NOT EXISTS {db_schema};
"""

CREATE_TABLE_SQL = f"""
CREATE TABLE IF NOT EXISTS {db_schema}.{table_name} (
    subject_id TEXT,
    session_number TEXT,
    scan_date DATE,
    filename TEXT,
    image_type TEXT,
    aor DOUBLE PRECISION,
    aqi DOUBLE PRECISION,
    dummy_trs INTEGER,
    dvars_nstd DOUBLE PRECISION,
    dvars_std DOUBLE PRECISION, 
    dvars_vstd DOUBLE PRECISION, 
    efc DOUBLE PRECISION,
    fber DOUBLE PRECISION,
    fd_mean DOUBLE PRECISION, 
    fd_num DOUBLE PRECISION, 
    fd_perc DOUBLE PRECISION,
    fwhm_avg DOUBLE PRECISION,
    fwhm_x DOUBLE PRECISION,
    fwhm_y DOUBLE PRECISION,
    fwhm_z DOUBLE PRECISION,
    gcor DOUBLE PRECISION,
    gsr_x DOUBLE PRECISION,
    gsr_y DOUBLE PRECISION,
    provenance_md5sum TEXT,
    size_t INTEGER,
    size_x INTEGER,
    size_y INTEGER,
    size_z INTEGER,
    snr DOUBLE PRECISION,
    spacing_tr DOUBLE PRECISION, 
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
    summary_fg_k DOUBLE PRECISION, 
    summary_fg_mad DOUBLE PRECISION, 
    summary_fg_mean DOUBLE PRECISION,
    summary_fg_median DOUBLE PRECISION,
    summary_fg_n BIGINT, 
    summary_fg_p05 DOUBLE PRECISION,
    summary_fg_p95 DOUBLE PRECISION,
    summary_fg_stdv DOUBLE PRECISION,
    tsnr DOUBLE PRECISION
);
""" 

# SQL to add constraint
ADD_CONSTRAINT_SQL = f"""
ALTER TABLE {db_schema}.{table_name}
ADD CONSTRAINT unique_subject_session_filename
UNIQUE (subject_id, session_number, filename);
"""


# Creates table in DB 
def create_table(conn):
    with conn.cursor() as cur:
        cur.execute(CREATE_SCHEMA_SQL)
        cur.execute(CREATE_TABLE_SQL)
    conn.commit()

    with conn.cursor() as cur:
        try:
            cur.execute(ADD_CONSTRAINT_SQL)
        except psycopg2.errors.DuplicateObject:
            # Constraint already exists; ignore
            conn.rollback()
    

# Loads JSON file
def load_json(file_path):
    with open(file_path, "r") as f:
        return json.load(f)

# Detects image type
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

# Extracts metadata from corresponding HTML file 
def extract_metadata_from_html(html_file, data=None):
    with open(html_file, "r") as f:
        soup = BeautifulSoup(f, "html.parser")
    text = soup.get_text()
    lines = text.splitlines()
    summary_lines = [line.strip() for line in lines if "BIDS filename:" in line or "Date and time:" in line]

    filename_line = next((line for line in summary_lines if "BIDS filename:" in line), "")
    date_line = next((line for line in summary_lines if "Date and time:" in line), "")

    filename = filename_line.replace("BIDS filename:", "").strip().rstrip(".") if filename_line else None
    scan_date_str = date_line.replace("Date and time:", "").strip().split(",")[0] if date_line else None

    try:
        scan_date = datetime.strptime(scan_date_str, "%Y-%m-%d").date() if scan_date_str else None
        scan_date = scan_date.strftime("%Y-%m-%d")
    except Exception:
        scan_date = None
        logging.warning(f"Could not parse scan_date from {scan_date_str}")

    # Extract subject and session IDs
    subject_id = session_number = None
    if filename:
        parts = filename.split("_")
        for p in parts:
            if p.startswith("sub-"):
                subject_id = p.replace("sub-", "")
            elif p.startswith("ses-"):
                session_number = p.replace("ses-", "")

    # Fallback if missing
    if data:
        subject_id = subject_id or data.get("bids_meta", {}).get("subject_id")
        session_number = session_number or data.get("bids_meta", {}).get("session_id")

    image_type = detect_image_type(filename or "")

    return {
        "subject_id": subject_id,
        "session_number": session_number,
        "scan_date": scan_date,
        "filename": filename,
        "image_type": image_type
    }

# Inserts data into table in DB
def insert_data(conn, data, metadata):
    provenance_md5 = data.get("provenance", {}).get("md5sum")
    keys = ["subject_id", "session_number", "scan_date", "filename", "image_type"]
    values = [
        metadata["subject_id"], metadata["session_number"], metadata["scan_date"],
        metadata["filename"], metadata["image_type"]
    ]

    values = [f"'{x}'" for x in values]
    for k, v in data.items():
        if k in ["provenance", "bids_meta"]:
            continue
        keys.append(k)

        values.append(v)
    keys.append("provenance_md5sum")
    values.append(f"'{provenance_md5}'")

    columns = ", ".join(keys)

    placeholders = ", ".join([str(x) for x in values])

    logging.info(f"values: {values}")
    logging.info(f"placeholders: {placeholders}")

    sql = f"""
        INSERT INTO {db_schema}.{table_name} ({columns})
        VALUES ({placeholders})
        ON CONFLICT DO NOTHING
    """

    logging.info(f"sql query: {sql}")
    try:
        with conn.cursor() as cur:
            cur.execute(sql, values)
        conn.commit()
    except Exception as e:
        logging.error(f"Error inserting data for {metadata.get('filename', 'unknown')}: {e}")
        conn.rollback()


# Processes and extracts data from JSON file 
def process_file(json_file, conn):
    json_path = Path(json_file)
    json_dir = json_path.parent

    parent_dir = json_dir.parent.parent.parent

    base_name = os.path.splitext(os.path.basename(json_file))[0]  
    html_file = parent_dir / (base_name + ".html")

    logging.info(f"HTML file location: {html_file}")
    logging.info(f"HTML file exists: {Path(html_file).is_file()}")

    if not os.path.exists(html_file):
        logging.warning(f"No HTML file found for {json_file} (looked for {html_file})")
        return

    data = load_json(json_file)
    metadata = extract_metadata_from_html(html_file, data)

    if not metadata.get("subject_id") or not metadata.get("filename"):
        logging.warning(f"Skipping {json_file}: missing subject_id or filename")
        return

    insert_data(conn, data, metadata)

def main(json_dir):
    with psycopg2.connect(
        dbname=db_name,
        user=db_user,
        password=db_password,
        host=db_host,
        port=db_port
    ) as conn:
        logging.info("Creating table...")
        try:
            create_table(conn)
        except Exception as e:
            logging.info(f"Not re-creating the table... ({e})")

        for root, dirs, files in os.walk(json_dir):
            for file in files:
                # Process only JSON files with "bold" (fMRI) in the filename
                if not file.endswith(".json") or "bold" not in file.lower():
                    continue

                json_path = os.path.join(root, file)
                logging.info(f"Processing {json_path}")

                try:
                    process_file(json_path, conn)
                except Exception as e:
                    logging.error(f"Error processing {json_path}: {e}")
                    conn.rollback()

        logging.info("All fMRI files processed.")


if __name__ == "__main__":
    json_dir = "/data/pnl/home/gb1009/testdata/MRIQC"
    main(json_dir)

