"""
Code for submitting updated CSV to SCC
Date: 2025-06-25
Version: 0.7.0
Author: INOE LB

"""


import pdfplumber
import pandas as pd
import os
import re
from datetime import datetime
import glob
from io import StringIO  # Fixed import for in-memory CSV reading

# === AUTODETECT FILES ===
pdf_matches = glob.glob("*.pdf")
csv_matches = glob.glob("LidarConfiguration*.csv")

if not pdf_matches:
    raise FileNotFoundError("No PDF file found in current directory.")
if not csv_matches:
    raise FileNotFoundError("No CSV file matching 'LidarConfiguration*.csv' found in current directory.")
if len(csv_matches) > 1:
    raise FileNotFoundError(f"Expected one CSV file, found multiple: {csv_matches}")

PDF_FILE = pdf_matches[0]
CSV_FILE = csv_matches[0]
OUTPUT_FILE = "NEW_" + CSV_FILE
LOG_FILE = "CSV_match.log"

# === HELPERS ===
def normalize_wavelength(w):
    try:
        return int(round(float(w)))
    except:
        return None

def parse_atlas_id(atlas_id):
    parts = {
        "wavelength": atlas_id[:4],
        "range": atlas_id[4],
        "scattering": atlas_id[5],
        "mode": atlas_id[6],
        "suffix": atlas_id[7] if len(atlas_id) > 7 else ""
    }
    return parts

# === PARSE PDF ===
with pdfplumber.open(PDF_FILE) as pdf:
    depol_date = None
    all_tables = []
    for i, page in enumerate(pdf.pages):
        text = page.extract_text()
        if i == 0:
            match = re.search(r"Depolarization calibration\s+(\d{2}-\d{2}-\d{4})", text)
            if match:
                depol_date = datetime.strptime(match.group(1), "%d-%m-%Y").strftime("%d/%m/%Y 00:00")
        all_tables.extend(page.extract_tables())

channel_raw = all_tables[3]
channel_df = pd.DataFrame(channel_raw[3:], columns=channel_raw[2])
channel_df.columns = [
    "ATLAS ID", "SCC ID", "Minimum channel height", "Maximum channel height",
    "Dead time", "First signal rangebin", "Trigger delay", "Emission Wavelength",
    "Interference filter center", "Interference filter FWHM", "G", "H"
]

background_raw = all_tables[4]
background_df = pd.DataFrame(background_raw[3:], columns=background_raw[2])
background_df.columns = [
    "ATLAS ID", "SCC ID", "Background Low Bin", "Background High Bin",
    "Background Mode", "Background Low", "Background High"
]

# Merge PDF data
channel_data = {}
for _, row in channel_df.iterrows():
    aid = row["ATLAS ID"]
    if pd.notna(aid) and str(aid).strip() != "ATLAS ID":
        channel_data[aid] = row.to_dict()
for _, row in background_df.iterrows():
    aid = row["ATLAS ID"]
    if aid in channel_data:
        channel_data[aid].update(row.to_dict())

# === READ CSV FILE ===
with open(CSV_FILE, "r", encoding="utf-8") as f:
    lines = f.readlines()

section_indices = {line.strip(): i for i, line in enumerate(lines) if line.strip() in ["HoiChannels", "PolarizationCrosstalkParameter"]}
hoi_start = section_indices.get("HoiChannels")
polar_start = section_indices.get("PolarizationCrosstalkParameter")

if hoi_start is None:
    raise ValueError("HoiChannels section not found in CSV.")
if polar_start is None:
    raise ValueError("PolarizationCrosstalkParameter section not found in CSV.")

hoi_columns = [h.strip() for h in lines[hoi_start + 1].strip().split(",")]
hoi_data_lines = lines[hoi_start + 2:polar_start - 1]
original_hoi_df = pd.DataFrame([dict(zip(hoi_columns, line.strip().split(","))) for line in hoi_data_lines if line.strip()])

polar_columns = [h.strip() for h in lines[polar_start + 1].strip().split(",")]
polar_data_lines = lines[polar_start + 2:]
polar_df = pd.DataFrame([dict(zip(polar_columns, line.strip().split(","))) for line in polar_data_lines if line.strip()])

# === REVERSED MATCHING: PDF is reference, CSV is matched ===
atlas_to_csv_id = {}
used_csv_ids = set()
log_lines = []

matched_data = {}
for aid, match in channel_data.items():
    atlas_parts = parse_atlas_id(aid)
    try:
        match_wl = int(atlas_parts["wavelength"])
    except ValueError:
        log_lines.append(f"Skipped invalid ATLAS ID (bad wavelength): {aid}")
        continue
    match_mode = "an" if atlas_parts["mode"] == "a" else "pc"

    possible_matches = original_hoi_df[
        (original_hoi_df["_detection_mode_id_id"] == match_mode) &
        (original_hoi_df["if_center"].apply(lambda x: normalize_wavelength(x) == match_wl)) &
        (~original_hoi_df["id"].isin(used_csv_ids))
    ]

    if len(possible_matches) > 0:
        row = possible_matches.iloc[0].copy()
        log_lines.append(f"Matched automatically: {aid} → CSV ID: {row['id']}")
        used_csv_ids.add(row["id"])
        atlas_to_csv_id[aid] = row["id"]
        matched_data[row["id"]] = match
    else:
        log_lines.append(f"Not matched: {aid}")

# Update HoiChannels while keeping order
updated_rows = []
for _, row in original_hoi_df.iterrows():
    row = row.copy()
    cid = row["id"]
    if cid in matched_data:
        match = matched_data[cid]
        row["if_center"] = match.get("Interference filter center", row.get("if_center", ""))
        row["if_fwhm"] = match.get("Interference filter FWHM", row.get("if_fwhm", ""))
        row["emission_wavelength"] = match.get("Emission Wavelength", row.get("emission_wavelength", ""))
        row["dead_time"] = match.get("Dead time", row.get("dead_time", ""))
        row["first_signal_rangebin"] = match.get("First signal rangebin", row.get("first_signal_rangebin", ""))
        row["trigger_delay"] = match.get("Trigger delay", row.get("trigger_delay", ""))
        row["_background_mode_id_id"] = 0 if match.get("Background Mode") == "Pre-Trigger" else 1
        row["_Minimum_channel_height_"] = match.get("Minimum channel height", "")
        row["_Maximum_channel_height_"] = match.get("Maximum channel height", "")
    updated_rows.append(row)

if "_Minimum_channel_height_" not in hoi_columns:
    hoi_columns += ["_Minimum_channel_height_", "_Maximum_channel_height_"]

updated_hoi_df = pd.DataFrame(updated_rows).reindex(columns=hoi_columns)

# === POLARIZATION SECTION ===
polar_updates = []
for aid, match in channel_data.items():
    if aid in atlas_to_csv_id and pd.notna(match.get("G")) and pd.notna(match.get("H")):
        entry = {col: "" for col in polar_columns}
        entry["channel_id"] = atlas_to_csv_id[aid]
        entry["g"] = match["G"]
        entry["h"] = match["H"]
        entry["measurement_date"] = depol_date
        polar_updates.append(entry)

polar_df_clean = pd.DataFrame(polar_updates, columns=polar_columns)
polar_df_clean = polar_df_clean[polar_df_clean["g"].notna() & polar_df_clean["h"].notna()]

# === SAVE OUTPUT ===
with open(OUTPUT_FILE, "w", encoding="utf-8") as f:
    f.writelines(lines[:hoi_start + 1])
    f.write(",".join(hoi_columns) + "\n")
    updated_hoi_df.to_csv(f, index=False, header=False, lineterminator="\n")
    f.write("\n")
    f.write("PolarizationCrosstalkParameter\n")
    f.write(",".join(polar_columns) + "\n")
    polar_df_clean.to_csv(f, index=False, header=False, lineterminator="\n")
    f.write("\n")
    f.write("Products\n")
    f.writelines(lines[lines.index("Products\n") + 1:])

# === CLEANUP EMPTY POLAR ROWS ===
with open(OUTPUT_FILE, "r", encoding="utf-8") as f:
    all_lines = f.readlines()

try:
    polar_start = all_lines.index("PolarizationCrosstalkParameter\n")
    products_index = all_lines.index("Products\n")
    polar_header = all_lines[polar_start + 1]
    polar_data = all_lines[polar_start + 2:products_index]
    polar_df = pd.read_csv(StringIO(polar_header + "".join(polar_data)))
    polar_df = polar_df[polar_df["g"].notna() & polar_df["g"].astype(str).str.strip().ne("")]

    with open(OUTPUT_FILE, "w", encoding="utf-8") as f:
        f.writelines(all_lines[:polar_start + 1])
        f.write(polar_header)
        polar_df.fillna("").astype(str).to_csv(f, index=False, header=False, lineterminator="\n")
        f.write("\n")
        f.writelines(all_lines[products_index:])
except Exception as e:
    print("Post-cleanup error:", e)

# === LOG FILE ===
with open(LOG_FILE, "w", encoding="utf-8") as logf:
    logf.write("Channel Matching Log\n")
    logf.write("====================\n\n")
    for line in log_lines:
        logf.write(line + "\n")

print(f"\nDONE: Saved updated file as '{OUTPUT_FILE}'")
print(f"Matching log saved as '{LOG_FILE}'")
