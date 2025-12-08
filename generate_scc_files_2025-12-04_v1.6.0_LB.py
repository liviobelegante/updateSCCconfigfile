#!/usr/bin/env python3
"""
Update SCC CSV (append-only polarization rows) + extract polarization_correction.csv

This script performs two main functions:
1. Updates an existing LidarConfiguration_XXX.csv file by extracting and merging
   Channel Range Limits (Min/Max height) and channel parameters from a QA PDF report
   into the HoiChannels section (with a fixed schema rule: only the two height columns
   may be created, and they are always placed last).
2. Extracts the 'Polarization Correction Parameters...' table from the PDF
   and formats it into a separate polarization_correction_XXX.csv file, correctly
   prefixing data rows with the Configuration ID for SCC import compliance.

Usage:
- Place this script in the same folder as your QA PDF and LidarConfiguration*.csv.
- Run: generate_scc_files_*.py
- Outputs are saved in ./toSCC.

Version History:

- 1.3.3 (2025-10-06):
    polarization_correction.csv filename includes configuration ID

- 1.3.4 (2025-10-10):
    Improved parsing of channel range table

- 1.3.5 (2025-10-12):
    Improved SCC ID matching robustness

- 1.3.6 (2025-10-17):
    Added warnings for missing columns

- 1.3.7 (2025-10-22):
    Enforced min/max columns position

- 1.3.8 (2025-12-02):
    Support for NEW SCC CSV format with min/max columns

- 1.3.9 (2025-12-03):
    Canonical lowercase names for min/max, remove duplicates

- 1.6.0 (2025-10-05):
    New structure compatible with CARPORT 


    
"""





from __future__ import annotations

import logging
import re
from datetime import datetime
from io import BufferedReader, BufferedWriter, StringIO, BytesIO
from pathlib import Path
from typing import Union, Tuple, List, Dict

import pandas as pd
import pdfplumber

# Make pdfminer quieter
logging.getLogger("pdfminer").setLevel(logging.ERROR)

# Type aliases
PdfInput = Union[str, Path, BufferedReader, BytesIO]
CsvInput = Union[str, Path, BufferedReader, StringIO]
CsvOutput = Union[str, Path, BufferedWriter, StringIO]
LogOutput = Union[str, Path, BufferedWriter, StringIO]
PolOutput = Union[str, Path, BufferedWriter, StringIO]


# =============================================================================
# Generic helpers
# =============================================================================

def _canon(s: str) -> str:
    """Canonicalize string: lowercase alnum only."""
    return re.sub(r"[^a-z0-9]", "", str(s).lower())


def _resolve_column(existing_cols: List[str], *candidates: str) -> str:
    """
    Given existing column names and possible candidates, returns the first
    actual existing column matching any candidate (case/spacing insensitive).
    If none exist, returns the first candidate as canonical name.
    """
    canon_map = {_canon(c): c for c in existing_cols}
    for cand in candidates:
        cc = _canon(cand)
        if cc in canon_map:
            return canon_map[cc]
    return candidates[0]


def _get_existing(existing_cols: List[str], *candidates: str) -> str | None:
    """
    Return the actual existing column name matching any of the candidates
    (canon insensitive), or None if none exist.
    """
    canon_map = {_canon(c): c for c in existing_cols}
    for cand in candidates:
        cc = _canon(cand)
        if cc in canon_map:
            return canon_map[cc]
    return None


def _set_if_exists(row: pd.Series, colname: str | None, value) -> None:
    """Assign value to row[colname] only if column exists and value is non empty."""
    if colname and colname in row.index and value not in (None, "", "nan", "NaN"):
        row[colname] = value


def _ensure_last_columns(df: pd.DataFrame, header_list: List[str], required_tail: List[str]) -> None:
    """
    Ensure each name in required_tail exists in df (create empty if missing) and
    move them to the end of header_list preserving their order.
    Only the required_tail columns may be created.
    """
    for col in required_tail:
        if col not in df.columns:
            df[col] = ""

    header_seen = set()
    new_order: List[str] = []
    for c in header_list:
        if c not in required_tail and c not in header_seen:
            header_seen.add(c)
            new_order.append(c)
    new_order.extend(required_tail)
    header_list[:] = new_order


# =============================================================================
# Date handling helpers
# =============================================================================

def _parse_depol_date_from_text(txt: str) -> str:
    """
    Finds the date that comes after the phrase "Depolarization calibration"
    in the PDF text and converts it to "YYYY-MM-DD 00:00:00".
    Looks only after that phrase to avoid catching Rayleigh test dates.
    """
    if not txt:
        return ""
    m_phrase = re.search(r"depolarization\s*calibration", txt, flags=re.IGNORECASE)
    if not m_phrase:
        return ""
    after_text = txt[m_phrase.end():]
    m_date = re.search(r"(\d{2}[./]\d{2}[./]\d{4}|\d{4}-\d{2}-\d{2})", after_text)
    if not m_date:
        return ""
    raw_date = m_date.group(1).strip()

    if "." in raw_date or "/" in raw_date:
        s = raw_date.replace(".", "/")
        try:
            dt = datetime.strptime(s, "%d/%m/%Y")
            return dt.strftime("%Y-%m-%d 00:00:00")
        except ValueError:
            return ""
    else:
        try:
            dt = datetime.strptime(raw_date, "%Y-%m-%d")
            return dt.strftime("%Y-%m-%d 00:00:00")
        except ValueError:
            return ""


def _parse_date_ymd_zeros(human_date: str) -> str:
    """
    Accepts dd.mm.yyyy or dd/mm/yyyy and returns "YYYY-MM-DD 00:00:00".
    Returns "" if parsing fails.
    """
    if not human_date:
        return ""
    s = human_date.strip().replace(".", "/")
    try:
        dt = datetime.strptime(s, "%d/%m/%Y")
        return dt.strftime("%Y-%m-%d 00:00:00")
    except ValueError:
        return ""


def _find_config_validity_dates(all_tables: List[list], cfg_id: str) -> Tuple[str, str]:
    """
    Locate the configuration validity table and return (start, end) dates
    formatted as "YYYY-MM-DD 00:00:00".

    Selection rule:
    - Choose the LAST data row whose Config cell starts with "new" or with a digit.
    - If none match, use the LAST data row.
    """
    wanted = ["config", "start", "end"]

    def canon(s: str) -> str:
        return re.sub(r"[^a-z0-9]", "", str(s).lower())

    target_table = None
    header_idx = None

    # 1) find the table whose header mentions Config / Start / End (case and spacing agnostic)
    for tbl in all_tables:
        if not tbl or not any(tbl):
            continue
        for ri, row in enumerate(tbl[:7]):
            cells = [c for c in row if c is not None]
            if not cells:
                continue
            header_line = " ".join(str(c) for c in cells)
            if all(k in canon(header_line) for k in wanted):
                target_table, header_idx = tbl, ri
                break
        if target_table is not None:
            break

    if target_table is None:
        return "", ""

    # 2) resolve column indices
    cols = [str(c or "").strip() for c in target_table[header_idx]]
    canon_cols = [canon(c) for c in cols]

    def _col_index(keys: List[str]) -> int | None:
        for i, cc in enumerate(canon_cols):
            if any(k in cc for k in keys):
                return i
        return None

    idx_cfg = _col_index(["configid", "configurationid", "config"])
    idx_start = _col_index(["startdate", "start"])
    idx_end = _col_index(["enddate", "end"])

    if idx_cfg is None or idx_start is None or idx_end is None:
        return "", ""

    # 3) collect data rows (non-empty)
    data_rows = [r for r in target_table[header_idx + 1:] if any((c not in (None, "")) for c in r)]
    if not data_rows:
        return "", ""

    # 4) choose the LAST row whose Config starts with "new" or a digit
    chosen = None
    for r in reversed(data_rows):
        cfg_val = str((r[idx_cfg] if idx_cfg < len(r) else "") or "").strip()
        if not cfg_val:
            continue
        if cfg_val.lower().startswith("new") or re.match(r"^\d", cfg_val):
            chosen = r
            break
            
            
    # 5) if none matched, use the last data row
    if chosen is None:
        chosen = data_rows[-1]

    start_raw = str((chosen[idx_start] if idx_start < len(chosen) else "") or "").strip()
    end_raw = str((chosen[idx_end] if idx_end < len(chosen) else "") or "").strip()

    return _parse_date_ymd_zeros(start_raw), _parse_date_ymd_zeros(end_raw)


def _update_hoisystems_dates_in_lines(
    lines: List[str],
    cfg_id: str,
    start_dt: str,
    end_dt: str
) -> List[str]:
    """
    Update configuration_from and configuration_to in the HoiSystems section
    for the row with id == cfg_id. Returns a new list of lines.
    """
    if not start_dt and not end_dt:
        return lines

    try:
        hs_start = next(i for i, ln in enumerate(lines) if ln.strip() == "HoiSystems")
    except StopIteration:
        return lines

    if hs_start + 1 >= len(lines):
        return lines

    header_line = lines[hs_start + 1].strip()
    headers = [h.strip() for h in header_line.split(",")]

    def idx(name: str) -> int | None:
        canon_map = {_canon(h): i for i, h in enumerate(headers)}
        return canon_map.get(_canon(name))

    id_idx = idx("id")
    from_idx = idx("configuration_from")
    to_idx = idx("configuration_to")

    if id_idx is None or from_idx is None or to_idx is None:
        return lines

    i = hs_start + 2
    while i < len(lines):
        s = lines[i].strip()
        if not s:
            break
        if s in {"HoiChannels", "PolarizationCrosstalkParameter", "Products"}:
            break
        parts = [p.strip() for p in lines[i].rstrip("\n").split(",")]
        if len(parts) < len(headers):
            parts += [""] * (len(headers) - len(parts))

        if parts[id_idx] == str(cfg_id):
            if start_dt:
                parts[from_idx] = start_dt
            if end_dt:
                parts[to_idx] = end_dt
            lines[i] = ",".join(parts) + "\n"
        i += 1

    return lines


# =============================================================================
# Polarization correction table processing
# =============================================================================

def extract_polarization_correction_table(
    pdf_path: Path,
    output_stream: Union[str, Path, BufferedWriter, StringIO]
) -> List[str]:
    """
    Extracts the "Polarization Correction Parameters and Associated Product Uncertainties"
    table from the PDF and writes the raw, unformatted data directly to the output
    CSV-like stream. Returns list of CSV lines.
    """
    title = "Polarization Correction Parameters and Associated Product Uncertainties"

    need_close = False
    if isinstance(output_stream, (str, Path)):
        f = open(output_stream, "w", encoding="utf-8", newline="")
        need_close = True
    else:
        f = output_stream

    csv_lines: List[str] = []
    extracted = False

    with pdfplumber.open(pdf_path) as pdf:
        for page in pdf.pages:
            text = page.extract_text() or ""
            if title in text:
                tables = page.extract_tables() or []
                for table in tables:
                    if table and len(table) > 1 and len(table[0]) > 3:
                        for row in table:
                            cleaned_row = [
                                "" if cell is None
                                else str(cell).replace("\n", " ").replace("\r", " ").strip()
                                for cell in row
                            ]
                            csv_lines.append(",".join(cleaned_row))
                        if csv_lines:
                            f.write("\n".join(csv_lines))
                        extracted = True
                        break
            if extracted:
                break

    if not extracted:
        pd.DataFrame([["TABLE NOT FOUND", title]]).to_csv(
            f, index=False, header=False
        )
        csv_lines = ["TABLE NOT FOUND," + title]

    if need_close:
        f.close()

    return csv_lines


def postprocess_polarization_csv(input_stream: Union[str, Path, StringIO], cfg_id: str) -> str:
    """
    Rewrite the extracted polarization correction CSV so that:
      - All lines before the data header are removed.
      - The first line is replaced with the required header.
      - Each data row is prefixed with the numeric SCC config ID.

    Returns the final CSV content as a single string.
    """
    REQUIRED_HEADER = (
        "SCC_config_ID, Atlas_ID_R, SCC_ID_R, Atlas_ID_T, SCC_ID_T, "
        "VLDR_residual, Min_bsc_ratio_PLDR, K, ND_T_ratio, ND_T_ratio_uncertainty, "
        "K*ND_T_ratio, K*ND_T_ratio_uncertainty"
    )

    def process_stream(stream: StringIO) -> str:
        raw = stream.read().splitlines()
        if not raw:
            return REQUIRED_HEADER + "\n"

        header_pat = re.compile(
            r'\bATLAS\b\s*ID\s*,\s*SCC\s*ID\s*,\s*ATLAS\s*ID\s*,\s*SCC\s*ID\b',
            re.IGNORECASE
        )

        data_header_idx = None
        for i, line in enumerate(raw):
            if header_pat.search(line):
                data_header_idx = i
                break

        start_idx = (data_header_idx + 1) if data_header_idx is not None else 0

        out_lines: List[str] = [REQUIRED_HEADER]
        for line in raw[start_idx:]:
            if not line.strip() or not re.search(r"[^,\s]", line):
                continue
            out_lines.append(f"{cfg_id}," + line.lstrip())

        return "\n".join(out_lines) + "\n"

    if isinstance(input_stream, (str, Path)):
        txt = Path(input_stream).read_text(encoding="utf-8")
        return process_stream(StringIO(txt))
    else:
        return process_stream(input_stream)


# =============================================================================
# PDF reading utilities (tables 3 and 4 hardcoded, as in 1.3.9)
# =============================================================================

def read_pdf_file(
    pdf_path: PdfInput,
    cfg_id: str
) -> Tuple[Dict[str, Dict[str, str]], str, str, str]:
    """
    Read QA PDF and return:
      - channel_data: dict keyed by ATLAS ID containing merged Channel Range Limits
                      and Background table info
      - depol_date: depolarization calibration date as "YYYY-MM-DD 00:00:00"
      - start_dt_cfg, end_dt_cfg: configuration validity dates for HoiSystems
    """
    with pdfplumber.open(pdf_path) as pdf:
        depol_date = ""
        all_tables: List[list] = []
        for page in pdf.pages:
            text = page.extract_text() or ""
            if not depol_date:
                depol_date = _parse_depol_date_from_text(text)
            tables = page.extract_tables() or []
            all_tables.extend(tables)

    start_dt_cfg, end_dt_cfg = _find_config_validity_dates(all_tables, cfg_id)

    if len(all_tables) < 5:
        raise ValueError(f"Expected at least 5 tables in the PDF, found {len(all_tables)}. Check PDF structure.")

    # Channel Range Limits table (Table 3)
    channel_raw = all_tables[3]
    channel_df = pd.DataFrame(channel_raw[3:], columns=channel_raw[2])
    channel_df.columns = [
        "ATLAS ID", "SCC ID", "Minimum channel height", "Maximum channel height",
        "Dead time", "First signal rangebin", "Trigger delay", "Emission Wavelength",
        "Interference filter center", "Interference filter FWHM", "G", "H"
    ]

    # Background table (Table 4)
    background_raw = all_tables[4]
    background_df = pd.DataFrame(background_raw[3:], columns=background_raw[2])
    background_df.columns = [
        "ATLAS ID", "SCC ID", "Background Low Bin", "Background High Bin",
        "Background Mode", "Background Low", "Background High"
    ]

    channel_data: Dict[str, Dict[str, str]] = {}
    for df in [channel_df, background_df]:
        for _, row in df.iterrows():
            aid = str(row.get("ATLAS ID", "")).strip()
            if not aid or aid == "ATLAS ID":
                continue
            if aid not in channel_data:
                channel_data[aid] = {}
            for k, v in row.to_dict().items():
                if pd.notna(v):
                    channel_data[aid][k] = str(v).strip()

    return channel_data, depol_date, start_dt_cfg, end_dt_cfg


# =============================================================================
# CSV reading utilities
# =============================================================================

def read_csv_file(
    csv: CsvInput,
    cfg_id: str,
    start_dt_cfg: str,
    end_dt_cfg: str
) -> List[str]:
    """
    Read existing LidarConfiguration CSV from a path or stream and update
    HoiSystems dates in memory.
    """
    if isinstance(csv, (str, Path)):
        lines = Path(csv).read_text(encoding="utf-8").splitlines(True)
    else:
        lines = csv.read().splitlines(True)

    lines = _update_hoisystems_dates_in_lines(lines, cfg_id, start_dt_cfg, end_dt_cfg)
    return lines


def get_section_indices(lines: List[str]) -> Tuple[int, int, int]:
    """
    Locate the starting lines for HoiChannels, PolarizationCrosstalkParameter, and Products.
    """
    section_indices = {
        line.strip(): i
        for i, line in enumerate(lines)
        if line.strip() in ["HoiChannels", "PolarizationCrosstalkParameter", "Products"]
    }
    hoi_start = section_indices.get("HoiChannels")
    polar_start = section_indices.get("PolarizationCrosstalkParameter")
    products_start = section_indices.get("Products")

    if any(idx is None for idx in [hoi_start, polar_start, products_start]):
        raise ValueError(
            "One or more required sections (HoiChannels, PolarizationCrosstalkParameter, Products) not found in CSV."
        )

    return hoi_start, polar_start, products_start


def extract_hoi_dataframe(
    lines: List[str],
    hoi_start: int,
    polar_start: int
) -> Tuple[pd.DataFrame, List[str]]:
    """
    Extract HoiChannels dataframe and column list from raw CSV lines.
    """
    hoi_columns = [h.strip() for h in lines[hoi_start + 1].strip().split(",")]
    hoi_data_lines = lines[hoi_start + 2:polar_start - 1]
    original_hoi_df = pd.DataFrame(
        [
            dict(zip(hoi_columns, [c.strip() for c in line.strip().split(",")]))
            for line in hoi_data_lines if line.strip()
        ]
    )
    return original_hoi_df, hoi_columns


def build_csv_atlas_map(original_hoi_df: pd.DataFrame) -> Dict[str, str]:
    """
    Build a dict mapping atlas_id (lowercase) to CSV channel id.
    """
    existing_cols = list(original_hoi_df.columns)
    atlas_col = _get_existing(existing_cols, "atlas_id", "ATLAS ID", "atlasid", "atlas")
    atlas_map: Dict[str, str] = {}
    if atlas_col:
        for _, r in original_hoi_df.iterrows():
            aid_val = str(r.get(atlas_col, "")).strip().lower()
            cid_val = str(r.get("id", "")).strip()
            if aid_val and cid_val and aid_val not in atlas_map:
                atlas_map[aid_val] = cid_val
    return atlas_map


def match_pdf_to_csv(
    channel_data: Dict[str, Dict[str, str]],
    original_hoi_df: pd.DataFrame,
    csv_atlas_map: Dict[str, str]
) -> Tuple[Dict[str, str], Dict[str, Dict[str, str]], List[str]]:
    """
    Match PDF ATLAS IDs + SCC IDs to CSV channel IDs.
    Returns:
      - atlas_to_csv_id: mapping ATLAS ID -> CSV channel id
      - matched_data: mapping CSV channel id -> merged PDF row dict
      - log_lines: human readable matching log
    """
    original_hoi_df["id"] = original_hoi_df["id"].astype(str).str.strip()
    csv_id_set = set(original_hoi_df["id"].tolist())

    atlas_to_csv_id: Dict[str, str] = {}
    matched_data: Dict[str, Dict[str, str]] = {}
    used_csv_ids: set[str] = set()
    log_lines: List[str] = []

    for aid, pdata in channel_data.items():
        scc_pdf_raw = str(pdata.get("SCC ID", "")).strip()
        m = re.search(r"\d+", scc_pdf_raw)
        scc_pdf = m.group(0) if m else ""

        csv_id = None
        if scc_pdf and scc_pdf in csv_id_set and scc_pdf not in used_csv_ids:
            csv_id = scc_pdf
            log_lines.append(f"Matched by SCC ID - PDF ATLAS ID: {aid}, SCC ID: {scc_pdf} -> CSV ID: {csv_id}")
        else:
            aid_key = str(aid).strip().lower()
            alt = csv_atlas_map.get(aid_key)
            if alt and alt not in used_csv_ids:
                csv_id = alt
                log_lines.append(f"Matched by ATLAS ID - PDF ATLAS ID: {aid}, SCC ID: {scc_pdf} -> CSV ID: {csv_id}")
            else:
                log_lines.append(f"Not matched - PDF ATLAS ID: {aid}, SCC ID: {scc_pdf}")

        if csv_id:
            used_csv_ids.add(csv_id)
            atlas_to_csv_id[aid] = csv_id
            matched_data[csv_id] = pdata

    return atlas_to_csv_id, matched_data, log_lines


def update_hoi_channels(
    original_hoi_df: pd.DataFrame,
    hoi_columns: List[str],
    matched_data: Dict[str, Dict[str, str]]
) -> Tuple[pd.DataFrame, List[str]]:
    """
    Update HoiChannels using channel data from the PDF.

    Rules:
      - Only the two height columns may be created.
      - The canonical column names are:
            minimum_channel_height
            maximum_channel_height
      - Any existing variants (for example "Minimum_channel_height",
        "Minimum channel height") are renamed to the canonical names.
      - The two height columns are always the last two columns in hoi_columns.
    """
    warnings_columns: List[str] = []

    existing_cols = list(original_hoi_df.columns)

    # Optional columns (do not create them if missing)
    dead_time_col = _get_existing(existing_cols, "dead_time")
    first_signal_rangebin_col = _get_existing(existing_cols, "first_signal_rangebin")
    trigger_delay_col = _get_existing(existing_cols, "trigger_delay")
    emission_wavelength_col = _get_existing(existing_cols, "emission_wavelength")
    if_center_col = _get_existing(existing_cols, "if_center")
    if_fwhm_col = _get_existing(existing_cols, "if_fwhm")
    background_mode_col = _get_existing(existing_cols, "_background_mode_id_id")

    MIN_CANON = "minimum_channel_height"
    MAX_CANON = "maximum_channel_height"

    min_existing = _get_existing(
        existing_cols,
        MIN_CANON,
        "Minimum_channel_height",
        "Minimum channel height",
        "minimum channel height",
    )
    max_existing = _get_existing(
        existing_cols,
        MAX_CANON,
        "Maximum_channel_height",
        "Maximum channel height",
        "maximum channel height",
    )

    if min_existing and min_existing != MIN_CANON:
        original_hoi_df.rename(columns={min_existing: MIN_CANON}, inplace=True)
        hoi_columns[:] = [MIN_CANON if c == min_existing else c for c in hoi_columns]

    if max_existing and max_existing != MAX_CANON:
        original_hoi_df.rename(columns={max_existing: MAX_CANON}, inplace=True)
        hoi_columns[:] = [MAX_CANON if c == max_existing else c for c in hoi_columns]

    if MIN_CANON not in original_hoi_df.columns:
        original_hoi_df[MIN_CANON] = ""
    if MAX_CANON not in original_hoi_df.columns:
        original_hoi_df[MAX_CANON] = ""

    min_col = MIN_CANON
    max_col = MAX_CANON

    required_tail = [MIN_CANON, MAX_CANON]
    _ensure_last_columns(original_hoi_df, hoi_columns, required_tail)

    expected_optionals = {
        "dead_time": dead_time_col,
        "first_signal_rangebin": first_signal_rangebin_col,
        "trigger_delay": trigger_delay_col,
        "emission_wavelength": emission_wavelength_col,
        "if_center": if_center_col,
        "if_fwhm": if_fwhm_col,
        "background_mode": background_mode_col,
    }
    for want, has in expected_optionals.items():
        if has is None:
            warnings_columns.append(
                f"WARNING: Column '{want}' not found in HoiChannels. Skipped updating it."
            )

    updated_rows = []
    for _, row in original_hoi_df.iterrows():
        row = row.copy()
        cid = row["id"]
        if cid in matched_data:
            match = matched_data[cid]

            row[min_col] = match.get("Minimum channel height", row.get(min_col, ""))
            row[max_col] = match.get("Maximum channel height", row.get(max_col, ""))

            _set_if_exists(row, dead_time_col, match.get("Dead time"))
            _set_if_exists(row, first_signal_rangebin_col, match.get("First signal rangebin"))
            _set_if_exists(row, trigger_delay_col, match.get("Trigger delay"))
            _set_if_exists(row, emission_wavelength_col, match.get("Emission Wavelength"))
            _set_if_exists(row, if_center_col, match.get("Interference filter center"))
            _set_if_exists(row, if_fwhm_col, match.get("Interference filter FWHM"))

            if background_mode_col:
                pdf_mode = str(match.get("Background Mode", "")).strip().lower().replace("-", " ")
                if pdf_mode == "pre trigger":
                    row[background_mode_col] = "0"
                elif pdf_mode == "far field":
                    row[background_mode_col] = "1"

        updated_rows.append(row)

    updated_hoi_df = pd.DataFrame(updated_rows).reindex(columns=hoi_columns)
    first_col = hoi_columns[0]
    updated_hoi_df = updated_hoi_df[updated_hoi_df[first_col] != "PolarizationCrosstalkParameter"]

    return updated_hoi_df, warnings_columns


def generate_polarization_crosstalk_rows(
    polar_columns: List[str],
    atlas_to_csv_id: Dict[str, str],
    matched_data: Dict[str, Dict[str, str]],
    depol_date: str
) -> List[str]:
    """
    Generate the lines to append to PolarizationCrosstalkParameter based on
    the matched PDF G/H values and depolarization date.
    """

    def mk_row(cid: str, g_val: str, h_val: str, meas_date: str) -> str:
        row = [""] * len(polar_columns)
        col_pos = {name: idx for idx, name in enumerate(polar_columns)}
        if "channel_id" in col_pos:
            row[col_pos["channel_id"]] = cid
        if "g" in col_pos:
            row[col_pos["g"]] = g_val
        if "h" in col_pos:
            row[col_pos["h"]] = h_val
        if "measurement_date" in col_pos:
            row[col_pos["measurement_date"]] = meas_date or ""
        return ",".join(row) + "\n"

    lines: List[str] = []
    for aid, cid in atlas_to_csv_id.items():
        pdata = matched_data[cid]
        g_val = str(pdata.get("G", "")).strip()
        h_val = str(pdata.get("H", "")).strip()
        if g_val and h_val and g_val.lower() != "nan" and h_val.lower() != "nan":
            lines.append(mk_row(cid, g_val, h_val, depol_date))

    return lines


def write_csv_file(
    output: CsvOutput,
    lines: List[str],
    hoi_start: int,
    polar_start: int,
    products_start: int,
    hoi_columns: List[str],
    updated_hoi_df: pd.DataFrame,
    new_polar_lines: List[str]
) -> None:
    """
    Write NEW_LidarConfiguration CSV to a path or stream.
    """
    need_close = False
    if isinstance(output, (str, Path)):
        f = open(output, "w", encoding="utf-8", newline="")
        need_close = True
    else:
        f = output

    f.writelines(lines[:hoi_start + 1])
    f.write(",".join(hoi_columns) + "\n")
    updated_hoi_df.to_csv(f, index=False, header=False, lineterminator="\n")
    f.write("\n")
    f.writelines(lines[polar_start:polar_start + 2])
    f.writelines(lines[polar_start + 2:products_start - 1])
    for line in new_polar_lines:
        f.write(line)
    f.write("\n")
    f.writelines(lines[products_start:])

    if need_close:
        f.close()


def write_log_file(
    log_output: LogOutput,
    log_lines: List[str],
    warnings_columns: List[str]
) -> None:
    """
    Write matching log to a path or stream.
    """
    need_close = False
    if isinstance(log_output, (str, Path)):
        f = open(log_output, "w", encoding="utf-8", newline="")
        need_close = True
    else:
        f = log_output

    f.write("Channel Matching Log (SCC ID first, ATLAS ID fallback)\n")
    f.write("==================================\n\n")
    if not log_lines:
        f.write(
            "INFO: No channel matching attempts were recorded. Please ensure PDF tables 3 and 4 "
            "contain 'ATLAS ID' and 'SCC ID' columns with data.\n"
        )
    for line in log_lines:
        f.write(line + "\n")
    if warnings_columns:
        f.write("\nColumn warnings:\n")
        for w in warnings_columns:
            f.write(w + "\n")

    if need_close:
        f.close()


# =============================================================================
# Main processing function (Victor style)
# =============================================================================

def process(
    pdf: PdfInput,
    csv: CsvInput,
    cfg_id: str,
    csv_output: CsvOutput | None = None,
    log_output: LogOutput | None = None,
    pol_corr_output: PolOutput | None = None
) -> dict:
    """
    High level pipeline, Victor style.

    Parameters:
      pdf:               QA PDF (path or stream)
      csv:               LidarConfiguration CSV (path or stream)
      cfg_id:            SCC configuration id (string)
      csv_output:        where to write NEW_LidarConfiguration CSV
      log_output:        where to write CSV_match.log
      pol_corr_output:   where to write polarization_correction_XXX.csv (raw and postprocessed)

    Returns:
      dict with a small summary (counts, dates, etc.)
    """

    # 1. Read PDF and build channel_data
    channel_data, depol_date, start_dt_cfg, end_dt_cfg = read_pdf_file(pdf, cfg_id)

    # 2. Read CSV lines and update HoiSystems dates
    lines = read_csv_file(csv, cfg_id, start_dt_cfg, end_dt_cfg)

    # 3. Section indices
    hoi_start, polar_start, products_start = get_section_indices(lines)

    # 4. Extract HoiChannels and columns
    original_hoi_df, hoi_columns = extract_hoi_dataframe(lines, hoi_start, polar_start)

    # 5. HoiChannels sanity: ensure "id" column
    if "id" not in original_hoi_df.columns:
        id_col = next((c for c in original_hoi_df.columns if _canon(c) == "id"), None)
        if id_col:
            original_hoi_df.rename(columns={id_col: "id"}, inplace=True)

    if "id" not in original_hoi_df.columns:
        raise ValueError("Cannot find 'id' column in HoiChannels section for merging.")

    # 6. Polarization header
    polar_columns = [c.strip() for c in lines[polar_start + 1].strip().split(",")]

    # 7. Build atlas map and match PDF to CSV
    csv_atlas_map = build_csv_atlas_map(original_hoi_df)
    atlas_to_csv_id, matched_data, log_lines = match_pdf_to_csv(
        channel_data, original_hoi_df, csv_atlas_map
    )

    # 8. Update HoiChannels with min/max and other optional fields
    updated_hoi_df, warnings_columns = update_hoi_channels(
        original_hoi_df, hoi_columns, matched_data
    )

    # 9. Generate polarization crosstalk rows
    new_polar_lines = generate_polarization_crosstalk_rows(
        polar_columns, atlas_to_csv_id, matched_data, depol_date
    )

    # 10. Write NEW_LidarConfiguration CSV
    if csv_output is not None:
        write_csv_file(
            csv_output,
            lines,
            hoi_start,
            polar_start,
            products_start,
            hoi_columns,
            updated_hoi_df,
            new_polar_lines
        )

    # 11. Write log file
    if log_output is not None:
        write_log_file(log_output, log_lines, warnings_columns)

    # 12. Polarization correction CSV: extract + postprocess
    pol_final_str = ""
    if pol_corr_output is not None:
        extract_polarization_correction_table(
            Path(pdf) if isinstance(pdf, (str, Path)) else Path("dummy.pdf"),
            pol_corr_output
        )
        pol_final_str = postprocess_polarization_csv(pol_corr_output, cfg_id)
        if isinstance(pol_corr_output, (str, Path)):
            Path(pol_corr_output).write_text(pol_final_str, encoding="utf-8")

    return {
        "matched_channels": len(matched_data),
        "total_channels": len(original_hoi_df),
        "depol_date": depol_date,
        "config_validity_start": start_dt_cfg,
        "config_validity_end": end_dt_cfg,
        "polarization_rows": len(new_polar_lines),
    }


# =============================================================================
# CLI entry point
# =============================================================================

def main() -> None:
    root_dir = Path(__file__).resolve().parent
    out_dir = root_dir / "toSCC"
    out_dir.mkdir(exist_ok=True)

    pdf_matches = sorted(root_dir.glob("*.pdf"))
    csv_matches = sorted(root_dir.glob("LidarConfiguration*.csv"))

    if not pdf_matches:
        raise FileNotFoundError("No PDF file found in current directory.")
    if not csv_matches:
        raise FileNotFoundError("No CSV file matching 'LidarConfiguration*.csv' found in current directory.")
    if len(csv_matches) > 1:
        raise FileNotFoundError(f"Expected one CSV file, found multiple: {[p.name for p in csv_matches]}")

    pdf_file = pdf_matches[0]
    csv_file = csv_matches[0]

    m = re.search(r"LidarConfiguration_([0-9]{3,5})_", csv_file.name)
    cfg_id = m.group(1) if m else "unknown"

    output_file = out_dir / ("NEW_" + csv_file.name)
    log_file = out_dir / "CSV_match.log"
    pol_corr_csv = out_dir / f"polarization_correction_{cfg_id}.csv"

    summary = process(
        pdf=pdf_file,
        csv=csv_file,
        cfg_id=cfg_id,
        csv_output=output_file,
        log_output=log_file,
        pol_corr_output=pol_corr_csv,
    )

    print(f"Saved: {output_file}")
    print(f"Saved: {pol_corr_csv}")
    print(f"Saved log: {log_file}")
    print("Summary:", summary)


if __name__ == "__main__":
    main()
