# SCC Configuration and Polarization Data Generator

**Current version: 1.6.2**  
Release date: 14 October 2026

## 1. Purpose

This Python script generates the files required to update lidar configurations in the Single Calculus Chain (SCC), based on parameters extracted from ACTRIS CARS Quality Assurance (QA) reports.

The script updates an existing SCC lidar configuration CSV file, extracts polarization correction parameters and generates a channel matching log.

## 2. Main Features

### HoiSystems

- Updates the configuration validity start and end dates.
- Replaces commas, carriage returns (CR) and line feeds (LF) in description and comment fields with spaces.
- Validates the number of CSV columns.
- Preserves the original column names and order.

### HoiChannels

Updates the following parameters from the QA report:

- Minimum channel height
- Maximum channel height
- Dead time
- First signal rangebin
- Trigger delay
- Emission wavelength
- Interference filter center
- Interference filter FWHM
- Background mode

Channels are matched primarily by SCC ID, with ATLAS ID as a fallback.

The original CSV schema is preserved. Only the `minimum_channel_height` and `maximum_channel_height` columns may be created if missing. These columns are always placed at the end of the HoiChannels section.

### PolarizationCrosstalkParameter

- Extracts G and H parameters from the QA report.
- Includes only matched channels for which both G and H are available.
- Uses the depolarization calibration date from the QA report.
- Preserves existing polarization records and appends new records.

### Polarization Correction Parameters

Extracts the *Polarization Correction Parameters and Associated Product Uncertainties* table from the QA report.

The generated CSV contains:

- SCC configuration ID
- ATLAS and SCC IDs for reflected and transmitted channels
- VLDR residual
- Minimum backscatter ratio for PLDR
- K correction factor
- ND filter transmission ratio and uncertainty
- K × ND filter transmission ratio and uncertainty

Only valid channel-pair data rows are retained. Additional PDF headers and descriptive rows are removed.

## 3. Requirements

- Python 3.10 or later
- pandas
- pdfplumber

Install the required Python packages:

```bash
python -m pip install pandas pdfplumber
```

## 4. Input Files

The following three files must be placed in the same folder:

1. `generate_scc_files_2026-10-14_v1.6.2_LB.py`
2. The CARS QA report in PDF format.
3. The original lidar configuration CSV file exported from SCC.

Example folder structure:

```text
working_folder/
    generate_scc_files_2026-10-14_v1.6.2_LB.py
    QA_report_brc_123_202_1073_20260915_mh_20261014.pdf
    LidarConfiguration_1073_UPCLidar_brc.csv
```

### Important input requirements

- Use the CARS QA report template version 0.1.1 or later, with the expected table structure.
- Each required QA table must be placed on a separate PDF page.
- Only one QA PDF report and one original SCC configuration CSV file should be present in the working folder.
- The configuration ID in the QA report must correspond to the SCC configuration being updated.
- The original SCC configuration filename must follow the pattern `LidarConfiguration_[ID]_*.csv`.
- Empty cells in the QA report should remain empty.

## 5. How to Run

Open a terminal or command prompt in the working folder and execute:

```bash
python generate_scc_files_2026-10-14_v1.6.2_LB.py
```

No command-line arguments are required.

The script identifies the input files automatically and creates an output subfolder named `toSCC`.

The original QA PDF and SCC configuration CSV files are not modified.

## 6. Generated Files

All generated files are saved in the `toSCC` subfolder.

| File | Description |
|---|---|
| `NEW_LidarConfiguration_[ID]_*.csv` | Updated SCC lidar configuration, including validity dates, channel parameters and new polarization crosstalk records. |
| `polarization_correction_[ID].csv` | Polarization correction parameters extracted from the QA report. |
| `CSV_match.log` | Channel matching results and warnings concerning missing configuration columns. |

Example:

```text
toSCC/
    NEW_LidarConfiguration_1073_UPCLidar_brc.csv
    polarization_correction_1073.csv
    CSV_match.log
```

## 7. Output Verification

Before importing the generated files into SCC, verify:

- Configuration ID and validity dates.
- Channel matching results in `CSV_match.log`.
- Minimum and maximum channel heights.
- Updated optical and acquisition parameters.
- Newly appended G and H polarization records.
- Polarization correction parameters and channel pairs.
- CSV structure and column order.

Existing polarization records are retained. Reprocessing an already updated configuration may therefore append duplicate polarization records.

The generated files must be reviewed before submission to SCC.

## 8. CARPORT Integration

The script uses a modular processing structure centered on the `process()` function, allowing integration with external tools such as CARPORT.

The function supports separate configuration, polarization and log outputs.

Standalone execution using local files is the recommended usage described in this README.

## 9. Version History

### v1.6.2 (2026-10-14)

- Improved HoiSystems CSV parsing.
- Added support for quoted multiline descriptions.
- Replaced commas, CR and LF characters in description and comment fields with spaces.
- Added column-count validation.
- Preserved the original HoiSystems schema and column order.
- Retained all corrections introduced in v1.6.1.

### v1.6.1 (2026-10-14)

- Fixed polarization correction CSV postprocessing.
- Removed unwanted PDF headers and descriptive rows.
- Added validation of polarization channel-pair data.
- Preserved the fixed polarization correction CSV schema.

### v1.6.0 (2025-12-04)

- Introduced a modular processing structure compatible with CARPORT integration.
- Added configuration validity date updates.
- Improved channel matching using SCC ID and ATLAS ID fallback.
- Preserved existing polarization records while appending new calibration parameters.

### v1.3.x

| Version | Date | Changes |
|---|---|---|
| 1.3.9 | 2025-12-03 | Standardized lowercase height column names and removed duplicates. |
| 1.3.8 | 2025-12-02 | Added support for the new SCC CSV format with height columns. |
| 1.3.7 | 2025-10-22 | Enforced minimum and maximum height column positions. |
| 1.3.6 | 2025-10-17 | Added warnings for missing columns. |
| 1.3.5 | 2025-10-12 | Improved SCC ID matching robustness. |
| 1.3.4 | 2025-10-10 | Improved channel range table parsing. |
| 1.3.3 | 2025-10-06 | Added configuration ID to the polarization correction filename. |

## 10. License

This project is distributed under the GNU General Public License v3.0 (GPL-3.0).

See the LICENSE file for details.
