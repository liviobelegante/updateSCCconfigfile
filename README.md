================================================================================
| LIDAR SCC CONFIGURATION AND POLARIZATION DATA GENERATOR |
================================================================================

| PURPOSE |
This script performs two critical functions for SCC submission:

1. Updates an existing Lidar Configuration CSV file by extracting Channel Range
   Limits (Min/Max height) from the QA PDF report and merging this data into the
   'HoiChannels' section.

2. Extracts the raw 'Polarization Correction Parameters and Associated Product
   Uncertainties' table from the QA PDF and formats it into a separate
   polarization_correction_XXX.csv file, ready for SCC upload.

================================================================================
| IMPORTANT NOTES |

PDF Version:
  The script supports QA reports created using version 0.1.1 or later.

Source Files:
  Only ONE PDF report and ONE LidarConfiguration*.csv file (matching the pattern
  LidarConfiguration_[ID]_*.csv) should be present in the execution folder to
  avoid ambiguity.

Table Layout:
  Each required table must be placed on a separate page of the report.
  Placing multiple tables on the same page may result in parsing errors.

Output Folder:
  All output files are placed into a new subfolder named `toSCC`.

================================================================================
| HOW TO USE |

1. Generate the QA report using version 0.1.1 or later and export it as a PDF file.
2. Place the PDF file and the original SCC configuration CSV file
   (LidarConfiguration_[ID]_[...].csv) in the same folder as this script.
3. Run the script from the command line:

   python update_scc_and_extract_polarization.py

================================================================================
| OUTPUTS (Saved to the ./toSCC folder) |

NEW_LidarConfiguration_[ID]_[...].csv:
  The updated configuration file with Min/Max channel height limits merged into
  the 'HoiChannels' section and new 'G'/'H' parameter rows appended to the
  'PolarizationCrosstalkParameter' section.

polarization_correction_[ID].csv:
  The polarization parameters file, formatted specifically for SCC upload,
  including the generated index row and configuration ID prefixes.

CSV_match.log:
  A log detailing which channels were successfully matched between the PDF
  (ATLAS ID) and the CSV (SCC ID).

================================================================================
