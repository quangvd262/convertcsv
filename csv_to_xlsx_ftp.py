"""
CSV to XLSX Converter with FTP Support
- Auto-detects CSV encoding (handles UTF-8, UTF-8-BOM, Shift-JIS, etc.)
- Converts CSV to XLSX using openpyxl (no encoding issues in Excel)
- Downloads CSV from FTP and uploads XLSX back to FTP

Usage:
    pip install -r requirements.txt
    python csv_to_xlsx_ftp.py
"""

import ftplib
import os
import tempfile
import logging
from pathlib import Path

import chardet
import pandas as pd

# ==============================================================================
# CONFIGURATION - Edit these values before running
# ==============================================================================

FTP_HOST = "ftp.example.com"
FTP_USER = "your_username"
FTP_PASS = "your_password"
FTP_PORT = 21
FTP_TIMEOUT = 30  # seconds

# Remote path of the CSV file on FTP server
FTP_REMOTE_CSV = "/path/to/input.csv"

# Remote path where the XLSX file will be uploaded
FTP_REMOTE_XLSX = "/path/to/output.xlsx"

# Optional: set to True to use FTPS (FTP over TLS)
USE_FTPS = False

# Encoding fallback order if chardet fails or confidence is low
ENCODING_FALLBACKS = ["shift-jis", "utf-8-sig", "utf-8", "cp932", "latin-1"]
CHARDET_MIN_CONFIDENCE = 0.7

# ==============================================================================

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger(__name__)


def connect_ftp() -> ftplib.FTP:
    """Connect to FTP server and return connection object."""
    log.info(f"Connecting to FTP: {FTP_HOST}:{FTP_PORT}")
    if USE_FTPS:
        ftp = ftplib.FTP_TLS()
        ftp.connect(FTP_HOST, FTP_PORT, timeout=FTP_TIMEOUT)
        ftp.login(FTP_USER, FTP_PASS)
        ftp.prot_p()  # Switch to secure data connection
    else:
        ftp = ftplib.FTP()
        ftp.connect(FTP_HOST, FTP_PORT, timeout=FTP_TIMEOUT)
        ftp.login(FTP_USER, FTP_PASS)
    ftp.set_pasv(True)
    log.info("FTP connected successfully")
    return ftp


def download_from_ftp(ftp: ftplib.FTP, remote_path: str, local_path: str) -> None:
    """Download a file from FTP server to local path."""
    log.info(f"Downloading: {remote_path} -> {local_path}")
    with open(local_path, "wb") as f:
        ftp.retrbinary(f"RETR {remote_path}", f.write)
    size = os.path.getsize(local_path)
    log.info(f"Downloaded {size:,} bytes")


def upload_to_ftp(ftp: ftplib.FTP, local_path: str, remote_path: str) -> None:
    """Upload a local file to FTP server."""
    log.info(f"Uploading: {local_path} -> {remote_path}")

    # Ensure remote directory exists
    remote_dir = str(Path(remote_path).parent).replace("\\", "/")
    try:
        ftp.mkd(remote_dir)
    except ftplib.error_perm:
        pass  # Directory already exists

    with open(local_path, "rb") as f:
        ftp.storbinary(f"STOR {remote_path}", f)
    log.info("Upload complete")


def detect_encoding(file_path: str) -> str:
    """
    Detect file encoding using chardet.
    Falls back to ENCODING_FALLBACKS list if confidence is low.
    """
    with open(file_path, "rb") as f:
        raw = f.read()

    result = chardet.detect(raw)
    encoding = result.get("encoding")
    confidence = result.get("confidence", 0)

    log.info(f"chardet detected: encoding={encoding}, confidence={confidence:.2%}")

    if encoding and confidence >= CHARDET_MIN_CONFIDENCE:
        return encoding

    log.warning(
        f"Low confidence ({confidence:.2%}), trying fallback encodings: {ENCODING_FALLBACKS}"
    )
    return None  # Will trigger fallback logic in convert_csv_to_xlsx


def convert_csv_to_xlsx(csv_path: str, xlsx_path: str) -> None:
    """
    Read CSV with auto-detected encoding and write as XLSX.
    Tries multiple encodings until one succeeds.
    """
    detected = detect_encoding(csv_path)
    encodings_to_try = []

    if detected:
        encodings_to_try.append(detected)
    encodings_to_try.extend(ENCODING_FALLBACKS)

    # Deduplicate while preserving order
    seen = set()
    encodings_to_try = [e for e in encodings_to_try if not (e in seen or seen.add(e))]

    df = None
    used_encoding = None

    for enc in encodings_to_try:
        try:
            log.info(f"Trying encoding: {enc}")
            df = pd.read_csv(csv_path, encoding=enc, dtype=str, keep_default_na=False)
            used_encoding = enc
            log.info(f"Successfully read CSV with encoding: {enc} ({len(df)} rows, {len(df.columns)} columns)")
            break
        except (UnicodeDecodeError, LookupError) as e:
            log.warning(f"Encoding {enc} failed: {e}")

    if df is None:
        raise RuntimeError(
            f"Failed to read CSV with any of the tried encodings: {encodings_to_try}"
        )

    log.info(f"Writing XLSX: {xlsx_path}")
    with pd.ExcelWriter(xlsx_path, engine="openpyxl") as writer:
        df.to_excel(writer, index=False, sheet_name="Sheet1")

    size = os.path.getsize(xlsx_path)
    log.info(f"XLSX created: {size:,} bytes (encoding used: {used_encoding})")


def main():
    tmp_csv = None
    tmp_xlsx = None

    try:
        # Create temp files
        tmp_dir = tempfile.gettempdir()
        tmp_csv = os.path.join(tmp_dir, "input_temp.csv")
        tmp_xlsx = os.path.join(tmp_dir, "output_temp.xlsx")

        # Connect to FTP
        ftp = connect_ftp()

        try:
            # Step 1: Download CSV from FTP
            download_from_ftp(ftp, FTP_REMOTE_CSV, tmp_csv)

            # Step 2: Convert CSV -> XLSX
            convert_csv_to_xlsx(tmp_csv, tmp_xlsx)

            # Step 3: Upload XLSX to FTP
            upload_to_ftp(ftp, tmp_xlsx, FTP_REMOTE_XLSX)

        finally:
            ftp.quit()
            log.info("FTP connection closed")

        log.info("Done! Conversion and upload completed successfully.")

    except Exception as e:
        log.error(f"Error: {e}", exc_info=True)
        raise

    finally:
        # Cleanup temp files
        for f in [tmp_csv, tmp_xlsx]:
            if f and os.path.exists(f):
                os.remove(f)
                log.debug(f"Cleaned up temp file: {f}")


if __name__ == "__main__":
    main()
