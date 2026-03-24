"""
CSV to XLSX Web Converter
Flask web app: upload CSV → preview → download XLSX
"""

import io
import os
import logging

import chardet
import pandas as pd
from flask import (
    Flask,
    render_template,
    request,
    send_file,
    jsonify,
    session,
)

app = Flask(__name__)
app.secret_key = os.environ.get("SECRET_KEY", "dev-secret-change-in-prod")

# Max upload size: 10 MB
app.config["MAX_CONTENT_LENGTH"] = 10 * 1024 * 1024

ENCODING_FALLBACKS = ["utf-8-sig", "utf-8", "shift-jis", "cp932", "latin-1"]
CHARDET_MIN_CONFIDENCE = 0.7

logging.basicConfig(level=logging.INFO)
log = logging.getLogger(__name__)


def detect_and_read_csv(file_bytes: bytes) -> pd.DataFrame:
    """Detect encoding and parse CSV bytes into DataFrame."""
    result = chardet.detect(file_bytes)
    detected = result.get("encoding")
    confidence = result.get("confidence", 0)

    encodings = []
    if detected and confidence >= CHARDET_MIN_CONFIDENCE:
        encodings.append(detected)
    encodings.extend(ENCODING_FALLBACKS)

    # Deduplicate
    seen = set()
    encodings = [e for e in encodings if not (e in seen or seen.add(e))]

    for enc in encodings:
        try:
            df = pd.read_csv(
                io.BytesIO(file_bytes),
                encoding=enc,
                dtype=str,
                keep_default_na=False,
            )
            log.info(f"Read CSV with encoding={enc}, rows={len(df)}")
            return df
        except (UnicodeDecodeError, LookupError):
            continue

    raise ValueError(f"Cannot decode CSV with any encoding: {encodings}")


def df_to_xlsx_bytes(df: pd.DataFrame) -> bytes:
    """Convert DataFrame to XLSX bytes."""
    buf = io.BytesIO()
    with pd.ExcelWriter(buf, engine="openpyxl") as writer:
        df.to_excel(writer, index=False, sheet_name="Sheet1")
    buf.seek(0)
    return buf.read()


@app.route("/")
def index():
    return render_template("index.html")


@app.route("/upload", methods=["POST"])
def upload():
    if "file" not in request.files:
        return jsonify({"error": "No file uploaded"}), 400

    f = request.files["file"]
    if not f.filename:
        return jsonify({"error": "Empty filename"}), 400
    if not f.filename.lower().endswith(".csv"):
        return jsonify({"error": "Only CSV files are supported"}), 400

    try:
        file_bytes = f.read()
        df = detect_and_read_csv(file_bytes)

        # Store XLSX in session as bytes (base64) — small files only
        # For larger files, store in server memory keyed by session
        xlsx_bytes = df_to_xlsx_bytes(df)

        # Keep data in app-level cache keyed by a simple token
        token = os.urandom(16).hex()
        _cache[token] = {
            "xlsx": xlsx_bytes,
            "filename": f.filename.rsplit(".", 1)[0] + ".xlsx",
        }
        session["token"] = token

        # Build preview (max 100 rows)
        preview_df = df.head(100)
        return jsonify({
            "token": token,
            "rows": len(df),
            "cols": len(df.columns),
            "preview_html": preview_df.to_html(
                classes="table table-sm table-striped table-bordered",
                index=False,
                border=0,
            ),
            "filename": f.filename,
        })

    except Exception as e:
        log.error(f"Upload error: {e}", exc_info=True)
        return jsonify({"error": str(e)}), 500


@app.route("/download/<token>")
def download(token):
    entry = _cache.get(token)
    if not entry:
        return "File not found or expired", 404

    buf = io.BytesIO(entry["xlsx"])
    return send_file(
        buf,
        as_attachment=True,
        download_name=entry["filename"],
        mimetype="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
    )


@app.route("/ping")
def ping():
    """Health check endpoint for UptimeRobot."""
    return "OK", 200


# Simple in-memory cache (resets on restart — fine for free tier)
_cache: dict = {}

if __name__ == "__main__":
    port = int(os.environ.get("PORT", 5000))
    app.run(host="0.0.0.0", port=port)
