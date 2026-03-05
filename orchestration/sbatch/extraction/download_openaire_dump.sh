#!/bin/bash
#SBATCH --job-name=openaire_ingest
#SBATCH --nodes=1
#SBATCH --ntasks=1
#SBATCH --cpus-per-task=32
#SBATCH --mem=64G
#SBATCH --time=2-00:00:00
#SBATCH --partition=standard
#SBATCH --output=/vast/lu72hip/logs/openaire_ingest_%j.log
#SBATCH --mail-user=walter.ehrenberger@uni-jena.de
#SBATCH --mail-type=ALL

# --- Path Configuration ---
BASE_DIR="/vast/lu72hip/data/pile"
LOG_DIR="/vast/lu72hip/logs"
DOWNLOAD_DIR="${BASE_DIR}/openaire_2025_12_01_dump"
EXTRACT_DIR="${BASE_DIR}/openaire_extracted"
VENV_PATH="${BASE_DIR}/oa_venv"

# OpenAIRE December 2025 record
ZENODO_RECORD="17725827"

mkdir -p "$LOG_DIR"
mkdir -p "$DOWNLOAD_DIR"
mkdir -p "$EXTRACT_DIR"

# --- Venv Setup ---
if [ ! -d "$VENV_PATH" ]; then
    python3 -m venv "$VENV_PATH"
    source "$VENV_PATH/bin/activate"
    pip install --upgrade pip requests
else
    source "$VENV_PATH/bin/activate"
fi

# --- 1. Fetch file list from Zenodo API ---
echo "Fetching file list from Zenodo record ${ZENODO_RECORD}: $(date)"
python3 << EOF
import requests
import json

record_id = "${ZENODO_RECORD}"
api_url = f"https://zenodo.org/api/records/{record_id}"
response = requests.get(api_url)
data = response.json()

files = data.get('files', [])
print(f"Found {len(files)} files in record")

# Save download URLs to file
with open("${DOWNLOAD_DIR}/file_urls.txt", 'w') as f:
    for file_info in files:
        url = file_info['links']['self']
        filename = file_info['key']
        f.write(f"{url}\t{filename}\n")

print(f"Saved URLs to ${DOWNLOAD_DIR}/file_urls.txt")
print(f"Total size: {sum(f['size'] for f in files) / (1024**3):.2f} GB")
EOF

# --- 2. Download files in parallel ---
echo "Downloading files with 32 parallel connections: $(date)"

# Read URLs and download in parallel
cat "${DOWNLOAD_DIR}/file_urls.txt" | while IFS=$'\t' read -r url filename; do
    echo "$DOWNLOAD_DIR"
    echo "$url"
    echo "$filename"
done | xargs -P 32 -n 3 bash -c '
    DOWNLOAD_DIR="$1"
    URL="$2"
    FILENAME="$3"
    OUTPUT="${DOWNLOAD_DIR}/${FILENAME}"
    
    wget -q --show-progress -O "$OUTPUT" "$URL" || \
    curl -L -o "$OUTPUT" "$URL"
    
    echo "Downloaded: $FILENAME"
' _

# --- 3. Validation ---
echo "Downloaded file count: $(date)"
find "$DOWNLOAD_DIR" -name "*.tar" -o -name "*.gz" | wc -l
du -sh "$DOWNLOAD_DIR"

# --- 4. Extract tar files (each tar contains multiple .gz files) ---
echo "Extracting tar archives: $(date)"
find "$DOWNLOAD_DIR" -name "*.tar" -type f -print0 | xargs -0 -P 16 -I {} bash -c '
    TARFILE="{}"
    BASENAME=$(basename "$TARFILE" .tar)
    TEMP_DIR="'"${EXTRACT_DIR}"'/${BASENAME}_temp"
    FINAL_DIR="'"${EXTRACT_DIR}"'/${BASENAME}"
    
    # Extract tar to temp directory
    mkdir -p "$TEMP_DIR"
    tar -xf "$TARFILE" -C "$TEMP_DIR"
    
    # Now decompress all .gz files inside
    mkdir -p "$FINAL_DIR"
    find "$TEMP_DIR" -name "*.gz" -type f | while read GZ_FILE; do
        REL_PATH="${GZ_FILE#$TEMP_DIR/}"
        OUT_FILE="$FINAL_DIR/${REL_PATH%.gz}"
        OUT_DIR=$(dirname "$OUT_FILE")
        mkdir -p "$OUT_DIR"
        gunzip -c "$GZ_FILE" > "$OUT_FILE"
    done
    
    # Clean up temp dir
    rm -rf "$TEMP_DIR"
    
    echo "Processed: $BASENAME"
'

# --- 5. Final counts ---
echo "Final extracted file count: $(date)"
find "$EXTRACT_DIR" -type f -name "*.json" | wc -l
echo "Compressed size:"
du -sh "$DOWNLOAD_DIR"
echo "Extracted size:"
du -sh "$EXTRACT_DIR"
echo "Process finished: $(date)"
