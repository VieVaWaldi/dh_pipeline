"""
Needs: $ gcloud init $ to be run first in the terminal.
Pick Models & Billing at https://console.cloud.google.com/vertex-ai/

"""

import json
import time

from dotenv import load_dotenv
from google import genai
from google.cloud import storage
from google.genai.types import CreateBatchJobConfig, JobState, HttpOptions

load_dotenv()
client = genai.Client(
    vertexai=True,
    project="digicher",
    location="europe-west3",
    http_options=HttpOptions(api_version="v1"),
)

data = [
    {"prompt": "Explain quantum computing"},
    {"prompt": "Compare Python and Java", "temperature": 0.7},
    {"prompt": "Translate 'Hello' to French", "max_output_tokens": 100},
]

# ToDo: should rm this right?
with open("batch-input.jsonl", "w") as f:
    for item in data:
        f.write(json.dumps(item) + "\n")

# Upload to GCS
bucket_name = "digicher-gemini-bucket"
storage_client = storage.Client()
bucket = storage_client.bucket(bucket_name)
blob = bucket.blob("batch-input.jsonl")
blob.upload_from_filename("batch-input.jsonl")


# ToDo: USE FAST MODEL


job = client.batches.create(
    model="gemini-1.5-flash-002",
    src="gs://your-bucket/your-input.jsonl",
    config=CreateBatchJobConfig(dest="gs://your-bucket/your-output/"),
)

print(f"Job name: {job.name}")
print(f"Job state: {job.state}")

completed_states = {
    JobState.JOB_STATE_SUCCEEDED,
    JobState.JOB_STATE_FAILED,
    JobState.JOB_STATE_CANCELLED,
    JobState.JOB_STATE_PAUSED,
}

while job.state not in completed_states:
    time.sleep(1)
    job = client.batches.get(name=job.name)
    print(f"Job state: {job.state}")
