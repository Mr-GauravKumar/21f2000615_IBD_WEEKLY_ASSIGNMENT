# Week 2 Graded Assignment 1 - Longest Line(s) in a GCS File

## Files
- `longest_line.py`: Downloads `sample.txt` from GCS bucket `garv-ga1-input-2026`, finds the longest line(s) by character length (reports all ties), prints the result in the terminal, and uploads it to `garv-ga1-output-2026/longest_lines_output.txt`.
- `sample.txt`: Input text file used for testing.
- `longest_lines_output.txt`: Output file downloaded from the output bucket.

## How to run (on GCP VM)
1. VM (e2-micro, asia-south1) with "Allow full access to all Cloud APIs" scope.
2. VM service account has `roles/storage.objectAdmin`.
3. `python3 -m venv venv && source venv/bin/activate && pip install google-cloud-storage`
4. `python3 longest_line.py`
