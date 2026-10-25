import functions_framework
from google.cloud import storage

OUTPUT_PREFIX = "results/"

@functions_framework.cloud_event
def longest_line(cloud_event):
    data = cloud_event.data
    # Event arrives via Pub/Sub notification from GCS
    attrs = data["message"]["attributes"]
    bucket_name = attrs["bucketId"]
    file_name = attrs["objectId"]

    # Prevent infinite loop: our own output upload also triggers this function
    if file_name.startswith(OUTPUT_PREFIX):
        print(f"Skipping output file: {file_name}")
        return

    print(f"Triggered by upload: gs://{bucket_name}/{file_name}")

    client = storage.Client()
    bucket = client.bucket(bucket_name)
    text = bucket.blob(file_name).download_as_text()
    lines = text.splitlines()

    if not lines:
        result = f"File {file_name} is empty."
    else:
        max_len = max(len(line) for line in lines)
        longest = [(i + 1, line) for i, line in enumerate(lines) if len(line) == max_len]
        out = [
            f"Input file: {file_name}",
            f"Total lines: {len(lines)}",
            f"Maximum line length: {max_len} characters",
            f"Number of longest lines: {len(longest)}",
            "",
        ]
        for line_no, line in longest:
            out.append(f"Line {line_no}: {line}")
        result = "\n".join(out)

    # Print to Cloud Logging
    print("===== LONGEST LINE RESULT =====")
    print(result)

    # Save back to the same bucket
    base = file_name.rsplit(".", 1)[0]
    out_name = f"{OUTPUT_PREFIX}{base}_longest_lines.txt"
    bucket.blob(out_name).upload_from_string(result)
    print(f"Result saved to gs://{bucket_name}/{out_name}")
    