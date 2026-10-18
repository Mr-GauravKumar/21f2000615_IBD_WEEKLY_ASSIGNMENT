from google.cloud import storage

BUCKET_IN = "garv-ga1-input-2026"
FILE_IN = "sample.txt"
BUCKET_OUT = "garv-ga1-output-2026"
FILE_OUT = "longest_lines_output.txt"

client = storage.Client()
client.bucket(BUCKET_IN).blob(FILE_IN).download_to_filename("input.txt")

with open("input.txt", encoding="utf-8") as f:
    lines = [line.rstrip("\r\n") for line in f]

max_len = max(len(line) for line in lines)
longest = [line for line in lines if len(line) == max_len]

result = (f"Longest line length: {max_len}\n"
          f"Number of longest lines: {len(longest)}\n\n"
          + "\n".join(longest) + "\n")
print(result)

with open("output.txt", "w", encoding="utf-8") as f:
    f.write(result)
client.bucket(BUCKET_OUT).blob(FILE_OUT).upload_from_filename("output.txt")
print(f"Saved to gs://{BUCKET_OUT}/{FILE_OUT}")
