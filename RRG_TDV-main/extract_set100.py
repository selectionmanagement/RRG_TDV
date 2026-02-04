import requests
import os
from PyPDF2 import PdfReader

url = "https://media.set.or.th/set/Documents/2025/Dec/SET50_100_H1_2026.pdf"
dest = "SET50_100_H1_2026.pdf"
if not os.path.exists(dest):
    resp = requests.get(url, timeout=20)
    resp.raise_for_status()
    with open(dest, "wb") as f:
        f.write(resp.content)

reader = PdfReader(dest)
pages = []
for page in reader.pages:
    text = page.extract_text()
    if text:
        pages.append(text)

print("\\n---PAGE---\\n".join(pages))
