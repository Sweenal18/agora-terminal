import sys

path = r"C:\Projects\agora-terminal\agora-terminal\infra\docker\docker-compose.yml"
with open(path, "r", encoding="utf-8") as f:
    content = f.read()

content = content.replace("\r\n", "\n")
old = "      FMP_API_KEY: ${FMP_API_KEY:-}\n      FRED_API_KEY: ${FRED_API_KEY:-}"
new = "      FMP_API_KEY: ${FMP_API_KEY:-}\n      FINNHUB_API_KEY: ${FINNHUB_API_KEY:-}\n      FRED_API_KEY: ${FRED_API_KEY:-}"

count = content.count(old)
print(f"Found {count} occurrences")
if count == 0:
    sys.exit(1)

content = content.replace(old, new)
with open(path, "w", encoding="utf-8", newline="\n") as f:
    f.write(content)
print("Done")
