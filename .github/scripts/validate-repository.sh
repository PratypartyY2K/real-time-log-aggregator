#!/usr/bin/env bash
set -euo pipefail

docker compose -f deployments/local/docker-compose.yml config --quiet

python3 - <<'PY'
from pathlib import Path
from xml.etree import ElementTree

for path in sorted(Path("deployments/local/clickhouse").glob("*.xml")):
    ElementTree.parse(path)
    print(f"validated XML: {path}")
PY

python3 - <<'PY'
import re
from pathlib import Path

files = [Path("README.md"), *sorted(Path("docs").glob("*.md"))]
link_pattern = re.compile(r"\[[^]]+\]\(([^)]+)\)")
errors = []

for path in files:
    text = path.read_text(encoding="utf-8")
    if text.count("```") % 2:
        errors.append(f"{path}: unbalanced fenced code block")
    for target in link_pattern.findall(text):
        if target.startswith(("http://", "https://", "#")):
            continue
        clean_target = target.split("#", 1)[0]
        resolved = (path.parent / clean_target).resolve()
        if not resolved.exists():
            errors.append(f"{path}: broken local link {target}")

if errors:
    raise SystemExit("\n".join(errors))
print(f"validated Markdown links and fences in {len(files)} files")
PY
