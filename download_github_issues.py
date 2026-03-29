"""
Downloads real GitHub issues as CSV with multi-line body fields.
97%+ of rows contain quoted newlines — exactly the kind of data
that breaks naive CSV splitters.

Requires: gh CLI (https://cli.github.com)
"""

import csv
import json
import subprocess
import sys

REPOS = [
    "microsoft/vscode",
    "rust-lang/rust",
    "golang/go",
    "kubernetes/kubernetes",
]
LIMIT = 2000
OUT = "github_issues.csv"

with open(OUT, "w", newline="") as f:
    writer = csv.writer(f)
    total = 0

    for repo in REPOS:
        print(f"  {repo}...", end=" ", flush=True)
        result = subprocess.run(
            ["gh", "issue", "list", "-R", repo,
             "--limit", str(LIMIT), "--state", "all",
             "--json", "number,title,body"],
            capture_output=True, text=True,
        )
        if result.returncode != 0:
            print(f"ERROR: {result.stderr.strip()}")
            continue

        issues = json.loads(result.stdout)
        for issue in issues:
            writer.writerow([issue["number"], issue["title"], issue["body"]])
        print(f"{len(issues)} issues")
        total += len(issues)

print(f"\nWrote {OUT}: {total} rows")
