from bs4 import BeautifulSoup
from collections import Counter
import os
import re

def parse_and_group_issues(file_path):
    if not os.path.exists(file_path):
        print(f"File not found: {file_path}")
        return

    with open(file_path, 'r') as file:
        content = file.read()

    soup = BeautifulSoup(content, 'html.parser')
    titles = [
        re.sub(r"^\[#\d+\]\s+\[.*?\]\s+", "", row.text.strip())  # Remove first two columns
        for row in soup.select('td.incident a')
    ]

    grouped_counts = Counter(titles)

    for title, count in grouped_counts.items():
        print(f"{title}\n{count}")

if __name__ == "__main__":
    issues_file = "issues.xml"
    parse_and_group_issues(issues_file)
