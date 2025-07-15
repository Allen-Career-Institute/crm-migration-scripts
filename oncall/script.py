import csv
from collections import Counter
import os
import re

def parse_and_group_issues(file_path, output_file):
    if not os.path.exists(file_path):
        print(f"File not found: {file_path}")
        return

    titles = []
    with open(file_path, 'r') as file:
        reader = csv.DictReader(file)
        for row in reader:
            # Extract and clean the "Title" column
            title = re.sub(r"^\[.+\]", "", row["Title"].strip())  # Remove metadata
            titles.append(title.strip())

    grouped_counts = Counter(titles)

    # Write the grouped results to a CSV file
    with open(output_file, 'w', newline='') as csvfile:
        writer = csv.writer(csvfile)
        writer.writerow(["Title", "Count"])  # Write header
        for title, count in grouped_counts.items():
            writer.writerow([title, count])

    print(f"Grouped results have been written to {output_file}")

if __name__ == "__main__":
    issues_file = "incident_activity_export.csv"
    output_file = "grouped_issues.csv"
    parse_and_group_issues(issues_file, output_file)