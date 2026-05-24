from collections import Counter

from src.categorizer import categorize_links
from src.exporters import write_html_report, write_links_to_csv, write_review_spreadsheet
from src.metadata import enrich_links
from src.parser import extract_links_from_export


def main() -> None:
    input_file = "data/raw/whatsapp_export.txt"
    extracted_file = "data/processed/extracted_links.csv"
    enriched_file = "data/processed/enriched_links.csv"
    categorized_file = "data/processed/categorized_links.csv"
    report_file = "data/processed/report.html"
    review_file = "data/processed/review.xlsx"

    records = extract_links_from_export(input_file)
    write_links_to_csv(records, extracted_file)
    enrich_links(extracted_file, enriched_file)
    categorize_links(enriched_file, categorized_file)
    write_html_report(categorized_file, report_file)
    write_review_spreadsheet(categorized_file, review_file)

    counts = Counter(r.platform for r in records)

    print(f"Extracted {len(records)} links")
    for platform, count in counts.items():
        print(f"  {platform}: {count}")
    print(f"Saved extracted links to {extracted_file}")
    print(f"Saved enriched links to {enriched_file}")
    print(f"Saved categorized links to {categorized_file}")
    print(f"Saved HTML report to {report_file}")
    print(f"Saved review spreadsheet to {review_file}")


if __name__ == "__main__":
    main()
