from collections import Counter

from src.exporters import write_links_to_csv
from src.metadata import enrich_links
from src.parser import extract_links_from_export


def main() -> None:
    input_file = "data/raw/whatsapp_export.txt"
    extracted_file = "data/processed/extracted_links.csv"
    enriched_file = "data/processed/enriched_links.csv"

    records = extract_links_from_export(input_file)
    write_links_to_csv(records, extracted_file)
    enrich_links(extracted_file, enriched_file)

    counts = Counter(r.platform for r in records)

    print(f"Extracted {len(records)} links")
    for platform, count in counts.items():
        print(f"{platform}: {count}")
    print(f"Saved extracted links to {extracted_file}")
    print(f"Saved enriched links to {enriched_file}")


if __name__ == "__main__":
    main()
