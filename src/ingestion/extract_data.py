from pathlib import Path
import zipfile

from src.config.settings import (
    GTFS_RAW_DIR,
    GTFS_EXTRACTED_DIR,
    DISRUPTION_RAW_DIR,
    DISRUPTION_EXTRACTED_DIR,
)


def extract_zip_files(source_dir: Path, destination_dir: Path) -> None:
    """
    Extract all ZIP files from source_dir into destination_dir.
    """

    destination_dir.mkdir(parents=True, exist_ok=True)

    zip_files = list(source_dir.glob("*.zip"))

    if not zip_files:
        print(f"No ZIP files found in {source_dir}")
        return

    for zip_file in zip_files:

        print(f"\nExtracting: {zip_file.name}")

        with zipfile.ZipFile(zip_file, "r") as archive:
            archive.extractall(destination_dir)

        print("Completed")


def run_extraction():

    print("=" * 60)
    print("ROUTEINTEL DATA EXTRACTION")
    print("=" * 60)

    extract_zip_files(
        GTFS_RAW_DIR,
        GTFS_EXTRACTED_DIR
    )

    extract_zip_files(
        DISRUPTION_RAW_DIR,
        DISRUPTION_EXTRACTED_DIR
    )

    print("\nExtraction Finished")


if __name__ == "__main__":
    run_extraction()