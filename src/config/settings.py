from pathlib import Path

# ======================================================
# RouteIntel Project Configuration
# ======================================================

PROJECT_ROOT = Path(__file__).resolve().parents[2]

# Data Directories
DATA_DIR = PROJECT_ROOT / "data"

RAW_DATA_DIR = DATA_DIR / "raw"
EXTRACTED_DATA_DIR = DATA_DIR / "extracted"
INTERIM_DATA_DIR = DATA_DIR / "interim"
PROCESSED_DATA_DIR = DATA_DIR / "processed"

# GTFS
GTFS_RAW_DIR = RAW_DATA_DIR / "gtfs"
GTFS_EXTRACTED_DIR = EXTRACTED_DATA_DIR / "gtfs"

# Disruptions
DISRUPTION_RAW_DIR = RAW_DATA_DIR / "disruptions"
DISRUPTION_EXTRACTED_DIR = EXTRACTED_DATA_DIR / "disruptions"

# Output
OUTPUT_DIR = PROJECT_ROOT / "outputs"

# Models
MODELS_DIR = PROJECT_ROOT / "models"

# Database
DATABASE_DIR = PROJECT_ROOT / "database"

APP_NAME = "RouteIntel"
SPARK_APP_NAME = "RouteIntel"