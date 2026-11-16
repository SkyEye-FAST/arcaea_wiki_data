"""Shared repository and regional output locations."""

from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
OUTPUT_ROOT = PROJECT_ROOT / "output"
INTL_OUTPUT = OUTPUT_ROOT / "intl"
CN_OUTPUT = OUTPUT_ROOT / "cn"
