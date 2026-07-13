"""
To call from command line
Written by Ian David Elder for the Canadian Open Energy Model (CANOE)
iandavidelder@gmail.com
2025/07/07
"""

import argparse

import all_sectors
import db
from config import CANOECEFConfig

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="canoe-cef: CEF -> CANOE demand aggregation")
    parser.add_argument(
        "--build-test-db",
        action="store_true",
        help=(
            "Build a small standalone SQLite database for local dev/Temoa test "
            "runs, instead of running the normal aggregation pipeline against "
            "the shared canoe-base database."
        ),
    )
    args = parser.parse_args()

    cfg = CANOECEFConfig.load()

    if args.build_test_db:
        db.build_test_database(cfg, f"{cfg.input_files_dir}/dsd_electricity.csv")
    else:
        all_sectors.build(cfg)
