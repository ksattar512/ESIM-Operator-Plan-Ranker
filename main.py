"""
Developer Signature: Kashif Sattar — ESIM Operator Plan Ranker
Purpose: Application entry point for the ESIM operator package ranking worker.
Public Repo Note: Runtime logs and credentials are intentionally excluded from source control.
"""

import logging
import time
from pathlib import Path

from controller.controller import MainController

LOG_DIR = Path("logs")
LOG_DIR.mkdir(exist_ok=True)

logging.basicConfig(
    filename=LOG_DIR / "process.log",
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
)


def main():
    """Run one ranking cycle by delegating orchestration to the main controller."""
    print("Main Worker Proc!")
    main_controller = MainController()
    return main_controller.init_process()


def write_log(message, log_type="process"):
    """Write a process or error message to the configured application log file."""
    level = logging.ERROR if log_type == "error" else logging.INFO
    logging.log(level, message)


if __name__ == "__main__":
    """Continuously run the ranking worker until an unrecoverable exception occurs."""
    while True:
        try:
            print("=======================================> Main Worker Proc Started!")
            write_log("Main thread started", "process")
            main()
            write_log("Main thread completed", "process")
            print("=======================================> Main Worker Proc Stopped!")
            time.sleep(5)
        except Exception as exc:
            print(f"An error occurred: {exc}")
            write_log(f"An error occurred: {exc}", "error")
            break
