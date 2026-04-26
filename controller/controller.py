"""
Developer Signature: Kashif Sattar — ESIM Operator Plan Ranker
Purpose: Controller layer that starts the business processing workflow.
Public Repo Note: Keep orchestration here and detailed ranking/data-access logic in service layers.
"""

from bpl.bplinit import BPLInit


class MainController:
    """Coordinates the main ESIM package ranking workflow."""

    def __init__(self):
        """Initialize the controller instance."""
        pass

    def parse_record(self, record):
        """Normalize a raw database record into the core fields used by the app."""
        return {
            "id": record["id"],
            "PackageName": record["package_name"],
        }

    def init_process(self):
        """Start the BPL ranking process and return the generated records/result."""
        bplinit = BPLInit()
        return bplinit.get_records_function_ssh_WDays()
