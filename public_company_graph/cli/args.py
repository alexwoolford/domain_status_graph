"""
Argument parsing utilities for public_company_graph CLI.

Provides standard argument patterns used across scripts.
"""


def add_execute_argument(parser):
    """
    Add standard --execute argument to an ArgumentParser.

    Args:
        parser: argparse.ArgumentParser instance
    """
    parser.add_argument(
        "--execute",
        action="store_true",
        help="Actually execute the operation (default is dry-run)",
    )
