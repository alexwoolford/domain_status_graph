"""
CLI command entry points for public_company_graph.

These functions are registered as console scripts in pyproject.toml.
Each function delegates to the corresponding script in scripts/.
"""

import subprocess
import sys
from pathlib import Path

# Allowlist of valid script names to prevent command injection
_ALLOWED_SCRIPTS = {
    "bootstrap_graph",
    "compute_gds_features",
    "health_check",
    "run_all_pipelines",
    "enrich_company_properties",
    "compute_company_similarity",
    "validate_ranking_quality",
    "validate_famous_pairs",
    "compute_company_similarity_via_domains",
    "optimize_similarity_weights",
}


def _run_script(script_name: str):
    """
    Helper to run a script with arguments.

    Args:
        script_name: Name of script file (without .py extension)

    Raises:
        ValueError: If script_name is not in the allowlist
        FileNotFoundError: If script file doesn't exist
    """
    # Validate script name against allowlist to prevent command injection
    if script_name not in _ALLOWED_SCRIPTS:
        raise ValueError(
            f"Invalid script name: {script_name}. Must be one of: {sorted(_ALLOWED_SCRIPTS)}"
        )

    script = Path(__file__).parent.parent.parent / "scripts" / f"{script_name}.py"

    # Verify script file exists
    if not script.exists():
        raise FileNotFoundError(f"Script not found: {script}")

    # Sanitize arguments: reject any that contain shell metacharacters
    # This prevents command injection even though we're using a list (not shell=True)
    safe_args = [sys.executable, str(script)]
    dangerous_chars = [";", "&", "|", "`", "$", "(", ")", "<", ">", "\n", "\r"]
    for arg in sys.argv[1:]:
        # Check for dangerous characters that could be used for injection
        if any(char in arg for char in dangerous_chars):
            raise ValueError(f"Invalid argument contains dangerous characters: {repr(arg)}")
        safe_args.append(arg)

    # Safe: using list (not shell=True) and arguments are validated
    subprocess.run(safe_args, check=False)


def run_bootstrap():
    """Entry point for bootstrap-graph command."""
    _run_script("bootstrap_graph")


def run_gds_features():
    """Entry point for compute-gds-features command."""
    _run_script("compute_gds_features")


def run_health_check():
    """Entry point for health-check command."""
    _run_script("health_check")


def run_all_pipelines():
    """Entry point for run-all-pipelines command."""
    _run_script("run_all_pipelines")


def run_enrich_company_properties():
    """Entry point for enrich-company-properties command."""
    _run_script("enrich_company_properties")


def run_compute_company_similarity():
    """Entry point for compute-company-similarity command."""
    _run_script("compute_company_similarity")


def run_validate_ranking_quality():
    """Entry point for validate-ranking-quality command."""
    _run_script("validate_ranking_quality")


def run_validate_famous_pairs():
    """Entry point for validate-famous-pairs command."""
    _run_script("validate_famous_pairs")


def run_compute_company_similarity_via_domains():
    """Entry point for compute-company-similarity-via-domains command."""
    _run_script("compute_company_similarity_via_domains")


def run_optimize_similarity_weights():
    """Entry point for optimize-similarity-weights command."""
    _run_script("optimize_similarity_weights")


def run_cache():
    """Entry point for cache command."""
    import argparse

    from public_company_graph.cache import get_cache

    parser = argparse.ArgumentParser(description="Manage unified cache")
    parser.add_argument("command", choices=["stats", "list", "clear"])
    parser.add_argument("--namespace", "-n", help="Filter by namespace")
    parser.add_argument("--limit", type=int, default=20, help="Limit for list")
    parser.add_argument("--yes", "-y", action="store_true", help="Skip confirmation")
    args = parser.parse_args()

    cache = get_cache()

    if args.command == "stats":
        stats = cache.stats()
        print(f"Cache: {stats['cache_dir']}")
        print(f"  Total entries: {stats['total']}")
        print(f"  Size: {stats['size_mb']} MB")
        print("  By namespace:")
        for ns, count in sorted(stats["by_namespace"].items()):
            print(f"    {ns}: {count}")

    elif args.command == "list":
        keys = cache.keys(namespace=args.namespace, limit=args.limit)
        ns_label = args.namespace or "all"
        print(f"Keys ({ns_label}, limit {args.limit}):")
        for key in keys:
            print(f"  {key}")

    elif args.command == "clear":
        if args.namespace:
            if not args.yes:
                confirm = input(f"Clear all {args.namespace} entries? [y/N] ")
                if confirm.lower() != "y":
                    print("Aborted")
                    return
            count = cache.clear_namespace(args.namespace)
            print(f"Cleared {count} entries from {args.namespace}")
        else:
            print("Specify --namespace to clear, or use 'rm -rf data/cache'")
