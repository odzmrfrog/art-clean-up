import subprocess
import argparse
import json
import os
import sys
import fnmatch
import uuid
import logging
from datetime import datetime, timezone
from concurrent.futures import ThreadPoolExecutor, as_completed  # [ADDED FOR PARALLELISM]

def setup_logger():
    log_filename = f"clean_old_artifacts_{datetime.now(timezone.utc).strftime('%Y%m%d_%H%M%S')}.log"
    logger = logging.getLogger("clean_old_artifacts")
    logger.setLevel(logging.DEBUG)

    formatter = logging.Formatter('%(asctime)s [%(levelname)s] %(message)s', '%Y-%m-%d %H:%M:%S')

    ch = logging.StreamHandler(sys.stdout)
    ch.setLevel(logging.INFO)
    ch.setFormatter(formatter)

    fh = logging.FileHandler(log_filename)
    fh.setLevel(logging.DEBUG)
    fh.setFormatter(formatter)

    logger.addHandler(ch)
    logger.addHandler(fh)

    return logger

def jfrog_cli_configure(server_id, url, token, logger):
    logger.info(f"Configuring JFrog CLI with server ID '{server_id}'...")
    try:
        result = subprocess.run(
            ["jf", "config", "show"],
            capture_output=True, text=True, check=True
        )
        if server_id in result.stdout:
            logger.info(f"Server '{server_id}' already configured, skipping add.")
            return
    except subprocess.CalledProcessError:
        pass

    cmd = [
        "jf", "config", "add", server_id,
        "--url", url,
        "--access-token", token,
        "--interactive=false"
    ]

    try:
        subprocess.run(cmd, check=True)
        logger.info(f"Successfully added JFrog CLI server '{server_id}'.")
    except subprocess.CalledProcessError as e:
        logger.error(f"Failed to configure JFrog CLI: {e}")
        sys.exit(1)

    try:
        subprocess.run(["jf", "config", "use", server_id], check=True)
        logger.info(f"Using '{server_id}' as default server.")
    except subprocess.CalledProcessError as e:
        logger.error(f"Failed to set default server: {e}")
        sys.exit(1)

def load_exclusion_patterns(json_path, logger):
    if not os.path.exists(json_path):
        logger.error(f"Exclusion file not found: {json_path}")
        sys.exit(1)
    with open(json_path, "r") as f:
        exclusions = json.load(f)
    logger.info(f"Loaded {len(exclusions.get('exclude', []))} exclusion patterns from {json_path}")
    return exclusions.get("exclude", [])

def is_excluded(full_path, exclusion_patterns):
    for pattern in exclusion_patterns:
        if fnmatch.fnmatch(full_path, pattern):
            return True
    return False

def get_old_artifacts(spec_path, spec_timeframe, logger):
    if not os.path.exists(spec_path):
        logger.error(f"AQL spec file not found: {spec_path}")
        sys.exit(1)

    command = ["jf", "rt", "search", "--spec", spec_path, "--spec-vars", f"timeframe={spec_timeframe}"]
    try:
        result = subprocess.run(command, capture_output=True, text=True, check=True)
        return result.stdout
    except subprocess.CalledProcessError as e:
        logger.error("Search using spec file failed:")
        logger.error(e.stderr.strip())
        sys.exit(1)

def parse_artifacts(search_output, logger):
    try:
        data = json.loads(search_output)
        if isinstance(data, list):
            return data
        elif isinstance(data, dict) and "results" in data:
            return data["results"]
        else:
            logger.warning("Unexpected data format in search output.")
            return []
    except json.JSONDecodeError:
        logger.error("Failed to parse search response.")
        return []

def build_delete_command(path, dry_run):
    cmd = ["jf", "rt", "del", path, "--quiet"]
    if dry_run:
        cmd.append("--dry-run")
    return cmd

def execute_delete(cmd, logger):
    try:
        subprocess.run(cmd, check=True)
        if "--dry-run" in cmd:
            logger.info(f"[DRYRUN-COMPLETE] {' '.join(cmd)}")
        else:
            logger.info(f"[DELETED] {' '.join(cmd)}")
    except subprocess.CalledProcessError as e:
        logger.error(f"[ERROR] Delete failed: {' '.join(cmd)} - {e}")

def main():
    logger = setup_logger()

    parser = argparse.ArgumentParser(description="Delete old artifacts from all JFrog repositories.")
    parser.add_argument("--artifactory-url", required=True, help="Artifactory base URL")
    parser.add_argument("--access-token", required=True, help="JFrog access token")
    parser.add_argument("--older-than", required=True,
                        help="Retention window (e.g. 90d, 3mo, 1y) — used only for logging")
    parser.add_argument("--exclusions-file", required=True,
                        help="Path to exclusions JSON file (with glob-style patterns)")
    parser.add_argument("--aql-spec", required=True,
                        help="Path to AQL spec file (e.g. aql-filespec.json)")
    parser.add_argument("--dry-run", action="store_true",
                        help="List deletions without executing them")
    parser.add_argument("--threads", type=int, default=4,  # [ADDED FOR PARALLELISM]
                        help="Number of parallel threads to use for deletion")

    args = parser.parse_args()

    server_id = f"cli-config-{uuid.uuid4().hex[:8]}"
    jfrog_cli_configure(server_id, args.artifactory_url, args.access_token, logger)

    exclusion_patterns = load_exclusion_patterns(args.exclusions_file, logger)

    logger.info(f"Searching for artifacts created before '{args.older_than}' using filespec '{args.aql_spec}'...")

    raw_output = get_old_artifacts(args.aql_spec, args.older_than, logger)
    artifacts = parse_artifacts(raw_output, logger)

    if not artifacts:
        logger.info("No matching artifacts found.")
        return

    logger.info(f"{len(artifacts)} artifact(s) found. Checking exclusions...")

    delete_commands = []
    for item in artifacts:
        path = item.get("path", "")
        if is_excluded(f"{path}", exclusion_patterns):
            logger.info(f"[SKIP] Excluded by pattern: {path}")
            continue

        cmd = build_delete_command(path, args.dry_run)
        log_label = "[DRYRUN]" if args.dry_run else "[DELETE]"
        logger.debug(f"{log_label} {' '.join(cmd)}")
        delete_commands.append(cmd)

    logger.info(f"Executing {len(delete_commands)} deletions using {args.threads} thread(s)...")

    # [ADDED FOR PARALLELISM]
    with ThreadPoolExecutor(max_workers=args.threads) as executor:
        futures = [executor.submit(execute_delete, cmd, logger) for cmd in delete_commands]
        for future in as_completed(futures):
            pass  # Optional: could check future.result()

if __name__ == "__main__":
    main()
