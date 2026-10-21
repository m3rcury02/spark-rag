"""Databricks job task: decide which tag the pipeline job runs on, and hand it to the other tasks (M7, D-016).

The job's `tag` parameter is either a tag (`v4.0.1`) or `latest`: the newest plain release (vX.Y.Z) in the
source repo's ref advertisement (docs_rag.fetch.release_tags; no previews or release candidates). The result
is set as this task's value `tag`, which the bronze, silver and change-detection tasks read as
`{{tasks.resolve_tag.values.tag}}`, so they all run on one tag even if a release lands mid-run. A tag older
than the chain head is not refused here: change_detect refuses it unless --allow-backwards is given.
"""

import argparse
from typing import Any

from docs_rag.config import Config, validate_tag
from docs_rag.fetch import download, latest_release, refs_url, release_tags
from docs_rag.jobenv import base_parser, check_environment, run_with_summary


def resolve(args: argparse.Namespace, summary: dict[str, Any]) -> None:
    summary["environment"] = check_environment(args.expect_env, args.expect_version)
    config = Config(catalog=args.catalog, schema=args.schema)
    if args.tag == "latest":
        tags = release_tags(download(refs_url(config.source_repo)).decode("utf-8", "replace"))
        tag = latest_release(tags)
        summary["releases_seen"] = len(tags)
    else:
        tag = validate_tag(args.tag)
    summary.update({"requested": args.tag, "tag": tag})
    from databricks.sdk.runtime import dbutils  # only on Databricks

    dbutils.jobs.taskValues.set(key="tag", value=tag)


def main() -> None:
    parser = base_parser()
    parser.add_argument("--tag", required=True, help="a release tag, or 'latest'")
    run_with_summary(resolve, parser)


if __name__ == "__main__":
    main()
