"""Import notebooks/00_workspace_smoke_test.py into the workspace and submit it as a one-off serverless run.

Auth comes from DATABRICKS_HOST / DATABRICKS_TOKEN (Databricks unified auth). Run with:

    uv run --with databricks-sdk==0.152.0 python scripts/m0_submit_smoke_test.py

Prints the run id; collect the results afterwards with scripts/m0_collect_evidence.py.
"""

import base64
import pathlib

from databricks.sdk import WorkspaceClient
from databricks.sdk.service import jobs, workspace

REPO = pathlib.Path(__file__).resolve().parents[1]


def main() -> None:
    w = WorkspaceClient()
    folder = f"/Users/{w.current_user.me().user_name}/docs_rag"
    nb_path = f"{folder}/00_workspace_smoke_test"
    w.workspace.mkdirs(folder)
    w.workspace.import_(
        path=nb_path,
        content=base64.b64encode((REPO / "notebooks/00_workspace_smoke_test.py").read_bytes()).decode(),
        format=workspace.ImportFormat.SOURCE,
        language=workspace.Language.PYTHON,
        overwrite=True,
    )
    # No compute spec on the task, so it runs on serverless jobs compute with the workspace's
    # default environment version (measured: client.2.5 / Python 3.11, see runs/m0_smoke_test.json).
    waiter = w.jobs.submit(
        run_name="docs_rag_m0_smoke_test",
        timeout_seconds=3 * 3600,
        tasks=[
            jobs.SubmitTask(
                task_key="smoke_test",
                notebook_task=jobs.NotebookTask(notebook_path=nb_path, source=jobs.Source.WORKSPACE),
            )
        ],
    )
    print(f"submitted run_id={waiter.run_id}")


if __name__ == "__main__":
    main()
