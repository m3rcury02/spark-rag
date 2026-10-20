"""Run the docs_rag code of an earlier commit, for verifying runs that today's code can no longer reproduce (a
cleaner or chunker version bump; D-012's re-baseline path).

Each commit's tree is checked out once into a temporary git worktree, and a child Python process imports
docs_rag from it (PYTHONPATH comes before the installed package; the child reports the module path it
imported, which is checked). So an evidence script can compare a logged run with the very code that produced
it, without importing two versions of docs_rag into one process. The worktrees are removed at exit.
"""

import atexit
import functools
import json
import os
import shutil
import subprocess
import sys
import tempfile
from typing import Any

from runguard import REPO

_CHILD = r"""
import json, sys
import docs_rag
from docs_rag.chunk import ChunkConfig, chunk_document
from docs_rag.config import Config
job = json.load(sys.stdin)
out = {"module": docs_rag.__file__, "published_config_id": Config().chunk.config_id}
if job["files"] is not None:
    config = ChunkConfig(job["max_tokens"], job["overlap_tokens"])
    out["config_id"] = config.config_id
    out["chunks"] = [
        {"path": p, "chunk_id": c.chunk_id, "chunk_index": c.chunk_index,
         "section_path": list(c.section_path),
         "body": c.body, "embed_text": c.embed_text, "n_tokens": c.n_tokens}
        for p, text in sorted(job["files"].items()) for c in chunk_document(p, text, config).chunks
    ]
json.dump(out, sys.stdout)
"""
_TREES: list[str] = []


def _git(*args: str) -> str:
    return subprocess.run(["git", "-C", str(REPO), *args], capture_output=True, text=True, check=True).stdout


@functools.cache
def _worktree(commit: str) -> str:
    path = tempfile.mkdtemp(prefix=f"docs_rag_{commit[:9]}_")
    _git("worktree", "add", "--detach", "--force", path, commit)
    _TREES.append(path)
    return path


@atexit.register
def _remove_worktrees() -> None:
    for path in _TREES:
        subprocess.run(["git", "-C", str(REPO), "worktree", "remove", "--force", path], capture_output=True)
        shutil.rmtree(path, ignore_errors=True)


def commit_of(code_version: str) -> str:
    """The full commit id in a hatch-vcs version such as '0.0.1.dev91+g6e9429936'."""
    short = code_version.split("+g", 1)[1].split(".", 1)[0]
    return _git("rev-parse", "--verify", f"{short}^{{commit}}").strip()


def _run(commit: str, files: dict[str, str] | None, max_tokens: int = 0, overlap_tokens: int = 0) -> Any:
    tree = _worktree(commit)
    env = {**os.environ, "PYTHONPATH": os.path.join(tree, "src")}
    job = json.dumps({"files": files, "max_tokens": max_tokens, "overlap_tokens": overlap_tokens})
    done = subprocess.run(
        [sys.executable, "-c", _CHILD], input=job, env=env, capture_output=True, text=True, check=True
    )
    result = json.loads(done.stdout)
    if not os.path.realpath(result["module"]).startswith(os.path.realpath(tree)):
        raise RuntimeError(f"the child imported docs_rag from {result['module']}, not from {tree}")
    return result


@functools.cache
def published_config_id(commit: str) -> str:
    """Config().chunk.config_id as the code of `commit` computes it: the config its jobs published."""
    return str(_run(commit, None)["published_config_id"])


def chunk_at(
    commit: str, files: dict[str, str], max_tokens: int, overlap_tokens: int
) -> tuple[str, list[dict[str, Any]]]:
    """(config_id, chunks) as the code of `commit` chunks `files`; each chunk a dict of path, chunk_id,
    chunk_index, section_path, body, embed_text and n_tokens."""
    result = _run(commit, files, max_tokens, overlap_tokens)
    return str(result["config_id"]), list(result["chunks"])
