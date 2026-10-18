"""Traceability guard for every script that writes evidence to runs/: the code must be committed, so each file
under runs/ points at the commit that produced it. Fails closed: a git error stops the script too."""

import pathlib
import subprocess
import sys

REPO = pathlib.Path(__file__).resolve().parents[1]


def _git(*args: str) -> str:
    return subprocess.run(["git", "-C", str(REPO), *args], capture_output=True, text=True, check=True).stdout


def committed_head() -> str:
    """HEAD's commit id, after checking there are no uncommitted changes outside runs/ and no stray Python
    files (untracked or ignored) under scripts/, src/ or jobs/ that could be imported."""
    if _git("status", "--porcelain", "--", ".", ":(exclude)runs"):
        sys.exit("uncommitted code changes (runs/ excepted): commit first, so the result can be traced")
    stray = [
        line[3:]
        for line in _git("status", "--porcelain", "--ignored", "--", "scripts", "src", "jobs").splitlines()
        if line.endswith(".py")
    ]
    if stray:
        sys.exit(f"untracked or ignored Python files could be imported: {stray}")
    head = _git("rev-parse", "HEAD").strip()
    if not head:
        sys.exit("no commit id")
    return head
