"""Redact account identifiers before anything is written to runs/ (the repo is public).

Databricks error messages and API results can name the owner's email, the workspace host and the workspace
id (for example "the identity used to run this job, <email>, lacks ..."). Every script that writes to runs/
passes its output through `Redactor.dump`, which also refuses to write if an identifier survived.
"""

import json
import re
from typing import Any

from databricks.sdk import WorkspaceClient


class Redactor:
    def __init__(self, w: WorkspaceClient) -> None:
        host = (w.config.host or "").removeprefix("https://").rstrip("/")
        user = w.current_user.me().user_name or ""
        self.pairs = [
            (user, "<owner>"),
            # Bundle dev mode names resources "[dev <user name without the domain>] ..." (M7)
            (user.split("@")[0] if "@" in user else "", "<owner-name>"),
            (host, "<workspace-host>"),
            (str(w.get_workspace_id()), "<workspace-id>"),
        ]

    def text(self, s: str) -> str:
        for old, new in self.pairs:
            if old:
                s = re.sub(re.escape(old), new, s, flags=re.IGNORECASE)
        return s

    def obj(self, o: Any) -> Any:
        if isinstance(o, str):
            return self.text(o)
        if isinstance(o, list):
            return [self.obj(x) for x in o]
        if isinstance(o, dict):
            return {self.text(str(k)): self.obj(v) for k, v in o.items()}
        return o

    def dump(self, data: Any) -> str:
        """Redacted JSON text, checked: raises rather than return text that still names the account."""
        text = json.dumps(self.obj(data), indent=2, default=str) + "\n"
        low = text.lower()
        for old, _ in self.pairs:
            if old and old.lower() in low:
                raise RuntimeError("an account identifier survived redaction; nothing written")
        return text
