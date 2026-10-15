"""Content hashing. Hashes are of raw bytes, so they do not depend on how text is decoded or normalised."""

import hashlib


def sha256_hex(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()
