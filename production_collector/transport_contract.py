"""Fixed, bounded IPC contract. No input URL, proxy, credentials or retry."""
import json
import struct

URL = "https://www.ecb.europa.eu/stats/eurofxref/eurofxref-daily.xml"
LIMIT = 64 * 1024
META_LIMIT = 16384
HEADER_NAMES = frozenset(("content-type", "content-length", "content-encoding", "date",
                          "last-modified", "etag", "age", "cache-control"))
FIXTURES = frozenset(("ok", "revised", "denied", "rate_limited", "server_error", "redirect", "empty", "oversize",
                      "malformed", "overflow", "hang", "exit_error", "compressed",
                      "bad_length", "invalid_html", "secret_header"))


def encode(meta, body):
    raw = json.dumps(meta, sort_keys=True, separators=(",", ":"), allow_nan=False).encode()
    if len(raw) > META_LIMIT:
        raise ValueError("metadata_cap")
    return struct.pack("!I", len(raw)) + raw + body


def decode(raw):
    if not 4 <= len(raw) <= 4 + META_LIMIT + LIMIT:
        raise ValueError("ipc_bound")
    n = struct.unpack("!I", raw[:4])[0]
    if not 0 < n <= META_LIMIT or len(raw) < n + 4:
        raise ValueError("ipc_frame")
    def no_duplicates(pairs):
        out = {}
        for k, v in pairs:
            if k in out:
                raise ValueError("duplicate_json_key")
            out[k] = v
        return out
    meta = json.loads(raw[4:4+n], object_pairs_hook=no_duplicates)
    if type(meta) is not dict or set(meta) != {"url", "status", "headers", "body_complete", "tls_verified"}:
        raise ValueError("ipc_fields")
    if meta["url"] != URL or type(meta["status"]) is not int or not 100 <= meta["status"] <= 599:
        raise ValueError("http_status_or_url")
    if type(meta["body_complete"]) is not bool or type(meta["tls_verified"]) is not bool:
        raise ValueError("ipc_boolean")
    headers = meta["headers"]
    if type(headers) is not dict or not len(headers) <= len(HEADER_NAMES):
        raise ValueError("header_fields")
    for k, v in headers.items():
        if k not in HEADER_NAMES or type(v) is not str or len(v) > 1024 or any(ord(c) < 32 or ord(c) == 127 for c in v):
            raise ValueError("header_contract")
    body = bytes(raw[4+n:])
    if len(body) > LIMIT:
        raise ValueError("body_bound")
    return meta, body
