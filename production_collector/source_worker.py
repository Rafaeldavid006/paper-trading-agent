"""Owned Linux worker. Live mode requires the exact compiled finite release.

PDEATHSIG prevents an already-running fixed worker surviving parent death. This
is not a hostile-code sandbox. Transport is single exact public HTTPS GET only.
"""
import ctypes
import os
from pathlib import Path
import resource
import signal
import sys
import time

# -I removes the script directory; add only our trusted sibling code explicitly.
sys.path.insert(0, str(Path(__file__).resolve().parent))
from transport_contract import URL, LIMIT, HEADER_NAMES, FIXTURES, encode

LIVE_APPROVED = False  # In-memory fake-opener test gate only; never enables CLI HTTP.


def bind_parent(expected_parent):
    if sys.platform != "linux":
        raise ValueError("linux_parent_death_contract_required")
    libc = ctypes.CDLL(None, use_errno=True)
    if libc.prctl(1, signal.SIGKILL, 0, 0, 0) != 0:
        raise OSError("parent_death_binding_failed")
    if os.getppid() != expected_parent:
        raise ValueError("parent_changed_before_binding")
    resource.setrlimit(resource.RLIMIT_CORE, (0, 0))
    resource.setrlimit(resource.RLIMIT_CPU, (5, 5))
    resource.setrlimit(resource.RLIMIT_AS, (256 * 1024 * 1024, 256 * 1024 * 1024))
    resource.setrlimit(resource.RLIMIT_NOFILE, (32, 32))


def request_once(*, _opener=None):
    # Injected opener exists solely for in-memory protocol tests. CLI/default
    # transport always validates the independent compiled production release.
    if _opener is not None:
        if LIVE_APPROVED is not True:
            raise ValueError("live_source_gate_closed")
    else:
        from production_release import APPROVED_RELEASE, REQUIRED_PYTHON, require_collection_release, approved_leases
        require_collection_release(APPROVED_RELEASE)
        if sys.version_info[:3] != REQUIRED_PYTHON:
            raise ValueError("exact_python_version_required")
        now = time.time()
        if len([p for p in approved_leases(APPROVED_RELEASE) if p["start"] <= now < p["end"]]) != 1:
            raise ValueError("live_lease_not_current_or_overlapping")
    from urllib.request import Request, build_opener, HTTPRedirectHandler, ProxyHandler, HTTPSHandler
    from urllib.error import HTTPError
    import ssl
    class NoRedirect(HTTPRedirectHandler):
        def redirect_request(self, *args, **kwargs):
            return None
    opener = _opener or build_opener(ProxyHandler({}), NoRedirect(), HTTPSHandler(context=ssl.create_default_context()))
    req = Request(URL, headers={"User-Agent": "PublicSourceResearch/1.0",
                                "Accept": "application/xml", "Accept-Encoding": "identity"})
    try:
        response = opener.open(req, timeout=8)
    except HTTPError as exc:
        response = exc
    with response as r:
        headers = {}
        for name in HEADER_NAMES:
            values = r.headers.get_all(name, [])
            if len(values) > 1:
                raise ValueError("duplicate_response_header")
            if values:
                value = values[0]
                if len(value) > 1024 or any(ord(c) < 32 or ord(c) == 127 for c in value):
                    raise ValueError("response_header_bound")
                headers[name] = value
        if r.geturl() != URL:
            raise ValueError("redirect_refused")
        status = r.code
        body = b""
        complete = False
        if status == 200:
            if headers.get("content-encoding", "identity").lower() != "identity":
                raise ValueError("content_encoding_refused")
            if "content-length" in headers:
                s = headers["content-length"]
                if not s.isascii() or not s.isdecimal() or len(s) > 8 or int(s) > LIMIT:
                    raise ValueError("declared_length_bound")
            body = r.read(LIMIT + 1)
            if len(body) > LIMIT:
                raise ValueError("body_bound")
            if "content-length" in headers and int(headers["content-length"]) != len(body):
                raise ValueError("incomplete_body")
            # read(n) without a length header reads until n bytes or EOF; n is cap+1.
            complete = True
        return {"url": r.geturl(), "status": status, "headers": headers,
                "body_complete": complete, "tls_verified": True}, body


def synthetic(mode):
    if mode not in FIXTURES:
        raise ValueError("fixed_fixture_required")
    # Offline fixtures must never acquire sockets, even if later changed by mistake.
    def guard(event, args):
        if event.startswith(("socket.", "subprocess.", "os.system", "os.spawn", "os.fork")):
            raise RuntimeError("offline_external_operation_refused")
    sys.addaudithook(guard)
    body = (b'<gesmes:Envelope xmlns:gesmes="http://www.gesmes.org/xml/2002-08-01" xmlns="http://www.ecb.int/vocabulary/2002-08-01/eurofxref"><Cube><Cube time="2026-10-28"><Cube currency="USD" rate="1.10"/></Cube></Cube></gesmes:Envelope>')
    if mode == "revised":
        body = body.replace(b'rate="1.10"', b'rate="1.11"')
    meta = {"url": URL, "status": 200, "headers": {"content-type": "application/xml; charset=utf-8"},
            "body_complete": True, "tls_verified": True}
    if mode == "hang":
        time.sleep(60)
    if mode == "exit_error":
        raise ValueError("synthetic_worker_error")
    if mode == "malformed":
        return b"not a frame"
    if mode == "overflow":
        return b"x" * (LIMIT + 16384 + 5)
    if mode == "denied":
        meta.update(status=403, body_complete=False); body = b""
    if mode == "rate_limited":
        meta.update(status=429, body_complete=False); body = b""
    if mode == "server_error":
        meta.update(status=503, body_complete=False); body = b""
    if mode == "redirect":
        meta.update(status=302, body_complete=False); body = b""
    if mode == "empty":
        body = b""
    if mode == "oversize":
        body = b"x" * (LIMIT + 1)
    if mode == "compressed":
        meta["headers"]["content-encoding"] = "gzip"
    if mode == "invalid_html":
        body = b"not html"
    if mode == "secret_header":
        meta["headers"]["authorization"] = "secret-sentinel-must-not-persist"
    meta["headers"]["content-length"] = "99" if mode == "bad_length" else str(len(body))
    return encode(meta, body)


def main():
    if len(sys.argv) != 3 or sys.argv[1] not in FIXTURES | {"live"}:
        raise ValueError("fixed_worker_arguments")
    bind_parent(int(sys.argv[2]))
    mode = sys.argv[1]
    output = encode(*request_once()) if mode == "live" else synthetic(mode)
    sys.stdout.buffer.write(output)
    sys.stdout.buffer.flush()


if __name__ == "__main__":
    try:
        main()
    except BaseException:
        # No raw URL/header/OS exception text in stderr or journal.
        raise SystemExit(70)
