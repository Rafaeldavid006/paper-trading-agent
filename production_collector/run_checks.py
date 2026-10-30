"""Standard-library verification; parent socket guard plus fixed synthetic workers.

This Python audit instrumentation is NOT an OS/network security sandbox and is
not inherited by supervisor subprocesses. Synthetic workers install their own
socket/process audit guard. The runner permits only synthetic worker modes; live grants default absent.
"""
from hashlib import sha256
import json
import gc
import warnings
from pathlib import Path
import sys
import unittest

ROOT = Path(__file__).resolve().parent
sys.path.insert(0, str(ROOT))
BLOCKED = []
SPAWNS = []
UNRAISABLE = []


def guard(event, args):
    if event.startswith("socket.") or event in ("os.system", "os.posix_spawn"):
        BLOCKED.append(event)
        raise RuntimeError("offline_external_operation_refused")
    if event == "subprocess.Popen":
        executable, argv, cwd, env = args
        if executable != sys.executable or env != {}:
            BLOCKED.append("unexpected_subprocess")
            raise RuntimeError("fixed_python_subprocess_required")
        from transport_contract import FIXTURES
        worker = (len(argv) == 6 and argv[1:3] == ["-I", "-S"]
                  and Path(argv[3]).name == "source_worker.py" and argv[4] in FIXTURES)
        # Independent signal/parent-death harnesses are local Python snippets.
        supervisor = len(argv) == 3 and argv[1] == "-c"
        if not worker and not supervisor:
            BLOCKED.append("unexpected_subprocess")
            raise RuntimeError("fixed_subprocess_contract")
        SPAWNS.append("fixed_synthetic_worker" if worker else "local_lifecycle_test_supervisor")


def main():
    warnings.simplefilter("error", ResourceWarning)
    sys.unraisablehook = lambda event: UNRAISABLE.append(type(event.exc_value).__name__)
    sys.addaudithook(guard)
    upstream = json.loads((ROOT / "UPSTREAM_HASHES.json").read_text())
    for row in upstream:
        if sha256((ROOT / row["bundled_path"]).read_bytes()).hexdigest() != row["sha256"]:
            raise SystemExit("upstream_copy_changed:" + row["bundled_path"])
    result = unittest.TextTestRunner(verbosity=2).run(unittest.defaultTestLoader.discover(str(ROOT / "tests")))
    gc.collect()  # Finalizer ResourceWarnings must fail rather than vanish as ignored errors.
    summary = {"tests_run": result.testsRun, "failures": len(result.failures), "errors": len(result.errors),
               "skipped": len(result.skipped), "upstream_hashes_verified": len(upstream),
               "parent_external_attempts_blocked": BLOCKED, "local_child_spawns": len(SPAWNS),
               "unraisable_errors": UNRAISABLE,
               "live_http_requests": 0, "synthetic_only": True, "active_collection": False,
               "os_sandbox_claimed": False, "order_permission": "none"}
    print(json.dumps(summary, indent=2))
    return 0 if result.wasSuccessful() and not BLOCKED and not UNRAISABLE else 1


if __name__ == "__main__":
    raise SystemExit(main())
