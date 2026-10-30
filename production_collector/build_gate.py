"""Future existing-pipeline exact-runtime gate; no deployment or persistent mount IO.

Run after the existing dependency install: python production_collector/build_gate.py
A failure must fail the build rather than start an unqualified replacement service.
"""
import json
from pathlib import Path
import shutil
import subprocess
import sys
import tempfile

ROOT = Path(__file__).resolve().parent
sys.path.insert(0, str(ROOT))
import production_release as policy


def main():
    if sys.version_info[:3] != policy.REQUIRED_PYTHON:
        print(json.dumps({"state":"build_refused","reason":"exact_python_version_required",
                          "required":"3.14.8","actual":".".join(map(str,sys.version_info[:3])),
                          "source_requests":0,"order_permission":"none"}))
        return 1
    # Check necessary runtime modules locally; never acquire an interpreter,
    # contact a provider, or inspect the attached production disk from build.
    import ctypes, fcntl, resource, sqlite3, ssl
    if policy.APPROVED_RELEASE is not None:
        policy.require_release(policy.APPROVED_RELEASE)
    child_path = [None]
    def guard(event,args):
        if event.startswith("socket.") or event in ("os.system","os.posix_spawn","os.fork"):
            raise RuntimeError("offline_build_external_operation_refused")
        if event == "subprocess.Popen":
            executable,argv,cwd,env=args
            if not (executable==sys.executable and argv==[sys.executable,"-I","-S",child_path[0]] and env=={}):
                raise RuntimeError("fixed_offline_build_child_required")
    sys.addaudithook(guard)
    with tempfile.TemporaryDirectory(prefix="collector-offline-build-") as temp:
        snapshot=Path(temp)/"collector"
        shutil.copytree(ROOT,snapshot,ignore=shutil.ignore_patterns("__pycache__","*.pyc","approved_release.json","review_evidence"))
        # No live grant is copied into the offline test child. Its runner rejects
        # parent sockets and live worker modes; synthetic workers reject sockets.
        child_path[0]=str(snapshot/"run_checks.py")
        result=subprocess.run([sys.executable,"-I","-S",child_path[0]],env={},cwd=snapshot,
                              close_fds=True,stdin=subprocess.DEVNULL,timeout=180)
    print(json.dumps({"state":"passed" if result.returncode==0 else "tests_failed",
                      "mode":"offline_exact_runtime_build_gate","python":"3.14.8",
                      "source_requests":0,"persistent_mount_access":False,
                      "production_data_activated":False,"order_permission":"none"}))
    return result.returncode


if __name__=="__main__":
    raise SystemExit(main())
