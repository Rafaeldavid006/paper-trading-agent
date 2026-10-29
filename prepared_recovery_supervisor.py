"""One direct finite worker, never a scheduler, grant issuer or restart mechanism."""
from pathlib import Path
import os
import subprocess
import sys
import threading
import time
from finite_pilot_contract import require, NS
import prepared_successor_activation as admission_activation
import prepared_execution_activation as execution_activation
import prepared_successor_contract as admission_contract
import prepared_execution_contract as execution_contract


def selected():
    """Exact compiled authority only. No paths, grants or environment overrides."""
    a, e = admission_activation.APPROVED_ADMISSION, execution_activation.APPROVED_EXECUTION
    require(a is None or e is None, 'prepared_mixed_authority')
    if a is not None:
        admission_contract.approved(a)
        return a, admission_contract, 'prepared_successor_worker.py', '--admit-readonly'
    if e is not None:
        execution_contract.approved(e)
        return e, execution_contract, 'prepared_execution_worker.py', '--execute-once'
    return None


def consumed(contract):
    # lexists deliberately treats dangling links, partial files and sidecars as consumed.
    require(contract.JOURNAL_PATH == '/var/data/' + contract.JOURNAL_NAME,
            'prepared_fixed_journal_required')
    return any(os.path.lexists(contract.JOURNAL_PATH + suffix)
               for suffix in ('', '-journal', '-wal', '-shm'))


def run(*, stopped=lambda: False, ownership=None):
    """Production-only inputs: every account access belongs to the canonical child."""
    ownership = {} if ownership is None else ownership
    ownership['reaped'] = True
    choice = selected()
    if choice is None:
        return 0
    grant, contract, module, argument = choice
    last, mono = time.time_ns(), time.monotonic_ns()
    require(type(last) is int and type(mono) is int and last > 0 and mono > 0,
            'prepared_clock_invalid')
    if last >= grant.expires_ns or consumed(contract):
        return 0
    # Entry must occur inside the shorter admission window for execution.
    entry_end = getattr(grant, 'admission_expires_ns', grant.expires_ns)
    if last >= entry_end:
        return 0
    absolute = mono + grant.expires_ns - last
    while last < grant.starts_ns:
        require(not stopped(), 'prepared_stopped')
        time.sleep(min(.1, (grant.starts_ns - last) / NS))
        new, current_mono = time.time_ns(), time.monotonic_ns()
        require(type(new) is int and type(current_mono) is int and new >= last
                and current_mono >= mono and current_mono < absolute,
                'prepared_clock_or_expiry')
        last, mono = new, current_mono
    require(last < entry_end and not stopped(), 'prepared_entry_closed')
    require(selected() == choice, 'prepared_authority_changed')
    if consumed(contract):
        return 0
    child = None
    try:
        # A failed/uncertain Popen is not represented as proven reaped.
        ownership['reaped'] = False
        child = subprocess.Popen([sys.executable, str(Path(__file__).resolve().with_name(module)), argument],
                                 stdin=subprocess.DEVNULL, stdout=None, stderr=subprocess.DEVNULL,
                                 shell=False, close_fds=True)
        while True:
            new, current_mono = time.time_ns(), time.monotonic_ns()
            require(type(new) is int and type(current_mono) is int and new >= last
                    and current_mono >= mono, 'prepared_clock_regression')
            require(not stopped() and new < grant.expires_ns and current_mono < absolute,
                    'prepared_stopped_or_expired')
            last, mono = new, current_mono
            result = child.poll()
            if result is not None:
                require(type(result) is int, 'prepared_exit_invalid')
                return result
            time.sleep(.1)
    finally:
        if child is not None:
            try:
                running = child.poll() is None
            except BaseException:
                running = True
            if running:
                try:
                    child.kill()
                except ProcessLookupError:
                    pass
                except BaseException:
                    # Still attempt a bounded reap; uncertainty remains visible.
                    try:
                        child.wait(timeout=2)
                    except BaseException:
                        pass
                    raise RuntimeError('prepared_kill_uncertain') from None
            try:
                child.wait(timeout=2)
            except BaseException:
                raise RuntimeError('prepared_reap_uncertain') from None
            ownership['reaped'] = True


class PreparedRecoverySupervisor:
    def __init__(self, external_stop=None):
        self._choice = selected()
        self._external_stop = external_stop
        self._stop, self._done = threading.Event(), threading.Event()
        self._ownership = {'reaped': True}
        self._thread = None
        self._code = None
        self._started = False

    @property
    def requested(self):
        # Expired/consumed scopes remain visible as requested but run() does no IO.
        # This lets serve reject a simultaneous old authority even after expiration.
        return self._choice is not None

    @property
    def completed(self):
        return self._done.is_set() and self._code == 0

    @property
    def halted(self):
        return self._done.is_set() and self._code != 0

    @property
    def reaped(self):
        return (self._thread is None or not self._thread.is_alive()) and self._ownership['reaped'] is True

    def _run(self):
        try:
            require(selected() == self._choice, 'prepared_authority_changed')
            self._code = run(stopped=lambda: self._stop.is_set() or
                             (self._external_stop is not None and self._external_stop.is_set()),
                             ownership=self._ownership)
        except BaseException:
            self._code = 1
        finally:
            self._done.set()

    def start(self):
        require(not self._started and not self._stop.is_set(), 'prepared_start_consumed')
        self._started = True
        if not self.requested:
            self._code = 0
            self._done.set()
            return
        self._thread = threading.Thread(target=self._run, daemon=True)
        self._thread.start()

    def poll(self):
        return None

    def close(self):
        self._stop.set()
        if self._thread is not None and self._thread.ident is not None:
            self._thread.join(timeout=3)
