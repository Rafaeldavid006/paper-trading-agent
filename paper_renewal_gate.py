"""Read-only eligibility gate for the single explicitly approved renewal.

No worker/factory wiring, database mutation, secret loading, or order method.
A later approved generation must repeat these checks under its final write lock.
"""
import hashlib
import json
import sqlite3
from pathlib import Path
from paper_adapter import SafetyError, money
from paper_experiment import EXPERIMENT_ID, APPROVAL


def client_id(generation,side):
    return 'pa-'+hashlib.sha256((generation+':'+side).encode()).hexdigest()[:40]


def original_fingerprint(database, *, allow_stopped=False):
    """Injected transport is used ONLY for fixed broker GETs. Never changes state."""
    path=Path(database)
    if path.is_symlink() or not path.is_file():raise SafetyError('Existing ledger required')
    with sqlite3.connect(path.absolute().as_uri()+'?mode=ro',uri=True) as c:
        c.row_factory=sqlite3.Row
        old=c.execute('SELECT * FROM paper_experiment WHERE id=1').fetchone()
        account_state=c.execute('SELECT * FROM paper_readonly WHERE id=1').fetchone()
        safety=c.execute('SELECT stopped FROM safety WHERE id=1').fetchone()
        if not old or not account_state or not safety:raise SafetyError('Incomplete ledger')
        if not allow_stopped and (account_state['stopped'] or safety['stopped']):raise SafetyError('Stop remains latched')
        if old['experiment']!=EXPERIMENT_ID or old['approval']!=APPROVAL or old['phase']!='manual_attention' or old['reason']!='quote_moved':
            raise SafetyError('Original experiment is not eligible for this renewal')
        if old['fingerprint']!=account_state['fingerprint'] or old['sell_id'] is not None:
            raise SafetyError('Identity or prior sell evidence requires investigation')
        if old['buy_id']!=client_id(EXPERIMENT_ID,'buy'):raise SafetyError('Original buy ID mismatch')
        intents=c.execute('SELECT * FROM intents').fetchall()
        if len(intents)!=1:raise SafetyError('Unexpected intent history')
        prior=intents[0]
        if prior['client_id']!=old['buy_id'] or prior['state']!='reserved' or prior['broker_id'] is not None or money(prior['filled'])!=0:
            raise SafetyError('Prior transmission/fill evidence requires investigation')
        payload=json.loads(prior['payload'])
        if (payload.get('client_order_id')!=prior['client_id'] or payload.get('symbol')!='AAPL'
                or payload.get('side')!='buy' or money(payload['qty'])!=1
                or payload.get('type')!='limit' or payload.get('time_in_force')!='day'
                or payload.get('extended_hours') is not False or not 1<=money(payload['limit_price'])<=1000):
            raise SafetyError('Unexpected prior payload')
        fingerprint=old['fingerprint']
    return fingerprint


def inspect_renewal(database, transport):
    fingerprint=original_fingerprint(database)
    def get(endpoint):
        status,data=transport.call('GET',endpoint)
        if status!=200:raise SafetyError('Read-only verification incomplete')
        return data
    account=get('/v2/account')
    identity=account.get('id')
    if not isinstance(identity,str) or hashlib.sha256(identity.encode()).hexdigest()!=fingerprint:
        raise SafetyError('Account mismatch')
    for side in ('buy','sell'):
        status,_=transport.call('GET','/v2/orders:by_client_order_id?client_order_id='+client_id(EXPERIMENT_ID,side))
        if status!=404:raise SafetyError('Prior order exists or lookup is uncertain')
    positions=get('/v2/positions')
    orders=get('/v2/orders?status=open&limit=500&nested=false')
    if not isinstance(positions,list) or len(positions)>500 or not isinstance(orders,list) or orders:
        raise SafetyError('Incomplete snapshot or existing orders')
    if any(not isinstance(p,dict) or p.get('symbol')=='AAPL' for p in positions):
        raise SafetyError('AAPL exposure or malformed position')
    return dict(eligible_for_separate_approval=True,execution_enabled=False,
                original_ledger_preserved=True,broker_old_ids_not_found=True,
                aapl_holding_absent=True,open_orders_absent=True)
