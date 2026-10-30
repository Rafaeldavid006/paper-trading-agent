"""No current PAPER admission authority.

The consumed, expired admission remains in immutable repository history and its
existing durable receipt. Read-only recovery reporting reconstructs that history
from the receipt; retiring this binding neither resets state nor renews a grant.
"""
APPROVED_ADMISSION = None
