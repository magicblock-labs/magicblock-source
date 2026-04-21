"""Solana pubkey ↔ byte-vector conversions using the base58 library."""

from typing import List

import base58


def vec_to_pubkey(vec: List[int]) -> str:
    """Convert a list of 32 byte values to a base58-encoded Solana pubkey."""
    if len(vec) != 32:
        raise ValueError(f"Expected 32 bytes, got {len(vec)}")
    return base58.b58encode(bytes(vec)).decode("ascii")


def bytes_to_pubkey(raw: bytes) -> str:
    """Convert 32 raw bytes to a base58-encoded Solana pubkey."""
    if len(raw) != 32:
        raise ValueError(f"Expected 32 bytes, got {len(raw)}")
    return base58.b58encode(raw).decode("ascii")


def pubkey_to_bytes(pubkey: str) -> bytes:
    """Convert a base58-encoded Solana pubkey to 32 raw bytes."""
    raw = base58.b58decode(pubkey)
    if len(raw) != 32:
        raise ValueError(f"Decoded to {len(raw)} bytes, expected 32")
    return raw


def pubkey_to_vec(pubkey: str) -> List[int]:
    """Convert a base58-encoded Solana pubkey to a list of 32 byte values."""
    return list(pubkey_to_bytes(pubkey))
