"""Entrypoints for the emissions tracker CLI commands."""

from emissions_tracker.entrypoints.contract import run as run_contract
from emissions_tracker.entrypoints.mining import run as run_mining

__all__ = ['run_contract', 'run_mining']
