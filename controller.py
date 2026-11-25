#!/usr/bin/env python3
"""Compatibility module for callers that prefer the name ``controller``."""

from duty_ctl import *  # noqa: F403
from duty_ctl import ControlError, ControlPlane, main, run_demo

__all__ = ["ControlPlane", "ControlError", "run_demo", "main"]
