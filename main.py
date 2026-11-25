#!/usr/bin/env python3
"""Conventional entry point; implementation lives in :mod:`duty_ctl`."""

from duty_ctl import ControlPlane, main

__all__ = ["ControlPlane", "main"]


if __name__ == "__main__":
    raise SystemExit(main())
