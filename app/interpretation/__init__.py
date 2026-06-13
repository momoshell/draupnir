"""Interpretation layer (tier-3): semantic views derived from the immutable canonical model.

This package never mutates extraction entities. It reads the materialized canonical model and
computes derived, recomputable semantic views (e.g. device/fixture schedules by associating
text tags to device instances spatially). See the data-platform backlog for the layering.
"""
