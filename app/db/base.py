"""SQLAlchemy declarative base for database models."""

from sqlalchemy.orm import DeclarativeBase


class Base(DeclarativeBase):
    """Base class for all SQLAlchemy models.

    Provides common configuration and utilities for all models.
    """
