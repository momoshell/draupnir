"""Tests for parser child-process resource limit helpers."""

from __future__ import annotations

from app.ingestion.adapters._process_limits import (
    ParserProcessLimits,
    apply_parser_process_limits,
)


class _FakeResource:
    RLIMIT_FSIZE = 1
    RLIMIT_AS = 2
    RLIMIT_CPU = 3
    RLIM_INFINITY = -1

    def __init__(self) -> None:
        self.hard_limits = {
            self.RLIMIT_FSIZE: self.RLIM_INFINITY,
            self.RLIMIT_AS: 512,
            self.RLIMIT_CPU: self.RLIM_INFINITY,
        }
        self.applied: list[tuple[int, tuple[int, int]]] = []

    def getrlimit(self, limit_kind: int) -> tuple[int, int]:
        return (self.RLIM_INFINITY, self.hard_limits[limit_kind])

    def setrlimit(self, limit_kind: int, limits: tuple[int, int]) -> None:
        self.applied.append((limit_kind, limits))


def test_apply_parser_process_limits_sets_supported_caps() -> None:
    resource = _FakeResource()

    apply_parser_process_limits(
        ParserProcessLimits(
            max_file_size_bytes=1024,
            max_address_space_bytes=2048,
            max_cpu_seconds=2.2,
        ),
        resource_module=resource,
    )

    assert resource.applied == [
        (resource.RLIMIT_FSIZE, (1024, 1024)),
        (resource.RLIMIT_AS, (512, 512)),
        (resource.RLIMIT_CPU, (3, 3)),
    ]


def test_apply_parser_process_limits_ignores_non_positive_caps() -> None:
    resource = _FakeResource()

    apply_parser_process_limits(
        ParserProcessLimits(
            max_file_size_bytes=0,
            max_address_space_bytes=-1,
            max_cpu_seconds=None,
        ),
        resource_module=resource,
    )

    assert resource.applied == []
