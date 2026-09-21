"""The :func:`backup` factory — one call that names a cloud and returns the
matching archive descriptor.

``S3Archive`` and ``GcsArchive`` remain the explicit spellings; this is the
form to reach for when the cloud is a parameter rather than a constant, e.g.
read from config.
"""

from __future__ import annotations

from typing import Any, Union

from ._quickhouse import GcsArchive, S3Archive

__all__ = ["backup"]

# Accepted spellings per backend. The aliases exist because "gs" (the URI
# scheme) and "google" are what people reach for as often as "gcs".
_GCS_NAMES = {"gcs", "gs", "google", "google_cloud_storage"}
_S3_NAMES = {"s3", "aws"}

_FORMATS = {"parquet"}


def backup(
    *,
    destination: str = "gcs",
    format: str = "parquet",  # noqa: A002 - the user-facing kwarg is `format=`
    **kwargs: Any,
) -> Union[S3Archive, GcsArchive]:
    """Build a Parquet backup descriptor for ``destination=`` on a sync.

    Pass the result as a destination's ``archive=``; every batch written to
    that destination is also streamed to cloud object storage as Parquet, one
    file per parallel partition::

        dst = quickhouse.ClickHouse(
            "http://host:8123", database="analytics",
            archive=quickhouse.backup(destination="gcs", format="parquet",
                                      bucket="my-lake", prefix="quickhouse"),
        )
        quickhouse.sync(src, dst, dest_table="orders", source_table="orders")

    Parameters
    ----------
    destination:
        ``"gcs"`` (default; also ``"gs"``, ``"google"``) or ``"s3"`` (also
        ``"aws"``). Case-insensitive.
    format:
        ``"parquet"`` — the only supported backup format, and the default.
        Accepted as a parameter so a call reads explicitly and so other
        formats can be added without changing the signature.
    **kwargs:
        Passed straight to :class:`~quickhouse.GcsArchive` or
        :class:`~quickhouse.S3Archive`, so every option those accept works
        here unchanged and the two can never drift. ``bucket`` is required;
        a backend-specific option given to the wrong cloud (``region=`` with
        ``destination="gcs"``) surfaces as that class's own ``TypeError``.

    Raises
    ------
    ValueError
        If ``format`` is not ``"parquet"``, or ``destination`` names a cloud
        quickhouse cannot archive to. Both are wrong literals in the call
        itself, so they fail here rather than later inside ``sync()``.

    .. versionadded:: 0.19.0
    """
    fmt = format.strip().lower()
    if fmt not in _FORMATS:
        raise ValueError(
            f"backup(format={format!r}) is not supported; "
            f"the only supported backup format is 'parquet'"
        )

    dest = destination.strip().lower()
    if dest in _GCS_NAMES:
        return GcsArchive(**kwargs)
    if dest in _S3_NAMES:
        return S3Archive(**kwargs)
    raise ValueError(
        f"backup(destination={destination!r}) is not supported; "
        f"expected 'gcs' or 's3'"
    )
