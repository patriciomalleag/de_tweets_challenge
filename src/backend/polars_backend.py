# src/backend/polars_backend.py

from __future__ import annotations

from datetime import date
from pathlib import Path
from typing import List, Tuple

import polars as pl

__all__ = [
    "top_active_dates",
    "top_emojis",
    "top_mentioned_users",
]


# -------------------------------------------------------------------------- #
# ───────────────────────────────  HELPERS  ──────────────────────────────── #
# -------------------------------------------------------------------------- #

def _lazy_scan(
    path: str | Path,
    cols: List[str]
) -> pl.LazyFrame:
    """
    Helper for scanning a NDJSON and selecting only the columns of the first
    level indicated in `cols`.

    Parameters:
        path: Path to the NDJSON file. Can be str or Path.
        cols: List of names of the first level columns that we want to load.
              Examples of valid names:
                - "date"
                - "user"
                - "content"
                - "mentionedUsers"
              Nested expressions are not allowed here (e.g. "user.username").
              Simply select the first level column.

    Returns:
        pl.LazyFrame with the selected columns.

    Raises:
        FileNotFoundError: if the `path` does not exist.
    """
    if not Path(path).exists():
        raise FileNotFoundError(f"No such file or directory: {path!s}")

    return pl.scan_ndjson(path).select([pl.col(c) for c in cols])


# -------------------------------------------------------------------------- #
# ────────────────────  1.  TOP ACTIVE DATES  ────────────────────────────── #
# -------------------------------------------------------------------------- #

def top_active_dates(
    path: str | Path,
    n: int = 10
) -> List[Tuple[date, str]]:
    """
    Returns the n dates with the most tweets and, for each date, the user that
    tweeted the most that day.

    Parameters:
        path: Path to the NDJSON file.
        n: Number of dates to return.

    Returns:
        List[Tuple[date (datetime.date), username (str)]] ordered by number of
        tweets from highest to lowest.
    """

    if Path(path).exists() and Path(path).stat().st_size == 0:
        return []

    lf_base = (
        _lazy_scan(path, ["date", "user"])
          .filter(pl.col("date").is_not_null())
          .filter(pl.col("user").is_not_null())
          .with_columns(
              pl.col("date")
                .str.strptime(pl.Datetime, "%Y-%m-%dT%H:%M:%S%z", strict=False)
                .dt.date()
                .alias("dt"),
              pl.col("user").struct.field("username").alias("username")
          )
          .select("dt", "username")
    )

    df_grouped = (
        lf_base
          .group_by(["dt", "username"])
          .agg(pl.len().alias("cnt"))
          .sort(["dt", "cnt"], descending=[False, True])
    )

    df_top_user = (
        df_grouped
          .group_by("dt")
          .first()
          .select("dt", "username")
    )

    total_per_day = (
        lf_base
          .group_by("dt")
          .agg(pl.len().alias("tweets_total"))
    )

    df_result = (
        total_per_day
          .join(df_top_user, on="dt", how="left")
          .sort("tweets_total", descending=True)
          .head(n)
          .collect()
    )

    result: List[Tuple[date, str]] = []
    for row in df_result.iter_rows(named=True):
        date: date = row["dt"]
        username: str = row["username"]
        result.append((date, username))

    return result


# -------------------------------------------------------------------------- #
# ────────────────────────  2.  TOP EMOJIS  ──────────────────────────────── #
# -------------------------------------------------------------------------- #

def top_emojis(
    path: str | Path,
    n: int = 10
) -> List[Tuple[str, int]]:
    """
    Returns the n most frequent emojis in the content of all tweets.

    Parameters:
        path: Path to the NDJSON file.
        n: Number of emojis to return.

    Returns:
        List[Tuple[emoji (str), frequency (int)]] ordered by frequency from
        highest to lowest.
    """
    lf_content = (
        _lazy_scan(path, ["content"])
          .filter(pl.col("content").is_not_null())
    )

    lf_emojis = (
        lf_content
            .with_columns(
                pl.col("content")
                .str.extract_all(r"\p{Extended_Pictographic}")
                .alias("emoji")
            )
            .select("emoji")
            .explode("emoji")
            .drop_nulls()
        )

    top_emojis_df = (
        lf_emojis
          .group_by("emoji")
          .agg(pl.len().alias("cnt"))
          .sort("cnt", descending=True)
          .head(n)
          .collect()
    )

    result: List[Tuple[str, int]] = []
    for row in top_emojis_df.iter_rows(named=True):
        emj: str = row["emoji"]
        freq: int = row["cnt"]
        result.append((emj, freq))

    return result


# -------------------------------------------------------------------------- #
# ────────────────────  3.  TOP MENTIONED USERS  ─────────────────────────── #
# -------------------------------------------------------------------------- #

def top_mentioned_users(
    path: str | Path,
    n: int = 10
) -> List[Tuple[str, int]]:
    """
    Returns the n most mentioned users in all tweets.

    Parameters:
        path : str | Path
            Path to the NDJSON file.
        n : int, default 10
            Number of mentioned users to return.

    Returns:
        List[Tuple[str, int]] ordered by frequency from highest to lowest.
    """
    if Path(path).exists() and Path(path).stat().st_size == 0:
        return []

    lf = _lazy_scan(path, ["mentionedUsers"])

    lf_exploded = lf.explode("mentionedUsers")

    lf_extracted = (
        lf_exploded
        .with_columns(
            pl.col("mentionedUsers")
              .map_elements(
                  lambda x: x.get("username") if isinstance(x, dict) else None,
                  return_dtype=pl.Utf8
              )
              .alias("username")
        )
        .drop_nulls("username")
        .select("username")
    )

    top_df = (
        lf_extracted
        .group_by("username")
        .agg(pl.len().alias("cnt"))
        .sort("cnt", descending=True)
        .head(n)
        .collect()
    )

    return [(row["username"], row["cnt"]) for row in top_df.iter_rows(named=True)]
