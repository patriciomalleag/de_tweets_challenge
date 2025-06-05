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
        path : str | Path
            Path to the NDJSON file. Can be str or Path.
        cols : List[str]
            List of names of the first level columns that we want to load.
            Examples of valid names:
                - "date"
                - "user"
                - "content"
                - "mentionedUsers"
            Nested expressions are not allowed here (e.g. "user.username").
            Simply select the first level column.

    Returns:
        pl.LazyFrame
            LazyFrame with the selected columns.

    Raises:
        FileNotFoundError
            If the `path` does not exist.
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
        path : str | Path
            Path to the NDJSON file.
        n : int, default 10
            Number of dates to return.

    Returns:
        List[Tuple[date, str]]
            List of tuples containing (date, username) ordered by number of
            tweets from highest to lowest.

    Raises:
        FileNotFoundError
            If the `path` does not exist.
    """

    path_obj = Path(path)
    if not path_obj.exists():
        raise FileNotFoundError(f"No such file or directory: {path!s}")
    if path_obj.stat().st_size == 0:
        return []

    lf = pl.scan_ndjson(path_obj).select(["date", "user"])

    lf_base = (
        lf.with_columns(
            pl.col("date")
              .str.strptime(pl.Datetime, "%Y-%m-%dT%H:%M:%S%z", strict=False)
              .dt.date()
              .alias("dt"),
            pl.col("user")
              .map_elements(
                  lambda u: u.get("username") if isinstance(u, dict) else None,
                  return_dtype=pl.Utf8,
              )
              .alias("username"),
        )
        .filter(pl.col("dt").is_not_null() & pl.col("username").is_not_null())
        .select("dt", "username")
    )

    grp = (
        lf_base
        .group_by(["dt", "username"])
        .agg(pl.len().alias("cnt"))
        .sort(["dt", "cnt", "username"], descending=[False, True, False])
    )

    top_user = (
        grp.group_by("dt")
           .first()
           .select("dt", "username")
    )

    total_day = lf_base.group_by("dt").agg(pl.len().alias("total"))

    result_df = (
        total_day.join(top_user, on="dt", how="left")
                 .sort("total", descending=True)
                 .head(n)
                 .collect()
    )

    result: List[Tuple[date, str]] = [(row["dt"], row["username"]) for row in result_df.iter_rows(named=True)]
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
        path : str | Path
            Path to the NDJSON file.
        n : int, default 10
            Number of emojis to return.

    Returns:
        List[Tuple[str, int]]
            List of tuples containing (emoji, frequency) ordered by frequency
            from highest to lowest.

    Raises:
        FileNotFoundError
            If the `path` does not exist.
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

    result: List[Tuple[str, int]] = [(row["emoji"], row["cnt"]) for row in top_emojis_df.iter_rows(named=True)]
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
        List[Tuple[str, int]]
            List of tuples containing (username, frequency) ordered by frequency
            from highest to lowest.

    Raises:
        FileNotFoundError
            If the `path` does not exist.
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

    result: List[Tuple[str, int]] = [(row["username"], row["cnt"]) for row in top_df.iter_rows(named=True)]
    return result
