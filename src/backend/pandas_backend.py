# src/backend/pandas_backend.py

from __future__ import annotations

from datetime import date
from pathlib import Path
from typing import List, Tuple

import pandas as pd
import json
import regex as re 

__all__ = [
    "top_active_dates",
    "top_emojis",
    "top_mentioned_users",
]


# -------------------------------------------------------------------------- #
# ───────────────────────────────  HELPERS  ──────────────────────────────── #
# -------------------------------------------------------------------------- #

def _load_ndjson(
    path: str | Path,
    cols: List[str]
) -> pd.DataFrame:
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
        pd.DataFrame with the selected columns.

    Raises:
        FileNotFoundError: if the `path` does not exist.
    """
    if not Path(path).exists():
        raise FileNotFoundError(f"No such file or directory: {path!s}")

    records: list[dict] = []
    with open(path, "r", encoding="utf-8") as f:
        for row in f:
            if not row.strip():
                continue
            obj = json.loads(row)
            filtered: dict = {k: obj.get(k) for k in cols if k in obj}
            records.append(filtered)

    return pd.DataFrame(records)


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

    df = _load_ndjson(path, ["date", "user"])

    df = df.dropna(subset=["date", "user"])

    df["dt"] = pd.to_datetime(df["date"], errors="coerce").dt.date

    df["username"] = df["user"].apply(lambda u: u.get("username") if isinstance(u, dict) else None)

    df = df.dropna(subset=["dt", "username"])

    df_grouped = df.groupby(["dt", "username"]).size().reset_index(name="cnt")

    top_users_by_date = (
        df_grouped
          .sort_values(["dt", "cnt"], ascending=[True, False])
          .drop_duplicates(subset=["dt"], keep="first")
          .set_index("dt")["username"]
    )

    total_tweets = (
        df.groupby("dt")
          .size()
          .reset_index(name="tweets_total")
          .sort_values("tweets_total", ascending=False)
          .head(n)
    )

    total_tweets["username"] = total_tweets["dt"].map(top_users_by_date)

    result: List[Tuple[date, str]] = [
        (row["dt"], row["username"]) for _, row in total_tweets.iterrows()
    ]
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
    df = _load_ndjson(path, ["content"])
    df = df.dropna(subset=["content"])

    emoji_pattern = re.compile(r"\p{Extended_Pictographic}", re.UNICODE)

    df["emojis"] = df["content"].apply(lambda texto: emoji_pattern.findall(texto))

    series_emojis = df.explode("emojis")["emojis"].dropna()

    df_emojis_count = series_emojis.value_counts().reset_index()

    df_emojis_count.columns = ["emoji", "cnt"]

    top_emojis_df = df_emojis_count.head(n)

    result: List[Tuple[str, int]] = [
        (row["emoji"], int(row["cnt"])) for _, row in top_emojis_df.iterrows()
    ]

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
        path: Path to the NDJSON file.
        n: Number of mentioned users to return.

    Returns:
        List[Tuple[str, int]] ordered by frequency from highest to lowest.
    """
    if Path(path).exists() and Path(path).stat().st_size == 0:
        return []

    df = _load_ndjson(path, ["mentionedUsers"])
    df = df.dropna(subset=["mentionedUsers"])

    df_exploded = df.explode("mentionedUsers")

    df_exploded["username"] = df_exploded["mentionedUsers"].apply(
        lambda u: u.get("username") if isinstance(u, dict) else None
    )
    df_exploded = df_exploded.dropna(subset=["username"])

    top_df = (
        df_exploded
          .groupby("username")
          .size()
          .reset_index(name="cnt")
          .sort_values("cnt", ascending=False)
          .head(n)
    )

    result: List[Tuple[str, int]] = [
        (row["username"], int(row["cnt"])) for _, row in top_df.iterrows()
    ]

    return result
