# src/backend/ijson_backend.py
from __future__ import annotations

from datetime import datetime, date
from pathlib import Path
from typing import List, Tuple, Union, Iterator
from collections import Counter

import ijson
import regex as re

__all__ = [
    "top_active_dates",
    "top_emojis",
    "top_mentioned_users",
]

# -------------------------------------------------------------------------- #
# ───────────────────────────────  HELPERS  ──────────────────────────────── #
# -------------------------------------------------------------------------- #

def _check_file_exists(path: Union[str, Path]) -> Path:
    """
    Helper for checking that the file exists.

    Parameters:
        path : Union[str, Path]
            Path to the file.

    Returns:
        Path
            Path object to the file.

    Raises:
        FileNotFoundError
            If the file does not exist.
    """
    path_obj = Path(path)
    if not path_obj.exists():
        raise FileNotFoundError(f"No such file or directory: {path!s}")
    return path_obj


def _parse_ndjson_stream(path: Path) -> Iterator[dict]:
    """
    Generator that produces each JSON object in a NDJSON file using *streaming*
    with ijson.

    Parameters:
        path : Path
            Path to the NDJSON file.

    Returns:
        Iterator[dict]
            Iterator of dictionaries, one for each JSON line.
    """
    with path.open("rb") as f:
        yield from ijson.items(f, "", multiple_values=True)


# -------------------------------------------------------------------------- #
# ────────────────────  1.  TOP ACTIVE DATES  ────────────────────────────── #
# -------------------------------------------------------------------------- #

def top_active_dates(
    path: Union[str, Path],
    n: int = 10,
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
    path_obj = _check_file_exists(path)
    if path_obj.stat().st_size == 0:
        return []

    counts_per_user_date = Counter()
    total_per_date = Counter()

    for record in _parse_ndjson_stream(path_obj):
        date_str = record.get("date")
        if not isinstance(date_str, str):
            continue
        try:
            dt_obj = datetime.strptime(date_str, "%Y-%m-%dT%H:%M:%S%z").date()
        except Exception:
            continue

        user_obj = record.get("user")
        if not isinstance(user_obj, dict):
            continue
        username = user_obj.get("username")
        if not isinstance(username, str):
            continue

        counts_per_user_date[(dt_obj, username)] += 1
        total_per_date[dt_obj] += 1

    if not total_per_date:
        return []

    top_user_by_date = {}
    for (dt, user), cnt in counts_per_user_date.items():
        if cnt > counts_per_user_date.get((dt, top_user_by_date.get(dt, "")), -1):
            top_user_by_date[dt] = user

    ordered_dates = sorted(
        total_per_date.items(), key=lambda x: x[1], reverse=True
    )[:n]

    result: List[Tuple[date, str]] = [
        (dt, top_user_by_date[dt]) for dt, _ in ordered_dates
    ]
    return result


# -------------------------------------------------------------------------- #
# ────────────────────────  2.  TOP EMOJIS  ──────────────────────────────── #
# -------------------------------------------------------------------------- #

def top_emojis(
    path: Union[str, Path],
    n: int = 10,
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
    path_obj = _check_file_exists(path)
    if path_obj.stat().st_size == 0:
        return []

    emoji_pattern = re.compile(r"\p{Extended_Pictographic}", re.UNICODE)
    counter_emojis = Counter()

    for record in _parse_ndjson_stream(path_obj):
        content = record.get("content")
        if not isinstance(content, str):
            continue
        found = emoji_pattern.findall(content)
        if found:
            counter_emojis.update(found)

    result: List[Tuple[str, int]] = counter_emojis.most_common(n)
    return result


# -------------------------------------------------------------------------- #
# ────────────────────  3.  TOP MENTIONED USERS  ─────────────────────────── #
# -------------------------------------------------------------------------- #

def top_mentioned_users(
    path: Union[str, Path],
    n: int = 10,
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
    path_obj = _check_file_exists(path)
    if path_obj.stat().st_size == 0:
        return []

    counter_mentions = Counter()

    for record in _parse_ndjson_stream(path_obj):
        date_str = record.get("date")
        if not isinstance(date_str, str):
            continue
        try:
            _ = datetime.strptime(date_str, "%Y-%m-%dT%H:%M:%S%z")
        except Exception:
            continue

        user_obj = record.get("user")
        if not isinstance(user_obj, dict):
            continue
        user_name = user_obj.get("username")
        if not isinstance(user_name, str):
            continue

        mentioned = record.get("mentionedUsers")
        if not isinstance(mentioned, list):
            continue

        for mentioned_user in mentioned:
            if not isinstance(mentioned_user, dict):
                continue
            uname = mentioned_user.get("username")
            if isinstance(uname, str):
                counter_mentions[uname] += 1

    result: List[Tuple[str, int]] = counter_mentions.most_common(n)
    return result
