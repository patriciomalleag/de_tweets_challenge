# src/backend/duckdb_backend.py

from __future__ import annotations

from datetime import date
from pathlib import Path
from typing import List, Tuple

import duckdb
import regex as re
from collections import Counter

__all__ = [
    "top_active_dates",
    "top_emojis",
    "top_mentioned_users",
]


# -------------------------------------------------------------------------- #
# ───────────────────────────────  HELPERS  ──────────────────────────────── #
# -------------------------------------------------------------------------- #

def _get_connection() -> duckdb.DuckDBPyConnection:
    """
    Helper for creating and returning a new connection to DuckDB in in-memory
    mode. It is used in each main function to execute the SQL queries.

    Returns:
        duckdb.DuckDBPyConnection
            A connection to DuckDB in in-memory mode.
    """
    return duckdb.connect(database=":memory:")


def _check_file_exists(path: Path) -> None:
    """
    Helper for checking that the file exists.
    
    Parameters:
        path : Path
            Path to the file.
    
    Raises:
        FileNotFoundError
            If the file does not exist in the file system.
    """
    if not path.exists():
        raise FileNotFoundError(f"No such file or directory: {str(path)}")


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
    _check_file_exists(path_obj)
    if path_obj.stat().st_size == 0:
        return []

    con = _get_connection()
    try:
        tabla_ndjson = f"read_ndjson('{str(path_obj)}')"

        base_query = f"""
            WITH cte_base AS (
                SELECT
                    -- TRY_CAST evita explotar con fechas malas
                    TRY_CAST(SUBSTR("date", 1, 10) AS DATE)         AS dt,
                    TRIM(BOTH '"' FROM JSON_EXTRACT("user",'$.username')) AS username
                FROM {tabla_ndjson}
                WHERE "date" IS NOT NULL
                  AND "user" IS NOT NULL
                  AND TRY_CAST(SUBSTR("date", 1, 10) AS DATE) IS NOT NULL
                  AND JSON_EXTRACT("user",'$.username') IS NOT NULL
                  AND JSON_EXTRACT("user",'$.username') <> 'null'
            ),

            cte_total_per_day AS (
                SELECT
                    dt,
                    COUNT(*) AS tweets_total
                FROM cte_base
                GROUP BY dt
            ),

            cte_counts_per_user AS (
                SELECT
                    dt,
                    username,
                    COUNT(*) AS cnt
                FROM cte_base
                GROUP BY dt, username
            ),

            cte_top_user_per_day AS (
                SELECT
                    dt,
                    username
                FROM (
                    SELECT
                        dt,
                        username,
                        cnt,
                        ROW_NUMBER() OVER (
                            PARTITION BY dt
                            ORDER BY cnt DESC,
                                     username ASC
                        ) AS rn
                    FROM cte_counts_per_user
                ) sub
                WHERE rn = 1
            )

            SELECT
                a.dt,
                t.username
            FROM cte_total_per_day AS a
            LEFT JOIN cte_top_user_per_day AS t
                ON a.dt = t.dt
            ORDER BY a.tweets_total DESC
            LIMIT {n}
        """

        fetched_data = con.execute(base_query).fetchall()
        result: List[Tuple[date, str]] = [(row[0], row[1]) for row in fetched_data]
        return result

    finally:
        con.close()


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
    path_obj = Path(path)
    _check_file_exists(path_obj)
    if path_obj.stat().st_size == 0:
        return []

    emoji_pattern = re.compile(r"\p{Extended_Pictographic}", re.UNICODE)

    con = _get_connection()
    try:
        ndjson_table = f"read_ndjson('{str(path_obj)}')"
        content_query = f"""
            SELECT content
            FROM {ndjson_table}
            WHERE content IS NOT NULL
        """
        result = con.execute(content_query).fetchall()
    finally:
        con.close()

    counter = Counter()
    for row in result:
        content_str = row[0]
        found = emoji_pattern.findall(content_str)
        counter.update(found)

    top_n = counter.most_common(n)
    result: List[Tuple[str, int]] = [(emoji, int(freq)) for emoji, freq in top_n]
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
    path_obj = Path(path)
    _check_file_exists(path_obj)
    if path_obj.stat().st_size == 0:
        return []

    con = _get_connection()
    try:
        ndjson_table = f"read_ndjson('{str(path_obj)}')"
        mentioned_users_query = f"""
            WITH cte_exploded AS (
                SELECT
                    UNNEST(mentionedUsers) AS unnest_element
                FROM {ndjson_table}
                WHERE mentionedUsers IS NOT NULL
            )
            SELECT
                TRIM(BOTH '"' FROM JSON_EXTRACT(unnest_element, '$.username')) AS username,
                COUNT(*) AS cnt
            FROM cte_exploded
            WHERE JSON_EXTRACT(unnest_element, '$.username') IS NOT NULL
            GROUP BY username
            ORDER BY cnt DESC
            LIMIT {n}
        """
        result = con.execute(mentioned_users_query).fetchall()
        result: List[Tuple[str, int]] = [(row[0], int(row[1])) for row in result]
        return result

    finally:
        con.close()
