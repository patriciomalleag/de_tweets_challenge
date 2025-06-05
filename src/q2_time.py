from typing import List, Tuple

from backend.polars_backend import top_emojis

def q2_time(file_path: str) -> List[Tuple[str, int]]:
    return top_emojis(file_path, n=10)