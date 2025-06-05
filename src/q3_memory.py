from typing import List, Tuple

from backend.ijson_backend import top_mentioned_users

def q3_memory(file_path: str) -> List[Tuple[str, int]]:
    return top_mentioned_users(file_path, n=10)