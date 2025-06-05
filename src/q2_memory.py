from typing import List, Tuple

from backend.ijson_backend import top_emojis

def q2_memory(file_path: str) -> List[Tuple[str, int]]:
    return top_emojis(file_path, n=10)