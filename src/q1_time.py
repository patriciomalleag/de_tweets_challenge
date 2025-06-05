from typing import List, Tuple
from datetime import datetime

from backend.polars_backend import top_active_dates

def q1_time(file_path: str) -> List[Tuple[datetime.date, str]]:
    return top_active_dates(file_path, n=10)