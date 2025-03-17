from typing import List

class DispenserStation:
    def __init__(self, id):
        self.id = id
        self.curr_mtrl_box: int = 0
        self.curr_sliding_platform: int = 0
        self.cmd_sliding_platform: int = 0
        self.is_completed: List[bool] = [False] * 28
        self.is_platform_ready: bool = False
        self.is_occupied: bool = False

    def __str__(self):
        return f"Station id:{self.id}, box:{self.curr_mtrl_box}, occupied:{self.is_occupied}"