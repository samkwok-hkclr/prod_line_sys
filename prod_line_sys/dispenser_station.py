from threading import Lock
from typing import List

from .const import Const

class DispenserStation:
    def __init__(self, station_id):
        self.id = station_id
        self._curr_mtrl_box: int = 0
        self._is_occupied: bool = False
        self._is_free: bool = True
        self._curr_sliding_platform: int = 0
        self._cmd_sliding_platform: int = 0
        
        self._is_cleared_up_conveyor: bool = False

        self._is_completed: List[bool] = [False] * Const().CELLS
        self._is_dispense_req_sent: List[bool] = [False] * Const.CELLS
        self._is_dispense_req_done: List[bool] = [False] * Const.CELLS
        self._is_verified: List[bool] = [False] * Const.CELLS
        self._is_platform_ready: bool = False
        
        self.mutex = Lock()

    def clear(self):
        with self.mutex:
            self._curr_mtrl_box = 0
            self._is_occupied = False
            self._is_cleared_up_conveyor = False
            self._is_completed = [False] * Const.CELLS
            self._is_dispense_req_sent = [False] * Const.CELLS
            self._is_dispense_req_done = [False] * Const.CELLS
            self._is_verified = [False] * Const.CELLS

    def occupy(self, mtrl_box_id: int):
        if not isinstance(mtrl_box_id, int):
            raise TypeError(f"Expected integer id, got {type(mtrl_box_id).__name__}")
        
        with self.mutex:
            self._curr_mtrl_box = mtrl_box_id
            self._is_occupied = True 

    def available(self) -> bool:
        if not self._is_occupied and self._is_free:
            return True
        return False

    @property
    def curr_mtrl_box(self):
        with self.mutex:
            return self._curr_mtrl_box
    
    @curr_mtrl_box.setter
    def curr_mtrl_box(self, value):
        if not isinstance(value, int):
            raise TypeError(f"Expected integer for curr_mtrl_box, got {type(value).__name__}")
        with self.mutex:
            self._curr_mtrl_box = value

    @property
    def is_occupied(self):
        with self.mutex:
            return self._is_occupied
    
    @is_occupied.setter
    def is_occupied(self, value):
        if not isinstance(value, bool):
            raise TypeError(f"Expected boolean for is_occupied, got {type(value).__name__}")
        with self.mutex:
            self._is_occupied = value

    @property
    def is_free(self):
        with self.mutex:
            return self._is_free
    
    @is_free.setter
    def is_free(self, value):
        if not isinstance(value, bool):
            raise TypeError(f"Expected boolean for is_free, got {type(value).__name__}")
        with self.mutex:
            self._is_free = value 

    @property
    def is_cleared_up_conveyor(self):
        with self.mutex:
            return self._is_cleared_up_conveyor
    
    @is_cleared_up_conveyor.setter
    def is_cleared_up_conveyor(self, value):
        if not isinstance(value, bool):
            raise TypeError(f"Expected boolean for is_cleared_up_conveyor, got {type(value).__name__}")
        with self.mutex:
            self._is_cleared_up_conveyor = value 

    @property
    def curr_sliding_platform(self):
        with self.mutex:
            return self._curr_sliding_platform
    
    @curr_sliding_platform.setter
    def curr_sliding_platform(self, value):
        if not isinstance(value, int):
            raise TypeError(f"Expected integer for curr_sliding_platform, got {type(value).__name__}")
        with self.mutex:
            self._curr_sliding_platform = value

    @property
    def cmd_sliding_platform(self):
        with self.mutex:
            return self._cmd_sliding_platform
    
    @cmd_sliding_platform.setter
    def cmd_sliding_platform(self, value):
        if not isinstance(value, int):
            raise TypeError(f"Expected integer for cmd_sliding_platform, got {type(value).__name__}")
        with self.mutex:
            self._cmd_sliding_platform = value

    @property
    def is_completed(self):
        with self.mutex:
            return self._is_completed.copy()  # Return a copy to prevent external modification
    
    @is_completed.setter
    def is_completed(self, value):
        if not isinstance(value, list) or len(value) != Const().CELLS or not all(isinstance(x, bool) for x in value):
            raise ValueError(f"Expected list of {Const().CELLS} booleans")
        with self.mutex:
            self._is_completed = value.copy()  # Store a copy

    # Getter and Setter methods for is_completed with index
    def get_completed(self, index: int) -> bool:
        if not 0 <= index < Const().CELLS:
            raise IndexError(f"Index {index} out of range [0, {Const().CELLS-1}]")
        with self.mutex:
            return self._is_completed[index]

    def set_completed(self, index: int, value: bool):
        if not 0 <= index < Const().CELLS:
            raise IndexError(f"Index {index} out of range [0, {Const().CELLS-1}]")
        if not isinstance(value, bool):
            raise TypeError(f"Expected boolean, got {type(value).__name__}")
        with self.mutex:
            self._is_completed[index] = value

    # Getter and Setter methods for is_dispense_req_sent with index
    def get_dispense_req_sent(self, index: int) -> bool:
        if not 0 <= index < Const().CELLS:
            raise IndexError(f"Index {index} out of range [0, {Const().CELLS-1}]")
        with self.mutex:
            return self._is_dispense_req_sent[index]

    def set_dispense_req_sent(self, index: int, value: bool):
        if not 0 <= index < Const().CELLS:
            raise IndexError(f"Index {index} out of range [0, {Const().CELLS-1}]")
        if not isinstance(value, bool):
            raise TypeError(f"Expected boolean, got {type(value).__name__}")
        with self.mutex:
            self._is_dispense_req_sent[index] = value

    # Getter and Setter methods for is_dispense_req_done with index
    def get_dispense_req_done(self, index: int) -> bool:
        if not 0 <= index < Const().CELLS:
            raise IndexError(f"Index {index} out of range [0, {Const().CELLS-1}]")
        with self.mutex:
            return self._is_dispense_req_done[index]

    def set_dispense_req_done(self, index: int, value: bool):
        if not 0 <= index < Const().CELLS:
            raise IndexError(f"Index {index} out of range [0, {Const().CELLS-1}]")
        if not isinstance(value, bool):
            raise TypeError(f"Expected boolean, got {type(value).__name__}")
        with self.mutex:
            self._is_dispense_req_done[index] = value

    # Getter and Setter methods for _is_verified with index
    def get_verified(self, index: int) -> bool:
        if not 0 <= index < Const().CELLS:
            raise IndexError(f"Index {index} out of range [0, {Const().CELLS-1}]")
        with self.mutex:
            return self._is_verified[index]

    def set_verified(self, index: int, value: bool):
        if not 0 <= index < Const().CELLS:
            raise IndexError(f"Index {index} out of range [0, {Const().CELLS-1}]")
        if not isinstance(value, bool):
            raise TypeError(f"Expected boolean, got {type(value).__name__}")
        with self.mutex:
            self._is_verified[index] = value

    def __str__(self):
        return f"Station id:{self.id}, box:{self.curr_mtrl_box}, occupied:{self.is_occupied}"