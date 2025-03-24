from threading import Lock

from .dispenser_station import DispenserStation

class ConveyorSegment:
    def __init__(self, id, l_station=None, r_station=None, next=None):
        self.id: int = id
        self._curr_mtrl_box: int = 0
        self._is_occupied: bool = False
        self.l_station: DispenserStation = l_station
        self.r_station: DispenserStation = r_station
        self.next = None

        self.mutex = Lock()

    def clear(self):
        with self.mutex:
            self._curr_mtrl_box = 0
            self._is_occupied = False

    def occupy(self, mtrl_box_id: int):
        if not isinstance(mtrl_box_id, int):
            raise TypeError(f"Expected integer id, got {type(mtrl_box_id).__name__}")
        
        with self.mutex:
            self._curr_mtrl_box = mtrl_box_id
            self._is_occupied = True

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

    def __str__(self):
        return f"Conveyor id:{self.id}, box:{self.curr_mtrl_box}, occupied:{self.is_occupied}"