from .dispenser_station import DispenserStation

class ConveyorSegment:
    def __init__(self, id, l_station=None, r_station=None, next=None):
        self.id: int = id
        self.curr_mtrl_box: int = 0
        self.is_occupied: bool = False
        self.l_station: DispenserStation = l_station
        self.r_station: DispenserStation = r_station
        self.right_station = None
        self.next = None
    
    def __str__(self):
        return f"Conveyor id:{self.id}, box:{self.curr_mtrl_box}, occupied:{self.is_occupied}"