from typing import final, Final, Dict

@final
class Const:
    GO_STRAIGHT: Final[int] = 0
    
    CELLS: Final[int] = 28
    EXIT_STATION: Final[int] = 29
    
    NUM_DISPENSER_STATIONS: Final[int] = 14

    MOVEMENT_ADDR: Final[int] = 5000
    CAMERA_STATION_PLC_MAP: Final[Dict] = {
        1: ([1, 2],   5201),
        2: ([3, 4],   5202),
        3: ([5, 6],   5203),
        4: ([7],      5204),
        5: ([8],      5208),
        6: ([9, 10],  5205),
        7: ([11, 12], 5206),
        8: ([13, 14], 5207)
    }
    
    STATION_VALUE_MAP: Final[Dict] = {
        1: [2],
        2: [3],
        3: [2],
        4: [3],
        5: [2],
        6: [3],
        7: [2],
        8: [3],
        9: [2],
        10: [3],
        11: [2],
        12: [3],
        13: [2],
        14: [3]
    }
