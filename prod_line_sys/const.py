from typing import final, Final, Dict, List

@final
class Const:
    GRID_WIDTH:Final[int] = 7
    GRID_HEIGHT:Final[int] = 4

    CAMERA_ID_START: Final[int] = 1
    CAMERA_ID_END: Final[int] = 11

    MTRL_BOX_ID_START: Final[int] = 1
    MTRL_BOX_ID_END: Final[int] = 20

    PKG_MAC_ID_START: Final[int] = 1
    PKG_MAC_ID_END: Final[int] = 2

    MAX_MTRL_BOX_IN_CON: Final[int] = 20

    GO_STRAIGHT: Final[int] = 0
    
    CELLS: Final[int] = 28
    EXIT_STATION: Final[int] = 29
    
    NUM_DISPENSER_STATIONS: Final[int] = 14

    MOVEMENT_ADDR: Final[int] = 5000
    TRANSFER_MTRL_BOX_ADDR: Final[int] = 5200
    GO_OPPOSITE_ADDR: Final[int] = 5150

    EXIT_JACK_UP_VALUE: Final[List] = [1]
    MOVE_OPPOSITE_VALUE: Final[List] = [2]
    
    MTRL_BOX_RETRIEVAL_PLC_VALUES: Final[List] = [1]
    MTRL_BOX_RELEASE_PLC_VALUES: Final[List] = [2]
    MTRL_BOX_BYPASS_PLC_VALUES: Final[List] = [3]

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
    STRAIGHT_VALUE: Final[List] = [1]
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
    STATION_OPPOSITE: Final[Dict] = {
        1: [2],
        2: [1],
        3: [4],
        4: [3],
        5: [6],
        6: [5],
        7: [], # Not exist
        8: [], # Not exist
        9: [10],
        10: [9],
        11: [12],
        12: [11],
        13: [14],
        14: [13]
    }

    SENSOR_LENGTH: Final[int] = 77
    CONTAINER_SENSOR: Final[Dict] = {
        1: range(0, 1)
    }
    STATION_SENSOR: Final[Dict] = {
        1: range(7, 10),
        2: range(4, 7),
        3: range(16, 19),
        4: range(13, 16),
        5: range(25, 28),
        6: range(22, 25),
        7: range(31, 34),
        8: range(44, 47),
        9: range(53, 56),
        10: range(50, 53),
        11: range(62, 65),
        12: range(59, 62),
        13: range(71, 74),
        14: range(68, 71),
    }
    CONVEYOR_SENSOR: Final[Dict] = {
        1: range(1, 4),
        2: range(10, 13),
        3: range(19, 22),
        4: range(28, 31),
        5: range(34, 44),
        6: range(47, 50),
        7: range(56, 59),
        8: range(65, 68),
        9: range(74, 77)
    }
    

