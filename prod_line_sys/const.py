from typing import final, Final, Dict, List

@final
class Const:
    DEBUG_FLAG = False
    
    FAKE_DISPENSE: Final[bool] = False
    FORCE_MOVE_OUT: Final[bool] = False

    MOVEMENT_VERSION: Final[int] = 2

    GRID_WIDTH: Final[int] = 7
    GRID_HEIGHT: Final[int] = 4

    CAMERA_ID_START: Final[int] = 1
    CAMERA_ID_SPLIT_END: Final[int] = 8
    CAMERA_ID_VISION: Final[int] = 9
    CAMERA_ID_PKG_MAC_1: Final[int] = 10
    CAMERA_ID_PKG_MAC_2: Final[int] = 11
    CAMERA_ID_END: Final[int] = 11

    MTRL_BOX_ID_START: Final[int] = 1
    MTRL_BOX_ID_END: Final[int] = 20

    PKG_MAC_ID_START: Final[int] = 1
    PKG_MAC_ID_END: Final[int] = 2

    MAX_MTRL_BOX_IN_CON: Final[int] = 10

    GO_STRAIGHT: Final[int] = 0
    
    CELLS: Final[int] = 28
    
    NUM_DISPENSER_STATIONS: Final[int] = 14

    MAX_HISTORY_AGE_SEC: Final[float] = 60.0

    INDEX_MAP: Final[Dict] = {
        0: 0, 
        1: 4, 
        2: 8, 
        3: 12, 
        4: 16, 
        5: 20, 
        6: 24, 
        7: 25, 
        8: 21, 
        9: 17,
        10: 13, 
        11: 9, 
        12: 5, 
        13: 1, 
        14: 2, 
        15: 6, 
        16: 10, 
        17: 14, 
        18: 18,
        19: 22, 
        20: 26, 
        21: 27, 
        22: 23, 
        23: 19, 
        24: 15, 
        25: 11, 
        26: 7, 
        27: 3
    }