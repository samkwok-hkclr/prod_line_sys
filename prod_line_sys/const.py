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
    CAMERA_ID_END: Final[int] = 11

    MTRL_BOX_ID_START: Final[int] = 1
    MTRL_BOX_ID_END: Final[int] = 20

    PKG_MAC_ID_START: Final[int] = 1
    PKG_MAC_ID_END: Final[int] = 2

    MAX_MTRL_BOX_IN_CON: Final[int] = 10

    GO_STRAIGHT: Final[int] = 0
    
    CELLS: Final[int] = 28
    
    NUM_DISPENSER_STATIONS: Final[int] = 14