from typing import final, Final, Dict, List

from smdps_msgs.msg import OrderRequest

@final
class PkgInfo:
    DATE_FORMAT: Final[str] = "%Y-%m-%d"

    MEAL_TIME: Final[Dict] = {
        OrderRequest.MEAL_MORNING: "Morning",
        OrderRequest.MEAL_NOON: "Noon",
        OrderRequest.MEAL_AFTERNOON: "Afternoon",
        OrderRequest.MEAL_EVENING: "Evening"
    }

    QR_CODE: Final[str] = "https://www.hkclr.hk"