from threading import Lock

from rclpy.node import Node

from std_msgs.msg import Bool, UInt8MultiArray
from rclpy.callback_groups import MutuallyExclusiveCallbackGroup

from smdps_msgs.srv import DispenseDrug, MoveSlidingPlatform

class DispenseNode(Node):
    def __init__(self, id):
        super().__init__(f"dispense_node_{id}")
        self.id = id
        
        self.mutex = Lock()

        self.is_completed = False
        self.is_platfrom_ready = False
        self.cmd_sliding_platform_loc: int = 0
        self.curr_sliding_platform_loc: int = 0
        self.curr_cell: int = 0

        exe_timer_cbg = MutuallyExclusiveCallbackGroup()

        # Subscriptions
        self.sliding_platform_sub = self.create_subscription(UInt8MultiArray, "sliding_platform", self.sliding_platform_cb, 10)
        self.sliding_platform_ready_sub = self.create_subscription(UInt8MultiArray, "sliding_platform_ready", self.sliding_platform_ready_cb, 10)
        
        # Service Clients
        self.dis_station_cli = self.create_client(DispenseDrug, f"/dispenser_station_{id}/dispense_request")
        self.move_sliding_platform_cli = self.create_client(MoveSlidingPlatform, "move_sliding_platform")

        # Timers
        self.execution_timer = self.create_timer(0.2, self.execution, callback_group=exe_timer_cbg)
        self.completion_timer = self.create_timer(1.0, self.completion)


    def sliding_platform_cb(self, msg: UInt8MultiArray) -> None:
        if len(msg.data) < id:
            self.get_logger().warning(f"Message length {len(msg.data)} doesn't match stations")
            return
        try:
            with self.mutex:
                self.curr_sliding_platform_loc = msg.data[id]
            self.get_logger().info(f"Received sliding platform message: {msg}")
        except Exception as e:
            self.get_logger().error(f"Error processing sliding platform message: {e}")

    def sliding_platform_ready_cb(self, msg: UInt8MultiArray) -> None:
        if len(msg.data) < id:
            self.get_logger().warning(f"Message length {len(msg.data)} doesn't match stations")
            return
        try:
            with self.mutex:
                self.is_platfrom_ready = bool(msg.data[id])
            self.get_logger().info(f"Received sliding platform ready message: {msg}")
        except Exception as e:
            self.get_logger().error(f"Error processing sliding platform ready message: {e}")
    
    def execution(self):
        if not self.is_platfrom_ready:
            self.get_logger().info(f"The sliding platform is not ready")
            return
        

    def completion(self):
        with self.mutex:
            if rclpy.ok() and self.is_completed:
                rclpy.shutdown()
