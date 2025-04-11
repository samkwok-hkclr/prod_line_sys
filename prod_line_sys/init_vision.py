from typing import Optional

import rclpy

from rclpy.node import Node
from rclpy.callback_groups import MutuallyExclusiveCallbackGroup

from std_srvs.srv import Trigger

class InitVision(Node):
    def __init__(self):
        super().__init__("init_vision_node")

        # Callback groups
        init_vision_timer_cbg = MutuallyExclusiveCallbackGroup()
        srv_cli_cbg = MutuallyExclusiveCallbackGroup()

        # Service clients
        self.init_vision_cli = self.create_client(Trigger, "init_visual_camera", callback_group=srv_cli_cbg)

        # Timers
        self.init_vision_timer = self.create_timer(30.0, self.init_vision_cb, callback_group=init_vision_timer_cbg)
        
        self.get_logger().info("Init vision Node is started")

    def init_vision_cb(self) -> None:
        success = self.send_init_vision()

        if success:
            self.get_logger().info(f"Initiated the vision inspection system successfully")
        else:
            self.get_logger().error(f"Initiate the vision inspection system failure or timeout")
            
    def send_init_vision(self)-> Optional[bool]:
        MAX_RETIES = 3
        reties = 0

        while not self.init_vision_cli.wait_for_service(timeout_sec=0.5):
            if not rclpy.ok() or reties >= MAX_RETIES:
                return None
            reties += 1
            self.get_logger().warning("init_visual_camera Service not available, waiting again...")
        
        req = Trigger.Request()

        try:
            future = self.init_vision_cli.call_async(req)
            future.add_done_callback(self.init_vision_done_cb)

            self.get_logger().info("init_vision_cli is called, waiting for future done")
            return True
        except AttributeError as e:
            self.get_logger().error(f"Invalid service response: {str(e)}")
            return False
        except Exception as e:
            self.get_logger().error(f"Service call failed: {str(e)}")
            return False
        
    def init_vision_done_cb(self, future) -> None:
        res = future.result()

        if res and res.success:
            self.init_vision_timer.cancel()
            self.get_logger().info(f"Stopped the init vision timer")
            self.get_logger().info(f"Sent the init visual camera successfully")

            self.destroy_node()
        else:
            self.get_logger().error("Failed to send init visual camera. Retry later.")

    def destroy_node(self):
        self.get_logger().info(">>>> Yeah! Shutdown now!")

        super().destroy_node()
           
    
    