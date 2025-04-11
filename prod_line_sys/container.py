from typing import Optional, Final
from functools import partial
from threading import Lock

import rclpy

from rclpy.node import Node
from rclpy.callback_groups import MutuallyExclusiveCallbackGroup, ReentrantCallbackGroup

from smdps_msgs.msg import CameraTrigger
from smdps_msgs.srv import UInt8 as UInt8Srv

from .const import Const

class Container(Node):
    def __init__(self):
        super().__init__("container_node")

        self.MOVE_INTO_CAMERA_NO: Final[int] = 11
        self.MOVE_OUT_CAMERA_NO: Final[int] = 1

        self.last_move_in_mtrl_box_id: int = 0
        self.last_move_out_mtrl_box_id: int = 0

        self.mutex = Lock()

        # Callback groups
        sub_cbg = ReentrantCallbackGroup()
        srv_cli_cbg = MutuallyExclusiveCallbackGroup()

        # Subscriptions
        self.qr_scan_sub = self.create_subscription(CameraTrigger, "qr_camera_scan", self.qr_scan_cb, 10, callback_group=sub_cbg)

        # Service clients
        self.con_mtrl_box_cli = self.create_client(UInt8Srv, "container_material_box", callback_group=srv_cli_cbg)

        self.get_logger().info("Conatiner Node is started")

    def qr_scan_cb(self, msg: CameraTrigger) -> None:
        if not (Const.CAMERA_ID_START <= msg.camera_id <= Const.CAMERA_ID_END):
            self.get_logger().error(f"Received undefined camera_id: {msg.camera_id}")
            return
        if not (Const.MTRL_BOX_ID_START <= msg.material_box_id <= Const.MTRL_BOX_ID_END):
            self.get_logger().error(f"Received undefined material_box_id: {msg.material_box_id}")
            return
        
        match msg.camera_id:
            case self.MOVE_OUT_CAMERA_NO:
                with self.mutex:
                    prev_mtrl_box_id = self.last_move_out_mtrl_box_id
                    self.last_move_out_mtrl_box_id = msg.material_box_id

                if prev_mtrl_box_id != msg.material_box_id:
                    pass

            case self.MOVE_INTO_CAMERA_NO:
                with self.mutex:
                    prev_mtrl_box_id = self.last_move_in_mtrl_box_id
                    self.last_move_in_mtrl_box_id = msg.material_box_id
                
                if prev_mtrl_box_id != msg.material_box_id:
                    success = self.send_con_mtrl_box(msg.material_box_id)

    def send_con_mtrl_box(self, mtrl_box_id: int) -> Optional[bool]:
        """
        Send a material box ID to the container service.
        
        Args:
            mtrl_box_id: The material box ID to send
            
        Returns:
            bool: True if successfully sent, False otherwise
            
        Raises:
            TypeError: If mtrl_box_id is not an integer
            ValueError: If mtrl_box_id is negative
        """
        if not isinstance(mtrl_box_id, int):
            raise TypeError(f"Expected integer mtrl_box_id, got {type(mtrl_box_id).__name__}")
        if mtrl_box_id < 0:
            raise ValueError(f"Material box ID must be non-negative, got {mtrl_box_id}")
        
        req = UInt8Srv.Request()
        req.data = mtrl_box_id

        MAX_RETIES = 3
        reties = 0

        while not self.con_mtrl_box_cli.wait_for_service(timeout_sec=1.0):
            if not rclpy.ok() or reties >= MAX_RETIES:
                return None
            reties += 1
            self.get_logger().info("container_material_box Service not available, waiting again...")

        try:
            future = self.con_mtrl_box_cli.call_async(req)
            future.add_done_callback(partial(self.con_mtrl_box_done_cb, mtrl_box_id))
            
            return True
        except AttributeError as e:
            self.get_logger().error(f"Invalid service response for ID {mtrl_box_id}: {str(e)}")
            return False
        except Exception as e:
            self.get_logger().error(f"Service call failed for ID {mtrl_box_id}: {str(e)}")
            return False
        
    def con_mtrl_box_done_cb(self, mtrl_box_id: int, future) -> None:
        res = future.result()

        if res and res.success:
            self.get_logger().info(f"Successfully sent material box ID: {mtrl_box_id}")
        else:
            self.get_logger().error(f"Service call succeeded but reported failure for ID: {mtrl_box_id}")  
    
    def destroy_node(self):
        self.get_logger().info(">>>> Yeah! Shutdown now!")

        super().destroy_node()