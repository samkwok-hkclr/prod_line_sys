import sys
import time
import asyncio
from asyncio import run_coroutine_threadsafe
from collections import deque
from threading import Thread, Lock, RLock
from typing import Dict, List, Set, Tuple, Optional, Final
from array import array
from copy import copy, deepcopy
from datetime import datetime, timedelta
from functools import partial

import rclpy

from rclpy.node import Node
from rclpy.action import ActionClient
from rclpy.callback_groups import MutuallyExclusiveCallbackGroup, ReentrantCallbackGroup

from action_msgs.msg import GoalStatus
from std_msgs.msg import Bool, UInt8, UInt8MultiArray
from std_srvs.srv import Trigger

from smdps_msgs.action import VisualDetection
from smdps_msgs.msg import (CameraState, 
                            CameraStatus, 
                            CameraTrigger, 
                            Drug,
                            DrugLocation,
                            MaterialBoxStatus, 
                            ProductionLineStatus, 
                            OrderRequest,
                            OrderResponse,
                            MaterialBox,
                            PackagingMachineStatus,
                            DispenseContent,
                            DispensingDetail)
from smdps_msgs.srv import (NewOrder, 
                            ReadRegister, 
                            WriteRegister, 
                            MoveSlidingPlatform, 
                            DispenseDrug, 
                            PackagingOrder,
                            UInt8 as UInt8Srv)

from .dispenser_station import DispenserStation
from .conveyor import Conveyor
from .const import Const
from .plc_const import PlcConst
from .pkg_info import PkgInfo

class CoreSystem(Node):
    def __init__(self, executor):
        super().__init__("core_system_node")
        self.exec = executor

        self.MUTEX_TIMEOUT: Final[float] = 1.0

        # Constant
        self.const = Const()
        self.plc_const = PlcConst()

        # Local memory storage
        self.recv_order = deque(maxlen=32)
        self.proc_order: Dict[int, OrderRequest] = {} # order_id, OrderRequest
        self.mtrl_box_status: Dict[int, MaterialBoxStatus] = {} # order_id, MaterialBoxStatus
        
        self.qr_scan: List[CameraTrigger] = []
        self.qr_scan_history = deque(maxlen=16)

        self.elevator_ready = deque(maxlen=4) # append to this queue if elevator state from 0 -> 1, 
        self.elevator_request = deque(maxlen=4) # append a request if needed

        self.vision_request = deque(maxlen=8)

        self.jack_up_queue = deque(maxlen=16)
        self.jack_up_opposite_queue = deque(maxlen=16)

        self.jack_up_status: Dict[int, bool] = dict() # jack_up_pt, status
        self.jack_up_exit_status: Dict[int, bool] = dict() # jack_up_pt, status

        self.is_mtrl_box_storing: bool = False
        self.is_mtrl_box_coming: bool = False

        self.cleaning_state: bool = False
        self.release_cleaning: bool = False

        self.vision_state: bool = False
        self.release_vision: bool = False

        self.num_of_mtrl_box_in_container = 0
        self.pkg_mac_status: Dict[int, PackagingMachineStatus] = {} # pkg_mac_id, PackagingMachineStatus

        # Status of PLC registers
        self.is_plc_connected: bool = False
        self.is_releasing_mtrl_box: bool = False
        self.is_elevator_ready: bool = False
        self.is_vision_block_released: bool = False

        # ROS2 client node, maybe unused
        # self.test_cli_node = rclpy.create_node("test_cli_node")
        # self.exec.add_node(self.test_cli_node)

        # mutex, FIXME: very messy!!!
        self.mutex = RLock()
        self.occupy_mutex = RLock()
        self.qr_scan_mutex = RLock()
        self.elevator_mutex = RLock()
        self.vision_mutex = RLock()
        self.jack_up_mutex = RLock()

        # Graph of conveyor structure
        self.conveyor = None
        self._initialize_conveyor()

        # handle async functions
        self.loop = asyncio.new_event_loop()
        self.loop_thread = Thread(target=self._run_loop, daemon=True)
        self.loop_thread.start()
        self.get_logger().info("Loop Thread are started")

        # sync delay, try to avoid to use it
        # self.waiting_result = self.create_rate(1.0, self.get_clock())

        # Callback groups
        sub_cbg = MutuallyExclusiveCallbackGroup()
        plc_sub_cbg = MutuallyExclusiveCallbackGroup()
        srv_ser_cbg = MutuallyExclusiveCallbackGroup()
        srv_cli_cbg = MutuallyExclusiveCallbackGroup()
        action_cli_cbg = MutuallyExclusiveCallbackGroup()
        station_srv_cli_cbgs = [MutuallyExclusiveCallbackGroup() for _ in range(self.const.NUM_DISPENSER_STATIONS)]

        normal_timer_cbg = MutuallyExclusiveCallbackGroup()
        order_timer_cbg = MutuallyExclusiveCallbackGroup()
        occupancy_timer_cbg = MutuallyExclusiveCallbackGroup()
        elevator_timer_cbg = MutuallyExclusiveCallbackGroup()
        qr_handle_timer_cbg = MutuallyExclusiveCallbackGroup()
        jack_up_timer_cbg = MutuallyExclusiveCallbackGroup()
        clear_timer_cbg = MutuallyExclusiveCallbackGroup()
        station_timer_cbgs = [MutuallyExclusiveCallbackGroup() for _ in range(self.const.NUM_DISPENSER_STATIONS)]
        self.get_logger().info("Callback groups are created")

        # Publishers
        self.mtrl_box_status_pub = self.create_publisher(MaterialBoxStatus, "material_box_status", 10)
        self.station_occupancy_pub = self.create_publisher(UInt8MultiArray, "station_occupancy", 10)
        self.conveyor_occupancy_pub = self.create_publisher(UInt8MultiArray, "conveyor_occupancy", 10)
        self.jack_up_pub = self.create_publisher(UInt8MultiArray, "jack_up_status", 10)
        self.jack_up_exit_pub = self.create_publisher(UInt8MultiArray, "jack_up_exit_status", 10)
        self.get_logger().info("Publishers are created")

        # Subscriptions
        self.plc_conn_sub = self.create_subscription(Bool, "plc_connection", self.plc_conn_cb, 10, callback_group=sub_cbg)
        self.rel_mtrl_box_sub = self.create_subscription(Bool, "releasing_material_box", self.releasing_mtrl_box_cb, 10, callback_group=plc_sub_cbg)
        self.sliding_pfm_curr_sub = self.create_subscription(UInt8MultiArray, "sliding_platform_curr", self.sliding_pfm_curr_cb, 10, callback_group=plc_sub_cbg)
        self.sliding_pfm_cmd_sub = self.create_subscription(UInt8MultiArray, "sliding_platform_cmd", self.sliding_pfm_cmd_cb, 10, callback_group=plc_sub_cbg)
        self.sliding_pfm_ready_sub = self.create_subscription(UInt8MultiArray, "sliding_platform_ready", self.sliding_pfm_ready_cb, 10, callback_group=plc_sub_cbg)
        self.elevator_sub = self.create_subscription(Bool, "elevator", self.elevator_cb, 10, callback_group=plc_sub_cbg)
        self.qr_scan_sub = self.create_subscription(CameraTrigger, "qr_camera_scan", self.qr_scan_cb, 10, callback_group=sub_cbg)
        self.pkg_mac_status_sub = self.create_subscription(PackagingMachineStatus, "packaging_machine_status", self.pkg_mac_status_cb, 10, callback_group=sub_cbg)
        self.vision_block_sub = self.create_subscription(Bool, "vision_block", self.vision_block_cb, 10, callback_group=plc_sub_cbg)
        self.con_mtrl_box_sub = self.create_subscription(UInt8, "container_material_box", self.con_mtrl_box_cb, 10, callback_group=plc_sub_cbg)
        self.loc_sensor_sub = self.create_subscription(UInt8MultiArray, "location_sensor", self.loc_sensor_cb, 10, callback_group=plc_sub_cbg)
        self.get_logger().info("Subscriptions are created")
        
        # Service servers
        self.new_order_srv = self.create_service(NewOrder, "new_order", self.new_order_cb, callback_group=srv_ser_cbg)
        self.move_sliding_pfm = self.create_service(MoveSlidingPlatform, "move_sliding_platform", self.move_sliding_platform_cb, callback_group=srv_ser_cbg)
        self.get_logger().info("Service Servers are created")
        
        # Service clients
        self.read_cli = self.create_client(ReadRegister, "read_register", callback_group=srv_cli_cbg)
        self.write_cli = self.create_client(WriteRegister, "write_register", callback_group=srv_cli_cbg)
        self.in_mtrl_box_cli = self.create_client(UInt8Srv, "income_material_box", callback_group=srv_cli_cbg)
        self.rel_blocking_cli = self.create_client(Trigger, "release_blocking", callback_group=srv_cli_cbg)
        self.pkg_order_cli = self.create_client(PackagingOrder, "packaging_order", callback_group=srv_cli_cbg)
        self.dis_station_clis: Dict[int, rclpy.client.Client] = dict() # station_id, Client
        for i in range(1, self.const.NUM_DISPENSER_STATIONS + 1):
            cbg_index = int(i - 1)
            self.dis_station_clis[i] = self.create_client(
                DispenseDrug, 
                f"/dispenser_station_{i}/dispense_request",
                callback_group=station_srv_cli_cbgs[cbg_index]
            )
            self.get_logger().debug(f"Station [{i}] service client is created")
        self.get_logger().info("Service Clients are created")

        # Timers
        self.once_timer = self.create_timer(3.0, self.once_timer_cb, callback_group=normal_timer_cbg)
        self.qr_handle_timer = self.create_timer(1.0, self.qr_handle_cb, callback_group=qr_handle_timer_cbg)
        self.start_order_timer = self.create_timer(1.0, self.start_order_cb, callback_group=order_timer_cbg)
        self.order_status_timer = self.create_timer(1.0, self.order_status_cb, callback_group=normal_timer_cbg)
        self.elevator_dequeue_timer = self.create_timer(1.0, self.elevator_dequeue_cb, callback_group=elevator_timer_cbg)
        self.jack_up_timer = self.create_timer(1.0, self.jack_up_cb, callback_group=jack_up_timer_cbg)
        self.occupancy_status_timer = self.create_timer(1.0, self.occupancy_status_cb, callback_group=occupancy_timer_cbg)
        self.release_cleaning_timer = self.create_timer(1.0, self.release_cleaning_cb, callback_group=normal_timer_cbg)
        self.release_vision_timer = self.create_timer(1.0, self.release_vision_cb, callback_group=normal_timer_cbg)
        self.clear_conveyor_occupancy_timer = self.create_timer(0.1, self.clear_conveyor_occupancy_cb, callback_group=clear_timer_cbg)
        self.dis_station_timers: Dict[int, rclpy.Timer.Timer] = dict()
        for i in range(1, self.const.NUM_DISPENSER_STATIONS + 1):
            cbg_index = int(i - 1)
            self.dis_station_timers[i] = self.create_timer(
                1.0,
                lambda station_id=i: self.station_decision_cb(station_id),
                callback_group=station_timer_cbgs[cbg_index]
            )
            self.get_logger().debug(f"Station [{i}] timer is created")
        self.get_logger().info("Timers are created")

        # Action clients
        self.visual_action_cli = ActionClient(
            self, 
            VisualDetection, 
            "visual_detection",
            callback_group=action_cli_cbg
        )
        self.get_logger().info("Action clients are created")

        self.get_logger().info("Produation Line Core System Node is started")
    
    def once_timer_cb(self) -> None:
        self.get_logger().info(f"once_timer callback is started")

        # block_success = self.send_income_mtrl_box(1)
        # if block_success:
        #     self.get_logger().info(f"Sent a income materail box to packaging machine manager successfully")
        # else:
        #     self.get_logger().error(f"Failed to send the income materail box to packaging machine manager")

        # success = self.send_rel_blocking()
        # if success:
        #     self.get_logger().info(f"Sent a release blocking request successfully")
        # else:
        #     self.get_logger().error(f"Error in Sending a release blocking request")
        
        self.once_timer.cancel()
        self.get_logger().info(f"once_timer is cancelled")

    # Subscription callbacks
    def plc_conn_cb(self, msg: Bool) -> None:
        with self.mutex:
            self.is_plc_connected = msg.data
        if not msg.data:
            self.get_logger().error(f"Received: PLC is disconnected")

    def releasing_mtrl_box_cb(self, msg: Bool) -> None:
        """
        Handle material box release status updates.
        
        Updates the release state.
        """
        new_state = bool(msg.data)
        
        with self.mutex:
            prev_state = self.is_releasing_mtrl_box
            self.is_releasing_mtrl_box = new_state
            
        if new_state and not prev_state:
            self.get_logger().info(f"The container is releasing material box")
        
        if new_state != prev_state:
            self.get_logger().warning(f"Release state changed: {prev_state} -> {new_state}")

    def sliding_pfm_curr_cb(self, msg: UInt8MultiArray) -> None:
        """
        Update current sliding platform positions for all dispenser stations.
        
        Args:
            msg: UInt8MultiArray message containing platform locations for each station
        """
        if len(msg.data) != self.const.NUM_DISPENSER_STATIONS:
            self.get_logger().error(f"Message length {len(msg.data)} doesn't match stations {len(self.dis_station)}")
            return
        
        updated_stations = []
        
        for station_id, platform_location in enumerate(msg.data, start=1):
            station = self.conveyor.get_station(station_id)
            if station is None:
                self.get_logger().warning(f"No station found for ID {station_id} <<<< 1")
                continue

            with self.mutex:
                station.curr_sliding_platform = platform_location

            updated_stations.append((station_id, platform_location))

        if updated_stations:
            self.get_logger().debug(f"Updated {len(updated_stations)} sliding platforms location: {dict(updated_stations)}")
        else:
            self.get_logger().error("No stations updated from platform message")

    def sliding_pfm_cmd_cb(self, msg: UInt8MultiArray) -> None:
        """
        Update command sliding platform positions for all dispenser stations.
        
        Args:
            msg: UInt8MultiArray message containing platform locations for each station
        """
        if len(msg.data) != self.const.NUM_DISPENSER_STATIONS:
            self.get_logger().error(f"Message length {len(msg.data)} doesn't match stations {len(self.dis_station)}")
            return
        
        updated_stations = []
        
        for station_id, state in enumerate(msg.data, start=1):
            station = self.conveyor.get_station(station_id)
            if station is None:
                self.get_logger().warning(f"No station found for ID {station_id} <<<< 2")
                continue

            with self.mutex:
                station.cmd_sliding_platform = state

            updated_stations.append((station_id, state))

        if updated_stations:
            self.get_logger().debug(f"Updated {len(updated_stations)} sliding platforms command: {dict(updated_stations)}")
        else:
            self.get_logger().error("No stations updated from platform message")

    def sliding_pfm_ready_cb(self, msg: UInt8MultiArray) -> None:
        """
        Update sliding platform ready status for all dispenser stations.
        
        Args:
            msg: UInt8MultiArray message containing platform ready status for each station
        """
        if len(msg.data) != self.const.NUM_DISPENSER_STATIONS:
            self.get_logger().error(f"Message length {len(msg.data)} doesn't match stations {len(self.dis_station)}")
            return
        
        updated_stations = []
        
        for station_id, platform_ready_state in enumerate(msg.data, start=1):
            station = self.conveyor.get_station(station_id)
            if station is None:
                self.get_logger().warning(f"No station found for ID {station_id} <<<< 3")
                continue

            with self.mutex:
                station.is_platform_ready = platform_ready_state

            updated_stations.append((station_id, platform_ready_state))

            if platform_ready_state != 0:
                self.get_logger().debug(f"Station [{station_id}] platform is ready!")
        
        if updated_stations:
            self.get_logger().debug(f"Updated {len(updated_stations)} sliding platforms ready state: {dict(updated_stations)}")
        else:
            self.get_logger().error("No stations updated from platform message")

        self.get_logger().debug(f"Received sliding platform ready message: {msg}")
    
    def elevator_cb(self, msg: Bool) -> None:
        """
        Handle elevator status update messages.
        
        Updates elevator ready state and manages queue when state changes from not ready to ready.
        
        Args:
            msg: Bool message containing elevator ready status
        """
        timestamp = self.get_clock().now()

        with self.elevator_mutex:
            prev_elevator_ready = self.is_elevator_ready
            self.is_elevator_ready = msg.data

        if msg.data and not prev_elevator_ready: # state transition (0 -> 1)
            with self.elevator_mutex:         
                self.elevator_ready.append(timestamp)
                self.get_logger().info(f"appended to elevator ready queue [{timestamp}]")
            
                if self.elevator_request:
                    mtrl_box_id, append_time = self.elevator_request.popleft()
                    self.get_logger().info(f"popped out from elevator request [{mtrl_box_id}] w/ {append_time}")

            self.get_logger().info(f"Elevator state changed 0 -> 1 at {timestamp}")
        
        elif not msg.data and prev_elevator_ready: # state transition (1 -> 0)
            with self.mutex:
                if self.is_mtrl_box_storing:
                    self.is_mtrl_box_storing = False
                    self.get_logger().info(f"Reset is_mtrl_box_storing")
                    
            self.get_logger().info(f"Elevator state changed 1 -> 0 at {timestamp}")

    def qr_scan_cb(self, msg: CameraTrigger) -> None:
        if not (self.const.CAMERA_ID_START <= msg.camera_id <= self.const.CAMERA_ID_END):
            self.get_logger().error(f"Received undefined camera_id: {msg.camera_id}")
            return
        if not (self.const.MTRL_BOX_ID_START <= msg.material_box_id <= self.const.MTRL_BOX_ID_END):
            self.get_logger().error(f"Received undefined material_box_id: {msg.material_box_id}")
            return

        camera_id = msg.camera_id
        mtrl_box_id = msg.material_box_id

        if camera_id in range(self.const.CAMERA_ID_START, self.const.CAMERA_ID_VISION):
            if conveyor_seg := self.conveyor.get_conveyor(camera_id):
                with self.occupy_mutex:
                    success = conveyor_seg.force_occupy(mtrl_box_id)
                    if success:
                        self.get_logger().warning(f">>>>> Conveyor Occupied: {conveyor_seg.id} by material box: {mtrl_box_id}")
                    else:
                        self.get_logger().error(f">>>>> Conveyor {camera_id} Occupy failed in qr_scan_cb!")
                        return

        curr_time = self.get_clock().now()
        
        with self.qr_scan_mutex:
            len_befoce = len(self.qr_scan_history)

            self.qr_scan_history = deque(
                (scan, scan_time) for scan, scan_time in self.qr_scan_history
                if (curr_time - scan_time).nanoseconds / 1e9 <= self.const.MAX_HISTORY_AGE_SEC
            )

            len_after = len(self.qr_scan_history)

            found = any(
                scan.camera_id == msg.camera_id and scan.material_box_id == msg.material_box_id
                for scan, _ in self.qr_scan_history
            )
            
            if not found:
                self.qr_scan_history.append((msg, self.get_clock().now()))
                self.qr_scan.append(msg)
                self.get_logger().info(f"Camera {msg.camera_id} box [{msg.material_box_id}] is appended to qr_scan")

        diff = len_befoce - len_after
        if diff > 0:
            self.get_logger().info(f"Number of camera history scan is removed: {diff} ")

        self.get_logger().debug(f"Received CameraTrigger message: camera_id={msg.camera_id}, material_box_id={msg.material_box_id}")

    def pkg_mac_status_cb(self, msg: PackagingMachineStatus) -> None:
        """
        Handle packaging machine status updates.
        
        Updates the status dictionary for valid packaging machine IDs (1 or 2).
        
        Args:
            msg: PackagingMachineStatus message containing machine status
        """
        machine_id = msg.packaging_machine_id

        if machine_id not in (self.const.PKG_MAC_ID_START, self.const.PKG_MAC_ID_END):
            self.get_logger().error(f"Ignoring status update for unknown machine ID {machine_id}")
            return
        
        with self.mutex:
            self.pkg_mac_status[machine_id] = msg

    def vision_block_cb(self, msg: Bool) -> None:
        """
        Handle vision block update messages.
        
        Updates vision block state and manages queue when state changes from not ready to ready.
        
        Args:
            msg: Bool message containing vision block status
        """
        timestamp = self.get_clock().now()

        with self.vision_mutex:
            prev_vision_block_state = self.is_vision_block_released
            self.is_vision_block_released = msg.data

            if msg.data and not prev_vision_block_state: # Check for state transition (0 -> 1)
                if self.vision_request:
                    success = self.send_vision_req(self.vision_request.popleft())
                    if success:
                        self.get_logger().info(f"Sent the vision request successfully")
                    else:
                        self.get_logger().info(f"Failed to send the vision request")

                self.get_logger().info(f"Vision block state changed 0 -> 1 at {timestamp}. Queue size: {len(self.vision_request)}")

            elif not msg.data and prev_vision_block_state: # Check for state transition (1 -> 0)
                self.get_logger().debug(f"Vision block state changed 1 -> 0. Queue size: {len(self.vision_request)}")

    def con_mtrl_box_cb(self, msg: UInt8) -> None:
        if not (0 <= msg.data <= self.const.MAX_MTRL_BOX_IN_CON):
            self.get_logger().error(f"Invalid number of material box in container: {msg.data}")
            return

        with self.mutex:
            self.num_of_mtrl_box_in_container = msg.data
        self.get_logger().debug(f"number of material box in container: {msg.data}")

    def loc_sensor_cb(self, msg: UInt8MultiArray) -> None:
        if len(msg.data) != self.plc_const.SENSOR_LENGTH:
            self.get_logger().error(f"Sensor length {len(msg.data)} doesn't match stations {self.plc_const.SENSOR_LENGTH}")
            return
        
        sensor_data = msg.data

        station_status = {
            station_id: all(sensor_data[i] == 0 for i in indices)
            for station_id, indices in self.plc_const.STATION_SENSOR.items()
        }
        conveyor_status = {
            conveyor_id: all(sensor_data[i] == 0 for i in indices)
            for conveyor_id, indices in self.plc_const.CONVEYOR_SENSOR.items()
        }

        with self.occupy_mutex:
            for station_id, status in station_status.items():
                if station := self.conveyor.get_station(station_id):
                    station.is_free = status

            for conveyor_id, status in conveyor_status.items():
                if conveyor := self.conveyor.get_conveyor(conveyor_id):
                    conveyor.is_free = status

        self.get_logger().debug(f"station_status: {station_status}")
        self.get_logger().debug(f"conveyor_status: {conveyor_status}")
        
        with self.mutex:
            prev_cleaning_state = self.cleaning_state
            self.cleaning_state = sensor_data[self.plc_const.CLEANING_LOC_INDEX]

            if sensor_data[self.plc_const.CLEANING_LOC_INDEX] and not prev_cleaning_state:
                self.release_cleaning = True
                self.get_logger().info(f"A material box is passing throught cleaning machine")

            prev_vision_state = self.vision_state
            self.vision_state = sensor_data[self.plc_const.VISION_LOC_INDEX]

            if sensor_data[self.plc_const.VISION_LOC_INDEX] and not prev_vision_state:
                self.release_vision = True
                self.get_logger().info(f"A material box is passing throught vision inspection")

        with self.jack_up_mutex:
            self.jack_up_status = {
                pt: bool(sensor_data[idx])
                for pt, idx in self.plc_const.JACK_UP_POINT_INDEX.items()
            }
            self.jack_up_exit_status = {
                pt: bool(sensor_data[idx])
                for pt, idx in self.plc_const.JACK_UP_EXIT_INDEX.items()
            }

        self.get_logger().debug(f"jack_up_status: {self.jack_up_status}")
        self.get_logger().debug(f"jack_up_exit_status: {self.jack_up_exit_status}")

    # Timers callback
    def start_order_cb(self) -> None:
        with self.mutex:
            if not self.recv_order:
                self.get_logger().debug("No order received")
                return
            if not self.is_plc_connected:
                self.get_logger().debug("PLC is disconnected")
                return 
            if self.is_releasing_mtrl_box:
                self.get_logger().info("The container is releasing box")
                return
            if self.num_of_mtrl_box_in_container == 0:
                self.get_logger().error(f"The material box is zero in container: {self.num_of_mtrl_box_in_container}")
                return
            
        with self.elevator_mutex:
            if self.elevator_request:
                self.get_logger().warning("Wait for the material box retrieval action complete")
                return

        with self.occupy_mutex:
            if conveyor_seg := self.conveyor.get_conveyor(self.const.CAMERA_ID_START): # first conveyor segment
                if not conveyor_seg.available():
                    self.get_logger().error("The first conveyor is unavailable")
                    return

        self.get_logger().info("The received queue stored a order")

        success = self._execute_movement(self.plc_const.TRANSFER_MTRL_BOX_ADDR, self.plc_const.MTRL_BOX_RELEASE_PLC_VALUES)
        if success:
            with self.mutex:
                highest_priority_order = max(self.recv_order, key=lambda _order: _order.priority)
                self.proc_order[highest_priority_order.order_id] = deepcopy(highest_priority_order)
                status = self.mtrl_box_status.get(highest_priority_order.order_id)
                self.recv_order.remove(highest_priority_order)
                if status:
                    status.start_time = self.get_clock().now().to_msg()
            self.get_logger().info(">>> Added a order to processing queue")
            self.get_logger().info(">>> Removed a order from received queue")
        else:
            self.get_logger().error("Failed to call the PLC to release a material box")
            with self.occupy_mutex:
                if conveyor_seg.is_occupied:
                    conveyor_seg.clear()
                    self.get_logger().error(f"Cleared conveyor id: {conveyor_seg.id}")
    
    def order_status_cb(self) -> None:
        """
        Publish material box status messages.
        """
        with self.mutex:
            for status in self.mtrl_box_status.values():
                if status.id <= 0:
                    self.get_logger().debug(f"Skipped status with invalid ID 0")
                    continue
                status.header.stamp = self.get_clock().now().to_msg()
                self.mtrl_box_status_pub.publish(status)
                self.get_logger().debug(f"Published status for material box ID {status.id}")

    def elevator_dequeue_cb(self) -> None:
        # with self.mutex:
        #     if self.is_releasing_mtrl_box:
        #         self.get_logger().debug(f"container is releasing material box in elevator_dequeue_cb")
        #         return
        with self.elevator_mutex:
            if not self.is_elevator_ready:
                self.get_logger().debug(f"elevator is not ready")
                return
            if not self.elevator_ready:
                self.get_logger().debug(f"elevator ready queue is empty")
                return

        with self.mutex:
            flag = self.is_mtrl_box_coming

        if flag:
            success = self.send_rel_blocking()
            if success:
                with self.mutex:
                    self.is_mtrl_box_coming = False
                self.get_logger().info(f"Reset is_mtrl_box_coming")
            else:
                self.get_logger().error(f"Error in Sending a release blocking request")
        
    def jack_up_cb(self) -> None:
        """
        Process the jack-up queue and make movement decisions for material boxes.
        Handles moving items to opposite stations or the next conveyor based on decision logic.
        """
        with self.jack_up_mutex:
            if not self.jack_up_opposite_queue:
                self.get_logger().debug(f"jack-up queue is empty")
                return

        self.get_logger().warning(f"Started decision-making at jack-up point: {len(self.jack_up_opposite_queue)}")
        to_remove = []

        with self.jack_up_mutex:
            jack_up_opposite_queue_copy = list(self.jack_up_opposite_queue)

        for jack_up_pt, order_id, source_station_id, mtrl_box_id in jack_up_opposite_queue_copy:
            with self.jack_up_mutex:
                if not self.jack_up_status.get(jack_up_pt):
                    self.get_logger().warning(f"jack_up_pt: {jack_up_pt} does not ready")
                    continue

            opposite = self.plc_const.STATION_OPPOSITE.get(source_station_id, [])
            if not opposite:
                self.get_logger().warning(f"order_id: {order_id} should be leave")
                self._handle_opposite_exit_movement(order_id, source_station_id, mtrl_box_id, to_remove, jack_up_pt)
                continue
            
            decided_station_id = 0
            with self.occupy_mutex:
                match self.const.MOVEMENT_VERSION:
                    case 1:
                        decided_station_id = self.movement_decision_v1(order_id, opposite)
                    case 2:
                        decided_station_id = self.movement_decision_v2(order_id, opposite)
            
                available_station = self.remove_unavailable_station([decided_station_id])
                decided_station_id = available_station.pop() if available_station else 0

            if decided_station_id in opposite:
                self.get_logger().warning(f"order_id: {order_id} should be move to opposite station")
                self._handle_opposite_movement(order_id, source_station_id, mtrl_box_id, decided_station_id, to_remove, jack_up_pt)
            else:
                self.get_logger().warning(f"order_id: {order_id} should be leave because opposite station is unavailable or does not required")
                self._handle_opposite_exit_movement(order_id, source_station_id, mtrl_box_id, to_remove, jack_up_pt)
            
            if jack_up_pt == self.const.CAMERA_ID_SPLIT_END:
                remainder = set()

                match self.const.MOVEMENT_VERSION:
                    case 1:
                        remainder = self.find_remainder(order_id)
                    case 2:
                        remainder = self.find_mini_remainder(order_id)

                self.get_logger().info(f"In jack-up point 8 remainder: {remainder}")
                if remainder is not None and len(remainder) == 0:
                    if status := self.mtrl_box_status.get(order_id):
                        status.status = MaterialBoxStatus.STATUS_DISPENSED
                        # FIXME
                        self.get_logger().warning(f"updated status to STATUS_DISPENSED w/ box [FIXME]") # {material_box_id}

        with self.jack_up_mutex:
            for item in to_remove:
                self.jack_up_opposite_queue.remove(item)
                self.get_logger().warning(f"Removed form jack-up queue: {item}")

        self.get_logger().debug(f"Handled number of jack-up point: {len(to_remove)}")

    def occupancy_status_cb(self) -> None:
        """
        Publishes occupancy status for conveyor segments and associated stations.
        
        Publishes:
            conveyor_occupancy_pub: UInt8MultiArray of conveyor segment occupancy
            station_occupancy_pub: UInt8MultiArray of station occupancy sorted by ID
        """
        conveyor_msg: UInt8MultiArray = UInt8MultiArray()
        station_msg: UInt8MultiArray = UInt8MultiArray()
        jack_up_status_msg: UInt8MultiArray = UInt8MultiArray()
        jack_up_exit_status_msg: UInt8MultiArray = UInt8MultiArray()

        with self.jack_up_mutex:
            jack_up_status_msg.data = [state for state in self.jack_up_status.values()]
            jack_up_exit_status_msg.data = [state for state in self.jack_up_exit_status.values()]

        curr_conveyor = self.conveyor.head

        with self.occupy_mutex:
            while curr_conveyor:
                conveyor_msg.data.append(curr_conveyor.is_occupied)
                self.get_logger().debug(f"Added conveyor segment {curr_conveyor.id}: {curr_conveyor.is_occupied}")

                stations = []
                if curr_conveyor.l_station:
                    stations.append(curr_conveyor.l_station)
                if curr_conveyor.r_station:
                    stations.append(curr_conveyor.r_station)
                    
                for station in sorted(stations, key=lambda x: x.id):
                    station_msg.data.append(station.is_occupied)
                    self.get_logger().debug(f"Added station {station.id}: {station.is_occupied}")

                curr_conveyor = curr_conveyor.next
        
        self.conveyor_occupancy_pub.publish(conveyor_msg)
        self.station_occupancy_pub.publish(station_msg)
        self.jack_up_pub.publish(jack_up_status_msg)
        self.jack_up_exit_pub.publish(jack_up_exit_status_msg)

        self.get_logger().debug(f"Conveyor occupancy data: {conveyor_msg.data}")
        self.get_logger().debug(f"Station occupancy data: {station_msg.data}")
        self.get_logger().debug(f"Jack-up status data: {jack_up_status_msg.data}")
        self.get_logger().debug(f"Jack-up exit status data: {jack_up_exit_status_msg.data}")
    
    def release_cleaning_cb(self) -> None:
        """
        Release the cleaning machine block if conditions are met.
        """
        self.get_logger().debug(f"Started to release the cleaning machine")

        with self.mutex:
            if not self.release_cleaning:
                self.get_logger().debug("Cleaning machine release not required")
                return
            
        success = self._execute_movement(self.plc_const.RELEASE_CLEANING_ADDR, self.plc_const.RELEASE_VALUE)
        if success:
            with self.mutex:
                self.release_cleaning = False
            self.get_logger().debug(f"release the cleaning machine")
        else:
            self.get_logger().error(f"write_registers movement failed")

    def release_vision_cb(self) -> None:
        """
        Release the vision inspection block if conditions are met.
        """
        self.get_logger().debug(f"Started to release the vision inspection")

        with self.mutex:
            if not self.release_vision:
                self.get_logger().debug("Vision release not required")
                return
            
        pkg_1_conveyor = self.conveyor.get_conveyor(self.const.CAMERA_ID_PKG_MAC_1)
        pkg_2_conveyor = self.conveyor.get_conveyor(self.const.CAMERA_ID_PKG_MAC_2)

        if not pkg_1_conveyor or not pkg_2_conveyor:
            self.get_logger().error("One or both conveyors not found")
            return
        
        if not pkg_1_conveyor.available() or not pkg_2_conveyor.available():
            self.get_logger().debug("One or both conveyors not available")
            return
        
        success = self._execute_movement(self.plc_const.RELEASE_VISION_ADDR, self.plc_const.RELEASE_VALUE)
        if success:
            with self.mutex:
                self.release_vision = False
            self.get_logger().debug(f"Release the vision block")
        else:
            self.get_logger().error(f"write_registers movement failed")

    def clear_conveyor_occupancy_cb(self) -> None:
        # self.get_logger().debug(f"Started to clear conveyor occupancy")

        # with self.jack_up_mutex:
        #     for conveyor_id, state in self.jack_up_exit_status.items():
        #         if state:
        #             conveyor = self.conveyor.get_conveyor(conveyor_id)
        #             with self.occupy_mutex:
        #                 if conveyor and conveyor.is_occupied:
        #                     conveyor.clear()
        #                     self.get_logger().error(f"Clear conveyor id: {conveyor_id}")
        """
        Clear occupancy of conveyors in the jack-up exit state.
        """
        self.get_logger().debug("Started clearing conveyor occupancy")
        cleared_count = 0
        conveyors_to_clear = []

        with self.jack_up_mutex:
            for conveyor_id, state in self.jack_up_exit_status.items():
                if state:
                    conveyor = self.conveyor.get_conveyor(conveyor_id)
                    if not conveyor:
                        self.get_logger().warning(f"No conveyor found for ID: {conveyor_id}")
                        continue
                    if conveyor.is_occupied:
                        conveyors_to_clear.append((conveyor_id, conveyor))

        with self.occupy_mutex:
            for conveyor_id, conveyor in conveyors_to_clear:
                try:
                    conveyor.clear()
                    self.get_logger().info(f"Cleared conveyor ID: {conveyor_id}")
                    cleared_count += 1
                except Exception as e:
                    self.get_logger().error(f"Failed to clear conveyor ID {conveyor_id}: {e}")

        self.get_logger().debug(f"Cleared {cleared_count} conveyors")

    def _handle_opposite_exit_movement(self, order_id: int, source_station_id: int, mtrl_box_id: int, to_remove: list, jack_up_pt: int) -> None:
        self.get_logger().info(f"handle exit movement!!!")

        next_conveyor = self.conveyor.get_next_conveyor_by_station(source_station_id)

        with self.occupy_mutex:
            self.get_logger().info(f"occupy_mutex locked!!!")
            if next_conveyor and not next_conveyor.available():
                self.get_logger().error(f"The next conveyor is unavailable. [free: {next_conveyor.is_free}, occupied: {next_conveyor.is_occupied}]")
                self.get_logger().error(f"It will try to leave in next callback.")
                return

            self.get_logger().info(f"The next conveyor is available")
            success = next_conveyor.occupy(mtrl_box_id)
            if success:
                self.get_logger().warning(f">>>>> Conveyor Occupied: {next_conveyor.id} by material box: {mtrl_box_id}")
            else:
                self.get_logger().error(f">>>>> Conveyor {next_conveyor.id} Occupy failed! It will try to occupy in next callback.")
                return
            
            success = self._execute_movement(self.plc_const.GO_OPPOSITE_ADDR + source_station_id, self.plc_const.EXIT_JACK_UP_VALUE)
            if success:
                tmp = (jack_up_pt, order_id, source_station_id, mtrl_box_id)
                to_remove.append(tmp)
                self.get_logger().warning(f"added to to_remove: {tmp}")
            else:
                self.get_logger().error(f"Failed to write to register {self.plc_const.GO_OPPOSITE_ADDR + source_station_id}")

    def _handle_opposite_movement(self, order_id: int, source_station_id: int, mtrl_box_id: int, decided_station_id: int, to_remove: list, jack_up_pt: int) -> None:
        self.get_logger().info(f"handle opposite movement!!!")
        
        target_station = self.conveyor.get_station(decided_station_id)

        with self.occupy_mutex:
            self.get_logger().info(f"occupy_mutex locked!!!")
            if target_station and not target_station.available():
                self.get_logger().error(f"Station [{target_station.id}] is unavailable. [free: {target_station.is_free}, occupied: {target_station.is_occupied}]")
                self.get_logger().error(f"It will try to leave in next callback.")
                return 
            
            self.get_logger().info(f"target_station available")
            success = target_station.occupy(mtrl_box_id)
            if success:
                self.get_logger().warning(f">>>>> Station Occupied: {target_station.id} by material box: {mtrl_box_id}")
            else:
                self.get_logger().error(f">>>>> Station {target_station.id} Occupy failed!")

            success = self._execute_movement(self.plc_const.GO_OPPOSITE_ADDR + source_station_id, self.plc_const.MOVE_OPPOSITE_VALUE)
            if success:
                tmp = (jack_up_pt, order_id, source_station_id, mtrl_box_id)
                to_remove.append(tmp)
                self.get_logger().warning(f"appeded to to_remove: {tmp}")
            else:
                self.get_logger().error(f"Failed to write to register {self.plc_const.GO_OPPOSITE_ADDR + source_station_id}")
    
    def qr_handle_cb(self) -> None:
        with self.qr_scan_mutex:
            self.get_logger().debug(f"len(self.qr_scan): {len(self.qr_scan)}")
            if not self.qr_scan:
                return
        
        self.get_logger().info(f"Started to handle the QR scan, Total: {len(self.qr_scan)}")
        qr_scan_handled = []

        with self.qr_scan_mutex:
            qr_scan_copy = list(self.qr_scan)

        for msg in qr_scan_copy:
            is_completed = False
            camera_id = msg.camera_id
            mtrl_box_id = msg.material_box_id
            self.get_logger().info(f"camera id: {camera_id}, material box id: {mtrl_box_id}")

            order_id = self.get_order_id_by_mtrl_box(mtrl_box_id)
            if order_id is None:
                self.get_logger().info(f"The material box [{mtrl_box_id}] does not bound to any order!")

            self.get_logger().info(f"Start to handle camera: {camera_id}")
            match camera_id:
                case 1: # QR camera
                    camera_1_result = self.camera_1_action(order_id, mtrl_box_id)
                    order_id = self.get_order_id_by_mtrl_box(mtrl_box_id)
                    if camera_1_result:
                        continue
                    is_completed = self.camera_1_to_8_action(order_id, mtrl_box_id, camera_id) 
                case 2 | 3 | 4 | 5 | 6 | 7 | 8: # QR camera
                    if order_id is None:
                        self.get_logger().info(f"The scan maybe incorrect cameras: {msg.camera_id}, box: {msg.material_box_id} ")
                        is_completed = True
                    else:
                        is_completed = self.camera_1_to_8_action(order_id, mtrl_box_id, camera_id) 
                case 9: # vision inspection
                    is_completed = self.camera_9_action(order_id, mtrl_box_id, camera_id)  
                case 10: # packaging machine 1
                    is_completed = self.camera_10_action(order_id, mtrl_box_id, camera_id)      
                case 11: # packaging machine 2
                    is_completed = self.camera_11_action(order_id, mtrl_box_id, camera_id)      
                case _:
                    self.get_logger().info(f"Received unknown camera ID: {camera_id}")
                
            if is_completed:
                qr_scan_handled.append(msg)
            
        with self.qr_scan_mutex:
            for msg in qr_scan_handled:
                self.qr_scan.remove(msg)
                self.get_logger().info(f"Removed message camera id: {msg.camera_id}, box: {msg.material_box_id} ")

        self.get_logger().info(f"Completed to handle the QR scan")

    def station_decision_cb(self, station_id: int) -> None:
        self.get_logger().debug(f"Dispenser Station [{station_id}] callback is called")

        station = self.conveyor.get_station(station_id)
        if station is None:
            self.get_logger().warning(f"No station found with ID: {station_id}")
            return
        with self.occupy_mutex:
            if not station.is_occupied:
                self.get_logger().debug(f"Station {station_id} is not accupied")
                return
            if station.curr_mtrl_box == 0:
                self.get_logger().warning(f"Station {station_id} has no material box")
                return
        with self.mutex:  
            if not station.is_platform_ready:
                self.get_logger().debug(f"Station {station_id} sliding platform is not ready")
                return

        with self.mutex:
            mtrl_box_id = station.curr_mtrl_box
            order_id = self.get_order_id_by_mtrl_box(mtrl_box_id)
            order = self.proc_order.get(order_id)
            status = self.mtrl_box_status.get(order_id)        
            curr_sliding_platform = station.curr_sliding_platform
            cmd_sliding_platform = station.cmd_sliding_platform

        if not status:
            self.get_logger().error(f"No material status found with order id: {order_id}")
            return
        
        with self.mutex:
            status.location = f"station_{station_id}"
        
        filtered_missing, target_cell = self.find_target_cell(station, station_id, status, order)
        self.get_logger().debug(f"target_cell: {target_cell}")

        if not station.is_cleared_up_conveyor:
            conveyor = self.conveyor.get_conveyor_by_station(station_id)
            with self.occupy_mutex:
                conveyor.clear()
            station.is_cleared_up_conveyor = True
            self.get_logger().error(f"The conveyor [{conveyor.id}] of station {station_id} is cleared up")

        if target_cell >= self.plc_const.EXIT_STATION:
            self.get_logger().error(f"The station {station_id} is completed dispense for the material box {mtrl_box_id}")
            success = self.move_out_station(station, station_id, mtrl_box_id)
            if success:
                with self.jack_up_mutex:
                    self.jack_up_opposite_queue.append((self.plc_const.JACK_UP_POINT[station_id], order_id, station_id, mtrl_box_id))
                self.get_logger().warning(f"Appended {(order_id, station_id, mtrl_box_id)} to jack-up queue")
            else:
                self.get_logger().warning(f"move_out_station failed")

        elif cmd_sliding_platform == 0 or \
             cmd_sliding_platform != curr_sliding_platform or \
             cmd_sliding_platform != target_cell + 1:
            self.get_logger().debug(f"station: {station_id} is sending the movement command")
            if target_cell + 1 != self.plc_const.EXIT_STATION:
                success = self._execute_movement(self.plc_const.MOVEMENT_ADDR + station_id, [target_cell + 1])
                if success:
                    self.get_logger().debug(f"station: {station_id} is moving to {target_cell + 1} cell")
                else:
                    self.get_logger().error(f"write_registers movement failed")
            else:
                self.get_logger().error(f"All cells are completed dispension. Waiting for leaving the station [{station_id}]")

        elif station.get_dispense_req_sent(target_cell):
            self.get_logger().debug(f"The station {[station_id]} dispense request are already sent! skip!")
            
        elif filtered_missing:
            self.get_logger().debug(f"try to dispense")
            # success = self.dispense_action(station, station_id, filtered_missing, order, target_cell)
            success = self.send_dispense_req(filtered_missing, station_id, target_cell, order.order_id)
            if success:
                with self.mutex:
                    station.set_dispense_req_sent(target_cell, True)

        self.get_logger().debug(f"The station {[station_id]} decision are made!!")

    # Service Server callbacks
    def new_order_cb(self, req, res):
        order_id = req.request.order_id
        priority = req.request.priority
        
        if not isinstance(order_id, int) or not isinstance(priority, int):
            self.get_logger().error("Invalid types")
            return res
        if order_id < 0 or priority < 0:
            self.get_logger().error("Invalid number")
            return res
        
        self.get_logger().info(f"Received NewOrder request: order_id={order_id}, priority={priority}")

        msg = MaterialBoxStatus(
            creation_time=self.get_clock().now().to_msg(),
            id=0,
            status=MaterialBoxStatus.STATUS_INITIALIZING,
            priority=priority,
            order_id=order_id
        )

        with self.mutex:
            self.mtrl_box_status[req.request.order_id] = msg
            self.recv_order.append(req.request)

        res.response.success = True
        self.get_logger().debug(f"Successfully processed order {order_id}")
        return res
    
    def move_sliding_platform_cb(self, req, res):
        """
        Move the sliding platform to a specified cell for a dispense station.
        
        Args:
            req: Request object containing cell_no and dispense_station_id
            res: Response object to be populated with results
            
        Returns:
            Response: Updated response with success status and message
        """
        cell_no = req.cell_no
        station_id = req.dispense_station_id
        
        if not (1 <= cell_no <= self.plc_const.EXIT_STATION):
            self.get_logger().error(f"Invalid cell_no {cell_no}: must be between 1 and 29")
            res.message = f"Cell number {cell_no} out of range (1-29)"
            return res

        if station_id < 0:
            self.get_logger().error(f"Invalid dispense_station_id {station_id}: must be non-negative")
            res.message = f"Station ID {station_id} must be non-negative"
            return res
    
        success = self._execute_movement(self.plc_const.MOVEMENT_ADDR + station_id, [cell_no])
        res.success = success

        if success:
            self.get_logger().info(f"Successfully moved sliding platform to cell {cell_no} at station {station_id}")
            res.message = "Platform movement successful"
        else:
            error_msg = f"Failed to write to register {self.plc_const.MOVEMENT_ADDR + station_id} with cell_no {cell_no}"
            self.get_logger().error(error_msg)
            res.message = error_msg

        return res

    def movement_decision_v2(self, order_id: int, station_ids: List[int]) -> Optional[int]:
        """
        Determine the next station ID for an order based on minimum station id requirements and availability.
        
        Args:
            order_id: The order to process
            station_ids: List of available dispenser station IDs
            
        Returns:
            Optional[int]: Selected station ID if successful, None if no suitable station found
            
        Raises:
            ValueError: If order_id is negative or station_ids is empty
        """
        if not station_ids:
            raise ValueError("Dispenser station ID list cannot be empty")

        try:
            remainder = self.find_mini_remainder(order_id)

            if remainder is None or not bool(remainder):
                self.get_logger().error(f"Empty remainder!")
                return self.const.GO_STRAIGHT
            
            for station_id in station_ids:
                station = self.conveyor.get_station(station_id)
                if station and station.available() and station_id in remainder:
                    self.get_logger().info(f"Selected station {station_id} for order {order_id}")
                    return station_id

            self.get_logger().info(f"No suitable station found for order {order_id} from {station_ids}")
            return self.const.GO_STRAIGHT
        
        except Exception as e:
            self.get_logger().error(f"Unexpected error for order {order_id}: {str(e)}")
            return self.const.GO_STRAIGHT

    def remove_unavailable_station(self, station_ids: List[int]) -> List[int]:
        """
        Remove unavailable station IDs from the input list and return remaining IDs.
        
        Args:
            station_ids: List of station IDs to check
            
        Returns:
            List[int]: List of station IDs that are unavailable
            
        Raises:
            TypeError: If station_ids is not a list or contains non-integer values
        """
        if not isinstance(station_ids, list):
            raise TypeError(f"Expected list, got {type(station_ids).__name__}")
        if not all(isinstance(station_id, int) for station_id in station_ids):
            raise TypeError("All station IDs must be integers")
        
        available_stations = []
        
        for station_id in station_ids:
            station = self.conveyor.get_station(station_id)
            if station and station.available():
                available_stations.append(station_id)

        return available_stations

    def is_bound(self, material_box_id: int) -> bool:
        """
        Check if a material box is currently bound to any order.
        
        Args:
            material_box_id: The ID of the material box to check
            
        Returns:
            bool: True if the material box is bound, False otherwise
            
        Raises:
            ValueError: If material_box_id is negative
        """
        if material_box_id < 0:
            raise ValueError(f"Material box ID must be non-negative, got {material_box_id}")
        if material_box_id == 0:
            return False
        
        with self.mutex:
            for order_id, status in self.mtrl_box_status.items():
                if status.id == material_box_id:
                    self.get_logger().debug(f"Found bound material box {material_box_id} with order {order_id}")
                    return True
        
        return False
    
    def bind_mtrl_box(self, material_box_id: int) -> Optional[int]:
        """
        Bind a material box to an available order.
        
        Args:
            material_box_id: Integer ID of the material box to bind
            
        Returns:
            bool: True if binding successful, False otherwise
            
        Raises:
            ValueError: If material_box_id is negative
        """
        if material_box_id < 0:
            raise ValueError(f"Material box ID must be non-negative, got {material_box_id}")
        
        with self.mutex:
            for order_id, status in self.mtrl_box_status.items():
                if status.id == 0:
                    new_status = deepcopy(status)
                    new_status.id = material_box_id
                    new_status.status = MaterialBoxStatus.STATUS_PROCESSING # STATUS_CLEANING
                    self.mtrl_box_status[order_id] = new_status
                    self.get_logger().info(f"Material box [{material_box_id}] has been bound to order [{order_id}]")
                    return order_id

            self.get_logger().info("No available material box found for binding")

        return None

    def get_curr_gone(self, mtrl_box: MaterialBox) -> set:
        curr_gone = set()
        for cell in mtrl_box.slots:
            for drug in cell.dispensing_detail:
                station_id = drug.location.dispenser_station
                curr_gone.add(station_id)
        return curr_gone

    def find_mini_req_to_go(self, mtrl_box: MaterialBox) -> set:
        if not isinstance(mtrl_box, MaterialBox):
            raise TypeError("mtrl_box must be MaterialBox object")
        
        req_to_go = set()
        for cell in mtrl_box.slots:
            drug_dict = self.convert_drugs_to_dict(copy(cell.drugs))
            mini_req = list()
            
            while drug_dict:
                # Find key with shortest list of tuples
                key_with_shortest_length = min(drug_dict.keys(), key=lambda k: len(drug_dict[k]))
                _tuple = drug_dict[key_with_shortest_length][0]
                station_id = _tuple[0]
                
                keys_to_remove = []
                # Find all keys containing the station_id
                for id, drug_tuple in drug_dict.items():
                    if any(t[0] == station_id for t in drug_tuple):
                        keys_to_remove.append(id)
                
                # Add station_id to mini_req list
                mini_req.append(station_id)
                
                # Remove all identified keys
                for key in keys_to_remove:
                    drug_dict.pop(key)
            
            for station_id in mini_req:
                req_to_go.add(station_id)

        return req_to_go

    def find_mini_remainder(self, order_id: int) -> Optional[set]:
        with self.mutex:
            status = self.mtrl_box_status.get(order_id)
            proc = self.proc_order.get(order_id)

            if status is None or proc is None:
                self.get_logger().error(f"Order ID {order_id} not found in status or process data")
                raise KeyError(f"Order ID {order_id} not found")
        
            # Track existing dispenser stations in current material box
            curr_mtrl_box = status.material_box
            curr_gone = self.get_curr_gone(curr_mtrl_box)
            # Minimum required stations
            req_mtrl_box = proc.material_box
            mini_req_to_go = self.find_mini_req_to_go(req_mtrl_box)

            self.get_logger().warning(f"Order {order_id}: Current stations {sorted(curr_gone)}, Required stations {sorted(mini_req_to_go)}")

        return mini_req_to_go - curr_gone 

    def get_order_id_by_mtrl_box(self, mtrl_box_id: int) -> Optional[int]:
        """
        Get the order ID associated with a material box ID.
        
        Args:
            mtrl_box_id: The material box ID to look up
            
        Returns:
            Optional[int]: The associated order ID if found, None if not found
            
        Raises:
            ValueError: If mtrl_box_id is negative
        """
        if mtrl_box_id < 0:
            raise ValueError(f"Material box ID must be non-negative, got {mtrl_box_id}")
        
        with self.mutex:
            for order_id, status in self.mtrl_box_status.items():
                if mtrl_box_id == status.id:
                    self.get_logger().debug(f"Found order {order_id} for material box {mtrl_box_id}")
                    return order_id
                
        self.get_logger().warning(f"No order found for material box {mtrl_box_id}")
        return None

    def get_any_pkc_mac_is_idle(self) -> bool:
        """
        Check if any packaging machine is in idle state.
        
        Returns:
            bool: True if at least one machine is idle, False otherwise
        """
        with self.mutex:
            if not self.pkg_mac_status:
                self.get_logger().debug("No packaging machines registered")
                return False

            is_any_idle = any(
                (idle_machine := status).packaging_machine_state == PackagingMachineStatus.IDLE
                for status in self.pkg_mac_status.values()
            )

            if is_any_idle:
                self.get_logger().info(f"Found idle packaging machine {idle_machine.packaging_machine_id}")
                return True
                
        self.get_logger().warning("No idle packaging machines found")
        return False
    
    def move_out_station(self, station: DispenserStation, station_id: int, mtrl_box_id: int) -> bool:
        self.get_logger().info(f"Station {station_id} has all cells completed")

        with self.mutex:
            if station_id in (1, 2) and self.is_releasing_mtrl_box:
                self.get_logger().info("Station 1 or 2: The material box is releasing. Try to move out in the next callback")
                return False

        with self.jack_up_mutex:
            if self.jack_up_status.get(station_id):
                self.get_logger().error(f"station {station_id}: Jack-up point is busy, wait for next callback")
                return False
            
        conveyor_seg = self.conveyor.get_conveyor_by_station(station_id)
        with self.occupy_mutex:
            if conveyor_seg and not conveyor_seg.available():
                self.get_logger().info("The conveyor segment is unavailable. Try to move out in the next callback")
                return False
    
            success = conveyor_seg.occupy(mtrl_box_id)
            if success:
                self.get_logger().warning(f">>>>> Conveyor Occupied: {conveyor_seg.id} by material box: {mtrl_box_id}")
            else:
                self.get_logger().error(f">>>>> Conveyor {conveyor_seg.id} Occupy failed!")
                return False
            
            success = self._execute_movement(self.plc_const.MOVEMENT_ADDR + station_id, [self.plc_const.EXIT_STATION])
            if success:
                station.clear()
                self.get_logger().error(f">>>>> Station Released: {station.id}")
                return True
            else:
                conveyor_seg.clear()
                self.get_logger().error(f">>>>> Conveyor Cleared: {conveyor_seg.id} because failed to execute movement")

        self.get_logger().info(f"Failed to move out material box [{mtrl_box_id}] in station [{station_id}]")
        return False
    
    def find_target_cell(self, 
                        station: DispenserStation, 
                        station_id: int, 
                        status: MaterialBoxStatus, 
                        order: OrderRequest):
        """
        Find the first cell with missing drugs for a specific station.
        
        Args:
            station: DispenserStation tracking completion status
            station_id: ID of the dispensing station
            status: Current material box status
            order: Order request with required drugs
            target_cell: cell index to check
            
        Returns:
            Tuple[int, Optional[List[Tuple[int, int]]]]: 
                - target_cell: First cell with missing drugs or next cell after last checked
                - filtered_missing: List of (unit, amount) for missing drugs, None if none found
        """
        if not all(isinstance(x, (DispenserStation, MaterialBoxStatus, OrderRequest)) for x in [station, status, order]):
            self.get_logger().error("Invalid object types provided")
            return None, 0
        if not isinstance(station_id, int) or station_id < 0:
            self.get_logger().error(f"Invalid station_id: {station_id}")
            return None, 0

        target_cell = self.plc_const.EXIT_STATION

        if self.const.FORCE_MOVE_OUT:
            self.get_logger().warning(f"Debug: force to move out Station [{station_id}]")
        else:
            with self.mutex:
                for i in range(0, self.const.CELLS):
                    transfered_index = self._map_index(i)
                    if not station.is_completed[transfered_index]:
                        target_cell = transfered_index
                        self.get_logger().debug(f"Selected target cell: {target_cell}")
                        break
        
        filtered_missing = []
        
        try:
            while target_cell < self.const.CELLS:
                self.get_logger().debug(f"Checking target_cell: {target_cell}")
                cell_curr_mtrl_box = status.material_box.slots[target_cell]
                cell_order_mtrl_box = order.material_box.slots[target_cell]

                # Find required drugs
                required: Set[Tuple[Tuple[int, int, int], ...]] = set()
                for drug in cell_order_mtrl_box.drugs:
                    locations = tuple((loc.dispenser_station, loc.dispenser_unit, drug.amount) for loc in drug.locations)
                    required.add(locations)

                # Find current drugs
                curr: Set[Tuple[int, int, int], ...] = set()
                for drug in cell_curr_mtrl_box.dispensing_detail:
                    curr.add((drug.location.dispenser_station, drug.location.dispenser_unit, drug.amount))

                if self.const.DEBUG_FLAG and bool(required):
                    self.get_logger().warning(f"required: {required}")
                if self.const.DEBUG_FLAG and bool(curr):
                    self.get_logger().warning(f"curr: {curr}")

                missing_sets = [req_set for req_set in required if req_set not in curr]
                missing_lists = [list(req_set) for req_set in missing_sets]

                if self.const.DEBUG_FLAG and missing_sets:
                    self.get_logger().warning(f"missing_sets: {missing_sets}")
                if self.const.DEBUG_FLAG and missing_lists:
                    self.get_logger().warning(f"missing_lists: {missing_lists}")

                # Find missing drugs for this station
                filtered_missing = [
                    [item for item in lst if item[0] == station_id]
                    for lst in missing_lists
                    if any(item[0] == station_id for item in lst)  # Skip lists with no matching station_id
                ]
                if self.const.DEBUG_FLAG and filtered_missing:
                    self.get_logger().warning(f"filtered_missing: {filtered_missing}")

                self.get_logger().warning(f"Cell {target_cell}: required={required}, curr={curr}, missing at station {station_id}={filtered_missing}")

                if filtered_missing:
                    with self.mutex:
                        station.set_verified(target_cell, True)
                    self.get_logger().warning(f"target_cell: {target_cell} has missing drugs: {filtered_missing}")
                    break
                else:
                    with self.mutex:
                        station.set_completed(target_cell, True)
                    self.get_logger().debug(f"Set target_cell: {target_cell} to True")
                    self.get_logger().debug(f"target_cell: {target_cell} is complete")
                    target_cell += 1
            
            return filtered_missing, target_cell
        
        except Exception as e:
            self.get_logger().error(f"Error finding target cell: {str(e)}")
            return None, 0

    def camera_1_action(self, order_id: Optional[int | None], material_box_id: int) -> Optional[bool]:
        is_continue = False

        if not self.is_bound(material_box_id):
            self.get_logger().error(f"The material box [{material_box_id}] does not bound")
            order_id = self.bind_mtrl_box(material_box_id)
            if order_id:
                self.get_logger().error(f"The material box [{material_box_id}] bound to order id [{order_id}]")
            else:
                self.get_logger().warning(f"Failed to bind material box {material_box_id}")
                is_continue = True

        return is_continue
    
    def camera_1_to_8_action(self, order_id: int, material_box_id: int, camera_id: int) -> Optional[bool]:
        """
        Process operations for camera 1 to 8 for a given order and material box.
        
        Args:
            order_id: The order being processed
            material_box_id: The material box ID to handle
            camera_id: The camera ID 
            
        Returns:
            bool: True if operation completed successfully, False otherwise
        """
        if camera_id not in self.plc_const.CAMERA_STATION_PLC_MAP:
            self.get_logger().error(f"No station mapping for camera {camera_id}")
            return None
        
        # update location
        if status := self.mtrl_box_status.get(order_id):
            with self.mutex:
                status.location = f"camera_{camera_id}"

        is_completed = False
        station_ids, register_addr = self.plc_const.CAMERA_STATION_PLC_MAP[camera_id]
        decision = 0

        with self.occupy_mutex:
            available_stations = self.remove_unavailable_station(station_ids)

            if available_stations:
                match self.const.MOVEMENT_VERSION:
                    case 1:
                        decision = self.movement_decision_v1(order_id, available_stations)
                    case 2:
                        decision = self.movement_decision_v2(order_id, available_stations)

            self.get_logger().warning(f"Decision {decision} made from available stations {available_stations} for camera {camera_id}")
            
            if decision in available_stations: 
                # try to move into station
                self.get_logger().warning(f"Move into station {decision} for camera {camera_id}")

                station = self.conveyor.get_station(decision)
               
                if station and not station.available():
                    self.get_logger().error(f"The station is unavailable for camera [{camera_id}]. There is unreasonable in this validation.")
                    return False

                success = station.occupy(material_box_id)
                if success:
                    self.get_logger().warning(f">>>>> Station Occupied: {station.id} by material box: {material_box_id}")
                else:
                    self.get_logger().error(f">>>>> Station {station.id} Occupy failed!")
                    return False
                
                success = self._execute_movement(register_addr, self.plc_const.STATION_VALUE_MAP.get(decision))
                if not success:
                    station.clear()
                    self.get_logger().error(f"Failed to write to register {register_addr}")
                    return False
                
                is_completed = True
            else: 
                # try to go straight
                self.get_logger().warning(f"Go straight for camera {camera_id}")

                next_conveyor = self.conveyor.get_next_conveyor(camera_id)
                
                if next_conveyor and not next_conveyor.available():
                    self.get_logger().warning(f"The next conveyor is unavailable for camera [{camera_id}]")
                    return False

                if camera_id in range(self.const.CAMERA_ID_START, self.const.CAMERA_ID_VISION):
                    success = next_conveyor.occupy(material_box_id)
                    if success:
                        self.get_logger().warning(f">>>>> Conveyor Occupied: {next_conveyor.id} by material box: {material_box_id}")
                    else:
                        self.get_logger().error(f">>>>> Conveyor {next_conveyor.id} Occupy failed in go straight!")
                        return False
                    
                success = self._execute_movement(register_addr, self.plc_const.STRAIGHT_VALUE)
                if not success:
                    next_conveyor.clear()
                    self.get_logger().error(f"Failed to write to register {register_addr}")
                    return False
                
                is_completed = True      
               
        self.get_logger().warning(f"decision [{decision}] / {station_ids} is made for material box [{material_box_id}] in camera [{camera_id}]")
        return is_completed
    
    def camera_9_action(self, order_id: int, material_box_id: int, camera_id: int) -> Optional[bool]:
        self.get_logger().info(f"Vision inspection triggered for material box [{material_box_id}]")

        is_completed = True
        
        # FIXME: Wait the vision system complete
        # update location
        if status := self.mtrl_box_status.get(order_id):
            with self.mutex:
                status.location = f"camera_{camera_id}"
            self.get_logger().info(f"updated location to camera_{camera_id} w/ material box [{material_box_id}]")

        remainder = set()
        match self.const.MOVEMENT_VERSION:
            case 1:
                remainder = self.find_remainder(order_id)
            case 2:
                remainder = self.find_mini_remainder(order_id)

        self.get_logger().info(f"in camera 9 remainder: {remainder}")
        
        if remainder is not None and len(remainder) == 0:
            self.vision_request.append(order_id)
            self.get_logger().info(f"added to vision inspection request order id [{order_id}]")
            status = self.mtrl_box_status.get(order_id)

            # FIXME: force to set the status because the vision inspection does not ready
            with self.mutex:
                if status:
                    status.status = MaterialBoxStatus.STATUS_AWAITING_PACKAGING
                    self.get_logger().info(f"updated status to STATUS_AWAITING_PACKAGING w/ box [{material_box_id}]")

        return is_completed
        
    def camera_10_action(self, order_id: int, material_box_id: int, camera_id: int) -> Optional[bool]:
        """
        Process operations for camera 10 for a given order and material box.
        
        Args:
            order_id: The order being processed
            material_box_id: The material box ID to handle
            
        Returns:
            bool: True if operation completed successfully, False otherwise
        """
        self.get_logger().info(f"Packaging machine 1 triggered for material box [{material_box_id}]")    

        is_completed = False
        pkg_req_success = False

        status = self.mtrl_box_status.get(order_id)
        ready_to_pkg = False
        with self.mutex:
            # update location
            if status:
                status.location = f"camera_{camera_id}"
                self.get_logger().info(f"updated location to camera_{camera_id} w/ material box [{material_box_id}]")
            if status and status.status == MaterialBoxStatus.STATUS_AWAITING_PACKAGING:
                ready_to_pkg = True

        if ready_to_pkg and self.get_any_pkc_mac_is_idle():
            pkg_req_success = self.send_pkg_req(order_id)
            if pkg_req_success:
                is_completed = True
                self.get_logger().warning(f"Sent a packaging request to packaging machine manager successfully")
                self.get_logger().warning(f"Removed the order in the proc_order successfully")
            else:
                self.get_logger().error(f"Failed to send the packaging request to packaging machine manager") 
        else:
            block_success = self.send_income_mtrl_box(material_box_id)
            if block_success:
                is_completed = True
                self.get_logger().warning(f"Sent a income materail box to packaging machine manager successfully")
            else:
                self.get_logger().error(f"Failed to send the income materail box to packaging machine manager")

        conveyor = self.conveyor.get_conveyor(self.const.CAMERA_ID_VISION)
        with self.occupy_mutex:
            if conveyor and conveyor.is_occupied:
                conveyor.clear()
                self.get_logger().error(f"Cleared conveyor id: {self.const.CAMERA_ID_VISION}")
        
        pkg_conveyor = self.conveyor.get_conveyor(self.const.CAMERA_ID_PKG_MAC_1)
        with self.occupy_mutex:
            if pkg_conveyor and pkg_conveyor.available():
                success = pkg_conveyor.occupy(material_box_id)
                if success:
                    self.get_logger().warning(f">>>>> Conveyor Occupied: {pkg_conveyor.id} by material box: {material_box_id}")
                else:
                    self.get_logger().error(f">>>>> Conveyor {pkg_conveyor.id} Occupy failed!")
            else:
                self.get_logger().error(f"The pkg 1 conveyor is unavailable. [free: {pkg_conveyor.is_free}, occupied: {pkg_conveyor.is_occupied}]")

        return is_completed
    
    def camera_11_action(self, order_id: int, material_box_id: int, camera_id: int) -> Optional[bool]:
        """
        Process operations for camera 11 for a given order and material box.
        
        Args:
            order_id: The order being processed
            material_box_id: The material box ID to handle
            camera_id: The camera ID being used
            
        Returns:
            bool: True if operation completed successfully, False otherwise
        """
        self.get_logger().info(f"Packaging machine 2 triggered for material box [{material_box_id}]")

        is_completed = False
        elevator_req = False

        with self.mutex:
            if self.is_releasing_mtrl_box:
                self.get_logger().debug(f"Skipped elevator movement for box {material_box_id} (currently releasing box)")
                return False

        if status := self.mtrl_box_status.get(order_id):
            # update location
            with self.mutex:
                status.location = f"camera_{camera_id}"

                # normal case
                if status.status == MaterialBoxStatus.STATUS_PASS_TO_PACKAGING:
                    elevator_success = self._execute_movement(self.plc_const.TRANSFER_MTRL_BOX_ADDR, self.plc_const.MTRL_BOX_RETRIEVAL_PLC_VALUES)
                    if elevator_success:
                        if popped_status := self.mtrl_box_status.pop(order_id):
                            self.get_logger().warning(f"Popped a material box status [order_id: {popped_status.order_id}]")
                            popped_status.end_time = self.get_clock().now().to_msg()
                            self.mtrl_box_status_pub.publish(popped_status)
                            self.get_logger().warning(f"Published the last status [order_id: {popped_status.order_id}]")
                            is_completed = True
                        self.get_logger().warning(f"Sent elevator request (retrieval) successfully")
                    else:
                        self.get_logger().error(f"Failed to write to register for elevator")
                else:
                    first_conveyor = self.conveyor.get_conveyor(self.const.CAMERA_ID_START)
                    if first_conveyor and not first_conveyor.available():
                        self.get_logger().warning(f"The bypass action is stopped because the first conveyor is unavailable.")
                        return False

                    elevator_success = self._execute_movement(self.plc_const.TRANSFER_MTRL_BOX_ADDR, self.plc_const.MTRL_BOX_BYPASS_PLC_VALUES)
                    if elevator_success:
                        is_completed = True
                        self.get_logger().warning(f"Sent elevator request (passby) successfully")
                    else:
                        self.get_logger().error(f"Failed to write to register for elevator")

            elevator_req = True
        else:
            # store material box to container for unbound material box
            is_completed, elevator_req = self.manually_store(material_box_id)

        if elevator_req:
            with self.elevator_mutex:
                if not any(req[0] == material_box_id for req in self.elevator_request):
                    self.elevator_request.append((material_box_id, self.get_clock().now()))
                    self.get_logger().info(f"appended to elevator request [{material_box_id}]")

        pkg_1_conveyor = self.conveyor.get_conveyor(self.const.CAMERA_ID_PKG_MAC_1)
        with self.occupy_mutex:
            if pkg_1_conveyor and pkg_1_conveyor.is_occupied:
                pkg_1_conveyor.clear()
                self.get_logger().error(f"Cleared conveyor id: {self.const.CAMERA_ID_PKG_MAC_1}")
        
        pkg_2_conveyor = self.conveyor.get_conveyor(self.const.CAMERA_ID_PKG_MAC_2)
        with self.occupy_mutex:
            if pkg_2_conveyor and pkg_2_conveyor.available():
                success = pkg_2_conveyor.occupy(material_box_id)
                if success:
                    self.get_logger().warning(f">>>>> Conveyor Occupied: {pkg_2_conveyor.id} by material box: {material_box_id}")
                else:
                    self.get_logger().error(f">>>>> Conveyor {pkg_2_conveyor.id} Occupy failed!")
            else:
                self.get_logger().error(f"The pkg 1 conveyor is unavailable. [free: {pkg_2_conveyor.is_free}, occupied: {pkg_2_conveyor.is_occupied}]")

        return is_completed
    
    def send_rel_blocking(self) -> Optional[bool]:
        while not self.rel_blocking_cli.wait_for_service(timeout_sec=1.0):
            if not rclpy.ok():
                return None
            self.get_logger().info("rel_blocking Service not available, waiting again...")
        
        req = Trigger.Request()

        try:
            future = self.rel_blocking_cli.call_async(req)
            future.add_done_callback(partial(self.rel_blocking_done_cb))

            self.get_logger().info("rel_blocking_cli is called, waiting for future done")
            return True

        except Exception as e:
            self.get_logger().error(f"Service call failed: {str(e)}")
            return False

    def send_income_mtrl_box(self, mtrl_box_id: int) -> Optional[bool]:
        """
        Send a material box ID to the income service.
        
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

        while not self.in_mtrl_box_cli.wait_for_service(timeout_sec=1.0):
            if not rclpy.ok():
                return None
            self.get_logger().info("income_mtrl_box Service not available, waiting again...")

        try:
            future = self.in_mtrl_box_cli.call_async(req)
            future.add_done_callback(partial(self.income_mtrl_box_done_cb, mtrl_box_id))

            self.get_logger().info("in_mtrl_box_cli is called, waiting for future done")
            return True

        except Exception as e:
            self.get_logger().error(f"Service call failed for ID {mtrl_box_id}: {str(e)}")
            return False

    def send_pkg_req(self, order_id) -> Optional[bool]:
        """
        Send a packaging request for a specific order.
        
        Args:
            order_id: The ID of the order to package
            
        Returns:
            Optional[bool]: True if successful, False if failed, None if service unavailable
            
        Raises:
            TypeError: If order_id is not an integer
            ValueError: If order_id is negative or order data is invalid
        """
        if not isinstance(order_id, int):
            raise TypeError(f"Expected integer order_id, got {type(order_id).__name__}")
        if order_id < 0:
            raise ValueError(f"Material box ID must be non-negative, got {order_id}")
        
        with self.mutex:
            proc_order = self.proc_order.get(order_id)
            status = self.mtrl_box_status.get(order_id)

        if proc_order is None or status is None:
            raise ValueError(f"Order {order_id} not found in process or status data")
        
        req = PackagingOrder.Request()
        req.order_id = order_id
        req.material_box_id = status.id
        req.requester_id = 1234

        for i, info in enumerate(req.print_info):
            if not proc_order.material_box.slots or i >= len(proc_order.material_box.slots):
                continue
            if not proc_order.material_box.slots[i].drugs:
                continue

            info.cn_name = proc_order.patient.name_ch
            info.en_name  = proc_order.patient.name

            curr_meal = (proc_order.start_meal + i) % 4
            info.time = PkgInfo.MEAL_TIME.get(curr_meal, "Unknown")

            _date = proc_order.start_date
            try:
                dt = datetime.strptime(_date, PkgInfo.DATE_FORMAT)
                self.get_logger().debug(f"dt: {dt}")
                days_to_add = (proc_order.start_meal + i) // 4 
                new_date = dt + timedelta(days=days_to_add)
                self.get_logger().debug(f"new_date: {new_date}")
                self.get_logger().debug(f"new_date.strftime({PkgInfo.DATE_FORMAT}): {new_date.strftime(PkgInfo.DATE_FORMAT)}")
                info.date = f"Date: {new_date.strftime(PkgInfo.DATE_FORMAT)}"
            except ValueError as e:
                info.date = "ERROR"
                self.get_logger().error(f"Invalid date format for order {order_id}: {str(e)}")

            info.qr_code = proc_order.prescription_id + ":" + str(i)

            for drug in proc_order.material_box.slots[i].drugs:
                drug_str = f"{drug.name}   {drug.amount}"
                self.get_logger().debug(f"Added to order {order_id}: {drug_str}")
                info.drugs.append(drug_str)

        while not self.pkg_order_cli.wait_for_service(timeout_sec=1.0):
            if not rclpy.ok():
                return None
            self.get_logger().info("packaging_order Service not available, waiting again...")

        try:
            future = self.pkg_order_cli.call_async(req)
            future.add_done_callback(partial(self.pkg_req_done_cb, order_id))
            
            self.get_logger().info("pkg_order_cli is called, waiting for future done")
            return True
        
        except Exception as e:
            self.get_logger().error(f"Service call failed for order id {order_id}: {str(e)}")
            return False
       
    def send_dispense_req(self, filtered_missing, station_id: int, cell_no: int, order_id: int) -> Optional[bool]:
        """
        Process dispensing operations for a material box cells.
        
        Args:
            filtered_missing: 
            station_id: ID of the dispensing station
            cell_no: 
            order_id:
            
        Returns:
            bool: True if dispensing completed successfully, False otherwise
        """
        if not all(isinstance(x, (int)) for x in [station_id, cell_no, order_id]):
            self.get_logger().error("Invalid integer types provided")
            return False
        if not isinstance(station_id, int) or station_id < 0:
            self.get_logger().error(f"Invalid station_id: {station_id}")
            return False
        if not (0 <= cell_no < self.const.CELLS):
            self.get_logger().error(f"Invalid cell_no: {cell_no}")
            return False
        
        req = DispenseDrug.Request()

        if not self.const.FAKE_DISPENSE:
            for item in filtered_missing:
                req.content.append(DispenseContent(unit_id=item[0][1], amount=item[0][2]))

        self.get_logger().info(f"filtered_missing length: {len(filtered_missing)}")
        self.get_logger().info(f"DispenseDrug.Request: {req}")

        while not self._get_dis_station_cli(station_id).wait_for_service(timeout_sec=1.0):
            if not rclpy.ok():
                return None
            self.get_logger().error("dispense request Service not available, waiting again...")

        try:
            future = self._get_dis_station_cli(station_id).call_async(req)
            future.add_done_callback(partial(self.dispense_done_cb, filtered_missing, station_id, cell_no, order_id))

            self.get_logger().debug(f"dis_station_clis [{station_id}] is called, waiting for future done")
            return True
        
        except Exception as e:
            self.get_logger().error(f"Service call failed {station_id}: {str(e)}")
            return False

    async def read_registers(self, address: int, count: int) -> Optional[array]:
        """
        Read a specified number of registers from a given address.
        
        Args:
            address: Starting register address to read from
            count: Number of registers to read
            
        Returns:
            Optional[List[int]]: List of register values if successful, None if failed
            
        Raises:
            TypeError: If address or count are not integers
            ValueError: If address or count are negative
        """
        if not isinstance(address, int) or not isinstance(count, int):
            raise TypeError(f"Expected integers, got address: {type(address).__name__}, count: {type(count).__name__}")
        if address < 0 or count < 0:
            raise ValueError(f"Address ({address}) and count ({count}) must be non-negative")

        req = ReadRegister.Request(
            address = address,
            count = count
        )

        while not self.read_cli.wait_for_service(timeout_sec=1.0):
            if not rclpy.ok():
                return None
            self.get_logger().info("read_registers Service not available, waiting again...")

        try:
            res = await self.read_cli.call_async(req)
            if res.success:
                if not isinstance(res.values, array):
                    self.get_logger().error("Invalid response format: values not a list")
                    return None

                self.get_logger().debug(f"Successfully read {len(res.values)} registers from address {address}")
                return res.values
            
            return None
        
        except Exception as e:
            self.get_logger().error(f"Failed to read registers at {address}: {str(e)}")
            return None
  
    async def write_registers(self, address: int, values: List[int]) -> bool:
        """
        Write values to registers starting at the specified address.
        
        Args:
            address: Starting register address to write to
            values: List of integer values to write
            
        Returns:
            bool: True if write successful, False otherwise
            
        Raises:
            TypeError: If address or values have incorrect types
            ValueError: If address is negative or values list is empty
        """
        if not isinstance(address, int):
            raise TypeError(f"Expected integer address, got {type(address).__name__}")
        if not isinstance(values, list) or not all(isinstance(v, int) for v in values):
            raise TypeError(f"Expected list of integers for values, got {type(values).__name__}")
        if address < 0:
            raise ValueError(f"Address must be non-negative, got {address}")
        if not values:
            raise ValueError("Values list cannot be empty")
        
        req = WriteRegister.Request(
            address = address,
            values = values
        )

        while not self.write_cli.wait_for_service(timeout_sec=1.0):
            if not rclpy.ok():
                return None
            self.get_logger().info("write_registers Service not available, waiting again...")

        try:
            res = await self.write_cli.call_async(req)

            if res and res.success:
                self.get_logger().debug(f"Successfully wrote {len(values)} registers at address {address}: {values}")
                return True
                
            self.get_logger().error(f"Service reported failure writing {len(values)} registers at {address}")
            return False
        
        except Exception as e:
            self.get_logger().error(f"Failed to write registers at {address}: {str(e)}")
            return False
    
    def send_vision_req(self, order_id: int) -> Optional[bool]:
        if not isinstance(order_id, int):
            raise TypeError(f"Expected integer order_id, got {type(address).__name__}")

        MAX_RETIES = 3
        reties = 0
        while not self.visual_action_cli.wait_for_server(timeout_sec=0.05):
            if not rclpy.ok() or reties >= MAX_RETIES:
                return None
            reties += 1
            self.get_logger().info("Waiting for vision action server not available, waiting again...")

        goal_msg = VisualDetection.Goal(order_id = order_id)

        self.get_logger().info("Sending goal request...")
        try:
            future = self.visual_action_cli.send_goal_async(
                goal_msg,
                feedback_callback=self.vision_feedback_cb
            )
            future.add_done_callback(self.vision_goal_response_cb)
            return True
        except Exception as e:
            self.get_logger().error(f"Failed to send vision request: {str(e)}")
            return False

    def rel_blocking_done_cb(self, future) -> None:
        res = future.result()

        if res and res.success:
            tmp = None
            with self.elevator_mutex:   
                if self.elevator_ready:
                    tmp = self.elevator_ready.popleft()
            if tmp is not None:
                self.get_logger().info(f"Removed from elevator_ready successfully")
            else:
                self.get_logger().info(f"elevator_ready popleft failed")

            pkg_2_conveyor = self.conveyor.get_conveyor(self.const.CAMERA_ID_PKG_MAC_2)
            with self.occupy_mutex:
                if pkg_2_conveyor and pkg_2_conveyor.is_occupied:
                    pkg_2_conveyor.clear()
                    self.get_logger().error(f"Cleared conveyor id: {self.const.CAMERA_ID_PKG_MAC_2}")

            self.get_logger().info(f"Released the block successfully")
        else:
            self.get_logger().error("Failed to send rel_blocking")

    def income_mtrl_box_done_cb(self, mtrl_box_id, future) -> None:
        res = future.result()

        if res and res.success:
            self.get_logger().info(f"Successfully sent income material box ID: {mtrl_box_id}")

            with self.mutex:
                self.is_mtrl_box_coming = True
            self.get_logger().warning(f"Set is_mtrl_box_coming in income_mtrl_box done callback")
        else:
            self.get_logger().error(f"Service call succeeded but reported failure for ID: {mtrl_box_id}")
       
    def pkg_req_done_cb(self, order_id: int, future) -> None:
        res = future.result()

        if res and res.success:
            self.get_logger().info(f"Packaging request successful for order {order_id}")

            if status := self.mtrl_box_status.get(order_id):
                with self.mutex:
                    status.status = MaterialBoxStatus.STATUS_PASS_TO_PACKAGING
                self.get_logger().info(f"Updated status to STATUS_PASS_TO_PACKAGING") # w/ box [{mtrl_box_id}]

            order = self.proc_order.pop(order_id)
            if order:
                self.get_logger().info(f"Removed the order in proc_order: {order_id}")

            with self.mutex:
                self.is_mtrl_box_coming = True
            self.get_logger().warning(f"Set is_mtrl_box_coming in pkg_req done callback")
        else:         
            self.get_logger().error(f"Packaging service failed for order {order_id}")

    def dispense_done_cb(self, filtered_missing, station_id: int, cell_no: int, order_id:int, future) -> None:
        res = future.result()

        if res and res.success:
            station = self.conveyor.get_station(station_id)
            if not station:
                self.get_logger().info(f"station: {station_id} not found in dispense done callback")
                return

            with self.mutex:
                station.set_completed(cell_no, True)
                station.set_dispense_req_done(cell_no, True)

            self.get_logger().info(f"Set station: {station_id}, cell: {cell_no} to True successfully")
            
            if status := self.mtrl_box_status.get(order_id):
                with self.mutex:
                    for item in filtered_missing: 
                        new_detail = DispensingDetail()
                        new_detail.location.dispenser_station = station_id
                        new_detail.location.dispenser_unit = item[0][1]
                        new_detail.amount = item[0][2]
                        try:
                            status.material_box.slots[cell_no].dispensing_detail.append(new_detail)
                        except Exception as e:
                            self.get_logger().error(f"Can't add drug to status")
                        self.get_logger().info(f"Added a new drug to status >>> {new_detail}")
                        self.get_logger().info(f"detail: >>> {status.material_box.slots[cell_no].dispensing_detail}")
            else:
                self.get_logger().info(f"status is not found in dispense_done_cb by order id {order_id}")
            
            self.get_logger().info(f"Dispense completed for station {station_id}, cell {cell_no}")
        else:
            self.get_logger().error(f"Service call succeeded but reported failure for dispenser station: {station_id}")

    def vision_goal_response_cb(self, future):
        goal_handle = future.result()
        if not goal_handle.accepted:
            self.get_logger().info("Goal rejected")
            return

        self.get_logger().info("Goal accepted")
        future = goal_handle.get_result_async()
        future.add_done_callback(self.vision_result_cb)

    def vision_feedback_cb(self, feedback):
        self.get_logger().info(f"Received feedback: {feedback.feedback.running}")

    def vision_result_cb(self, future):
        result = future.result().result
        status = future.result().status
        if status == GoalStatus.STATUS_SUCCEEDED:
            self.get_logger().info("Goal succeeded")
            self.get_logger().info(f"result: {result}")
        else:
            self.get_logger().info("Goal failed with status: NO!!!")
    
    def convert_drugs_to_dict(self, drugs: List[Drug]) -> Optional[dict]:
        drugs_dict = dict()
        for i, drug in enumerate(drugs, start=1):
            drugs_dict[i] = [(loc.dispenser_station, loc.dispenser_unit, drug.amount) for loc in drug.locations]
        
        return drugs_dict
        
    def _execute_movement(self, address: int, values: List[int]) -> Optional[bool]:
        """Execute a register write operation and return success status."""
        try:
            future = run_coroutine_threadsafe(
                self.write_registers(address, values), 
                self.loop
            )
            return future.result()
        except Exception as e:
            self.get_logger().error(f"Movement execution failed: {str(e)}")
            return False

    def manually_store(self, mtrl_box_id: int):
        is_completed = False
        elevator_req = False

        with self.mutex:
            if self.is_mtrl_box_storing:
                self.get_logger().info(f"The state is storing. The scan is invalid.")
                is_completed = True
                return is_completed, elevator_req

        with self.mutex: 
            self.is_mtrl_box_storing = True
        self.get_logger().info(f"Set is_mtrl_box_storing")
        
        block_success = self.send_income_mtrl_box(mtrl_box_id)
        if block_success:
            self.get_logger().warning(f"Sent a income materail box to packaging machine manager successfully")
        else:
            self.get_logger().error(f"Failed to send the income materail box to packaging machine manager")
        
        elevator_success = self._execute_movement(self.plc_const.TRANSFER_MTRL_BOX_ADDR, self.plc_const.MTRL_BOX_RETRIEVAL_PLC_VALUES)
        if elevator_success:
            self.get_logger().warning(f"Sent elevator request (retrieval) successfully")
        else:
            self.get_logger().error(f"Failed to write to register for elevator")
        
        is_completed = block_success and elevator_success
        elevator_req = True

        return is_completed, elevator_req
        
    def _initialize_conveyor(self) -> None:
        """Initialize Conveyor Structure."""
        self.conveyor = Conveyor()
        self.conveyor.append(1)
        self.conveyor.attach_station(1, "left", DispenserStation(1))
        self.conveyor.attach_station(1, "right", DispenserStation(2))
        self.conveyor.append(2)
        self.conveyor.attach_station(2, "left", DispenserStation(3))
        self.conveyor.attach_station(2, "right", DispenserStation(4))
        self.conveyor.append(3)
        self.conveyor.attach_station(3, "left", DispenserStation(5))
        self.conveyor.attach_station(3, "right", DispenserStation(6))
        self.conveyor.append(4)
        self.conveyor.attach_station(4, "left", DispenserStation(7))
        self.conveyor.append(5)
        self.conveyor.attach_station(5, "left", DispenserStation(8))
        self.conveyor.append(6)
        self.conveyor.attach_station(6, "left", DispenserStation(10))
        self.conveyor.attach_station(6, "right", DispenserStation(9))
        self.conveyor.append(7)
        self.conveyor.attach_station(7, "left", DispenserStation(12))
        self.conveyor.attach_station(7, "right", DispenserStation(11))
        self.conveyor.append(8)
        self.conveyor.attach_station(8, "left", DispenserStation(14))
        self.conveyor.attach_station(8, "right", DispenserStation(13))
        self.conveyor.append(self.const.CAMERA_ID_VISION) # Vision
        self.conveyor.append(self.const.CAMERA_ID_PKG_MAC_1) # Packaging Machine 1
        self.conveyor.append(self.const.CAMERA_ID_PKG_MAC_2) # Packaging Machine 2

        self.get_logger().debug(f"\n{self.conveyor}")

    def _map_index(self, index: int) -> int:
        if index == self.plc_const.EXIT_STATION:
            return index
        if not (0 <= index < self.const.CELLS):
            raise Exception("map index out of range")
        
        return self.const.INDEX_MAP.get(index)
        # return (index // self.const.GRID_WIDTH) + (index % self.const.GRID_WIDTH) * self.const.GRID_HEIGHT 

    def _get_dis_station_cli(self, station_id: int) -> Optional[rclpy.client.Client]:
        """Safely get a dispenser station client by ID."""
        return self.dis_station_clis.get(station_id)

    def _run_loop(self):
        """Run the asyncio event loop in a dedicated thread."""
        asyncio.set_event_loop(self.loop)
        self.get_logger().info("Started the event loop.")
        self.loop.run_forever()

    def destroy_node(self):
        if self.loop.is_running():
            self.loop.call_soon_threadsafe(self.loop.stop)
            self.get_logger().info("Removed loop successfully")

        if self.loop_thread is not None:
            self.loop_thread.join()
            self.get_logger().info("Removed loop thread successfully")

        super().destroy_node()

    # ================= caution!!! deprecated functions =================
        
    def movement_decision_v1(self, order_id: int, station_ids: List[int]) -> Optional[int]:
        return self.const.GO_STRAIGHT
        
    def find_remainder(self, order_id: int) -> Optional[set]:
        return None

    def get_req_to_go(self, mtrl_box: MaterialBox) -> Optional[set]:
        return None