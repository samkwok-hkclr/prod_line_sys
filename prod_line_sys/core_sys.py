import sys
import time
import asyncio
from asyncio import coroutine, run, run_coroutine_threadsafe
from collections import deque
from threading import Thread, Lock
from typing import Dict, List, Set, Tuple, Final, Optional
from queue import Queue
from array import array
from copy import deepcopy
from datetime import datetime, timedelta
import rclpy

from rclpy.node import Node
from rclpy.action import ActionServer, GoalResponse, CancelResponse
from rclpy.callback_groups import MutuallyExclusiveCallbackGroup, ReentrantCallbackGroup
from rclpy.executors import ExternalShutdownException, MultiThreadedExecutor

from std_msgs.msg import Bool, UInt8MultiArray
from std_srvs.srv import Trigger

from smdps_msgs.msg import (CameraState, 
                            CameraStatus, 
                            CameraTrigger, 
                            MaterialBoxStatus, 
                            ProductionLineStatus, 
                            OrderRequest,
                            OrderResponse,
                            MaterialBox,
                            PackagingMachineStatus,
                            PackageInfo,
                            DispenseContent,
                            DispensingDetail,
                            DrugLocation)

from smdps_msgs.srv import (NewOrder, 
                            ReadRegister, 
                            WriteRegister, 
                            MoveSlidingPlatform, 
                            DispenseDrug, 
                            PackagingOrder,
                            UInt8)

from smdps_msgs.action import NewOrder as NewOrderAction

from .dispenser_station import DispenserStation
from .conveyor_segment import ConveyorSegment
from .conveyor import Conveyor
from .const import Const

class CoreSystem(Node):
    def __init__(self, executor):
        super().__init__("core_system")
        # Local memory storage
        self.recv_order = deque()
        self.proc_order: Dict[int, OrderRequest] = {} # order_id, OrderRequest
        self.mtrl_box_status: Dict[int, MaterialBoxStatus] = {} # order_id, MaterialBoxStatus
        self.qr_scan: List[CameraTrigger] = [] 
        self.elevator_queue = deque()

        self.pkg_mac_status: Dict[int, PackagingMachineStatus] = {} # pkg_mac_id, PackagingMachineStatus
        # Action server goal
        self._goal_queue = deque()
        self._curr_goal = None

        # Constant
        self.const: Final = Const()

        self.exec = executor

        # ROS2 client node, maybe unused
        self.cli_node = rclpy.create_node("cli_node")
        self.exec.add_node(self.cli_node)

        # mutex
        self.mutex = Lock()

        # Graph of conveyor structure
        self.conveyor = None
        self._initialize_conveyor()

        # handle async functions
        self.loop = asyncio.new_event_loop()
        self.loop_thread = Thread(target=self.run_loop, daemon=True)
        self.loop_thread.start()

        # sync delay
        self.dispense_wait = self.create_rate(1.5, self.get_clock())
        self.waiting_result = self.create_rate(1.0, self.get_clock())

        # State of PLC registers
        self.is_plc_connected = False
        self.is_releasing_mtrl_box = False
        self.is_elevator_ready = False

        # Callback groups
        sub_cbg = MutuallyExclusiveCallbackGroup()
        srv_ser_cbg = MutuallyExclusiveCallbackGroup()
        srv_cli_cbg = MutuallyExclusiveCallbackGroup()
        station_srv_cli_cbgs = [MutuallyExclusiveCallbackGroup() for _ in range(self.const.NUM_DISPENSER_STATIONS)]

        normal_timer_cbg = MutuallyExclusiveCallbackGroup()
        qr_handle_timer_cbg = MutuallyExclusiveCallbackGroup()
        station_timer_cbgs = [MutuallyExclusiveCallbackGroup() for _ in range(self.const.NUM_DISPENSER_STATIONS)]

        action_ser_cbg = ReentrantCallbackGroup()
        self.get_logger().info("Callback groups are created")

        # Publishers
        self.mtrl_box_status_pub = self.create_publisher(MaterialBoxStatus, "material_box_status", 10)
        self.get_logger().info("Publishers are created")

        # Subscriptions
        self.plc_conn_sub = self.create_subscription(Bool, "plc_connection", self.plc_conn_cb, 10, callback_group=sub_cbg)
        self.rel_mtrl_box_sub = self.create_subscription(Bool, "releasing_material_box", self.releasing_mtrl_box_cb, 10, callback_group=sub_cbg)
        self.sliding_platform_curr_sub = self.create_subscription(UInt8MultiArray, "sliding_platform_curr", self.sliding_platform_curr_cb, 10, callback_group=sub_cbg)
        self.sliding_platform_cmd_sub = self.create_subscription(UInt8MultiArray, "sliding_platform_cmd", self.sliding_platform_cmd_cb, 10, callback_group=sub_cbg)
        self.sliding_platform_ready_sub = self.create_subscription(UInt8MultiArray, "sliding_platform_ready", self.sliding_platform_ready_cb, 10, callback_group=sub_cbg)
        self.elevator_sub = self.create_subscription(Bool, "elevator", self.elevator_cb, 10, callback_group=sub_cbg)
        self.qr_scan_sub = self.create_subscription(CameraTrigger, "qr_camera_scan", self.qr_scan_cb, 10, callback_group=sub_cbg)
        self.pkg_mac_status_sub = self.create_subscription(PackagingMachineStatus, "packaging_machine_status", self.pkg_mac_status_cb, 10, callback_group=sub_cbg)
        self.get_logger().info("Subscriptions are created")
        
        # Service servers
        self.new_order_srv = self.create_service(NewOrder, "new_order", self.new_order_cb, callback_group=srv_ser_cbg)
        self.move_sliding_platform = self.create_service(MoveSlidingPlatform, "move_sliding_platform", self.move_sliding_platform_cb, callback_group=srv_ser_cbg)
        self.get_logger().info("Service Servers are created")
        
        # Service clients
        self.read_cli = self.create_client(ReadRegister, "read_register", callback_group=srv_cli_cbg)
        self.write_cli = self.create_client(WriteRegister, "write_register", callback_group=srv_cli_cbg)
        self.income_mtrl_box_cli = self.create_client(UInt8, "income_material_box", callback_group=srv_cli_cbg)
        self.con_mtrl_box_cli = self.create_client(UInt8, "container_material_box", callback_group=srv_cli_cbg)
        self.rel_blocking_cli = self.create_client(Trigger, "release_blocking", callback_group=srv_cli_cbg)
        self.pkg_order_cli = self.create_client(PackagingOrder, "packaging_order", callback_group=srv_cli_cbg)
        self.dis_station_clis: Dict[int, rclpy.client.Client] = {} # station_id, Client
        self._initialize_dis_station_clis(srv_cli_cbg)
        self.get_logger().info("Service Clients are created")

        # Action server
        self.new_order_action_ser = ActionServer(
            self,
            NewOrderAction,
            "new_order",
            handle_accepted_callback=self.handle_accepted_cb,
            execute_callback=self.execute_cb,
            goal_callback=self.goal_cb,
            cancel_callback=self.cancel_cb,
            callback_group=action_ser_cbg
        )

        # Timers
        self.qr_handle_timer = self.create_timer(1.0, self.qr_handle_cb, callback_group=qr_handle_timer_cbg)
        self.start_order_timer = self.create_timer(1.0, self.start_order_cb, callback_group=normal_timer_cbg)
        self.order_status_timer = self.create_timer(1.0, self.order_status_cb, callback_group=normal_timer_cbg)
        self.elevator_timer = self.create_timer(1.0, self.elevator_dequeue_cb, callback_group=normal_timer_cbg)
        self.dis_station_timers: Dict[int, rclpy.Timer.Timer] = {}
        self._initialize_dis_station_timers(1.0, station_timer_cbgs)
        self.get_logger().info("Timers are created")
        
        self.get_logger().info("Produation Line Core System Node started")

    # Subscription callbacks
    def plc_conn_cb(self, msg: Bool) -> None:
        with self.mutex:
            self.is_plc_connected = msg.data
        if not msg.data:
            self.get_logger().error(f"Received: PLC is disconnected")

    def releasing_mtrl_box_cb(self, msg: Bool) -> None:
        """
        Handle material box release status updates.
        
        Updates the release state and marks conveyor as occupied when releasing.
        """
        new_state = bool(msg.data)
        
        with self.mutex:
            prev_state = self.is_releasing_mtrl_box
            self.is_releasing_mtrl_box = new_state
            if new_state and not prev_state:
                conveyor_id = 1  # Must be the first conveyor
                conveyor = self.conveyor.get_conveyor(conveyor_id)
                if conveyor is None:
                    self.get_logger().error(f"Conveyor {conveyor_id} not found")
                    return
                conveyor.is_occupied = True
        self.get_logger().debug(f"Material box release state: {new_state}{' (conveyor marked occupied)' if new_state else ''}")
        
        if new_state != prev_state:
            self.get_logger().info(f"Release state changed: {prev_state} -> {new_state}")

    def sliding_platform_curr_cb(self, msg: UInt8MultiArray) -> None:
        """
        Update current sliding platform positions for all dispenser stations.
        
        Args:
            msg: UInt8MultiArray message containing platform locations for each station
        """
        if len(msg.data) != self.const.NUM_DISPENSER_STATIONS:
            self.get_logger().warning(f"Message length {len(msg.data)} doesn't match stations {len(self.dis_station)}")
            return
        
        try:
            updated_stations = []
            with self.mutex:
                for station_id, platform_location in enumerate(msg.data, start=1):
                    station = self.conveyor.get_station(station_id)
                    if station is None:
                        self.get_logger().warning(f"No station found for ID {station_id} <<<< 1")
                        continue
                    station.curr_sliding_platform = platform_location
                    updated_stations.append((station_id, platform_location))

            if updated_stations:
                self.get_logger().debug(f"Updated {len(updated_stations)} sliding platforms location: {dict(updated_stations)}")
            else:
                self.get_logger().warning("No stations updated from platform message")
        except AttributeError as e:
            self.get_logger().error(f"Invalid message format: {e}")
        except Exception as e:
            self.get_logger().error(f"Error processing sliding platform message: {e}")
    
    def sliding_platform_cmd_cb(self, msg: UInt8MultiArray) -> None:
        """
        Update command sliding platform positions for all dispenser stations.
        
        Args:
            msg: UInt8MultiArray message containing platform locations for each station
        """
        if len(msg.data) != self.const.NUM_DISPENSER_STATIONS:
            self.get_logger().warning(f"Message length {len(msg.data)} doesn't match stations {len(self.dis_station)}")
            return
        
        try:
            updated_stations = []
            with self.mutex:
                for station_id, state in enumerate(msg.data, start=1):
                    station = self.conveyor.get_station(station_id)
                    if station is None:
                        self.get_logger().warning(f"No station found for ID {station_id} <<<< 2")
                        continue
                    station.cmd_sliding_platform = state
                    updated_stations.append((station_id, state))

            if updated_stations:
                self.get_logger().debug(f"Updated {len(updated_stations)} sliding platforms command: {dict(updated_stations)}")
            else:
                self.get_logger().warning("No stations updated from platform message")
        except AttributeError as e:
            self.get_logger().error(f"Invalid message format: {e}")
        except Exception as e:
            self.get_logger().error(f"Error processing sliding platform ready message: {e}")
        return

    def sliding_platform_ready_cb(self, msg: UInt8MultiArray) -> None:
        """
        Update sliding platform ready status for all dispenser stations.
        
        Args:
            msg: UInt8MultiArray message containing platform ready status for each station
        """
        if len(msg.data) != self.const.NUM_DISPENSER_STATIONS:
            self.get_logger().warning(f"Message length {len(msg.data)} doesn't match stations {len(self.dis_station)}")
            return
        
        try:
            updated_stations = []
            with self.mutex:
                for station_id, platform_ready_state in enumerate(msg.data, start=1):
                    station = self.conveyor.get_station(station_id)
                    if station is None:
                        self.get_logger().warning(f"No station found for ID {station_id} <<<< 3")
                        continue
                    station.is_platform_ready = platform_ready_state
                    updated_stations.append((station_id, platform_ready_state))

                    if station.is_platform_ready != 0:
                        self.get_logger().debug(f"Station [{station_id}] platform is ready!")
            
            if updated_stations:
                self.get_logger().debug(f"Updated {len(updated_stations)} sliding platforms ready state: {dict(updated_stations)}")
            else:
                self.get_logger().warning("No stations updated from platform message")

            self.get_logger().debug(f"Received sliding platform ready message: {msg}")
        except AttributeError as e:
            self.get_logger().error(f"Invalid message format: {e}")
        except Exception as e:
            self.get_logger().error(f"Error processing sliding platform ready message: {e}")
        return
    
    def elevator_cb(self, msg: Bool) -> None:
        """
        Handle elevator status update messages.
        
        Updates elevator ready state and manages queue when state changes from not ready to ready.
        
        Args:
            msg: Bool message containing elevator ready status
        """
        state_changed = False
        timestamp = self.get_clock().now()
        with self.mutex:
            prev_elevator_ready = self.is_elevator_ready
            self.is_elevator_ready = msg.data

            # Check for state transition (0 -> 1)
            if msg.data and not prev_elevator_ready:
                state_changed = True
                self.elevator_queue.append(timestamp)

        if state_changed:
            self.get_logger().info(f"Elevator state changed 0 -> 1 at {timestamp}. Queue size: {len(self.elevator_queue)}")
        elif msg.data != prev_elevator_ready:
            self.get_logger().debug(f"Elevator state changed 1 -> 0. Queue size: {len(self.elevator_queue)}")

    def qr_scan_cb(self, msg: CameraTrigger) -> None:
        if not (1 <= msg.camera_id <= 11):
            self.get_logger().error(f"Received undefined camera_id: {msg.camera_id}")
            return
        if not (1 <= msg.material_box_id <= 20):
            self.get_logger().error(f"Received undefined material_box_id: {msg.material_box_id}")
            return
        
        with self.mutex:
            if len(self.qr_scan) == 0:
                self.qr_scan.append(msg)
            else:
                found = False
                for scan in self.qr_scan:
                    if scan.camera_id == msg.camera_id and scan.material_box_id == msg.material_box_id:
                        found = True
                        self.get_logger().info(f"Camera {msg.camera_id} repeated the scan with box [{msg.material_box_id}]")
                        break
                if not found:
                    self.qr_scan.append(msg)
                    self.get_logger().info(f"Camera {msg.camera_id} box [{msg.material_box_id}] is append to qr_scan")
        self.get_logger().info(f"Received CameraTrigger message: camera_id={msg.camera_id}, material_box_id={msg.material_box_id}")

    def pkg_mac_status_cb(self, msg: PackagingMachineStatus) -> None:
        """
        Handle packaging machine status updates.
        
        Updates the status dictionary for valid packaging machine IDs (1 or 2).
        
        Args:
            msg: PackagingMachineStatus message containing machine status
        """
        machine_id = msg.packaging_machine_id
        if not isinstance(machine_id, int):
            self.get_logger().error(f"Invalid packaging_machine_id type: {type(machine_id).__name__}")
            return
        if machine_id not in (1, 2):
            self.get_logger().warning(f"Ignoring status update for unknown machine ID {machine_id}")
            return
        with self.mutex:
            self.pkg_mac_status[machine_id] = msg

    # Timers callback
    async def start_order_cb(self) -> None:
        with self.mutex:
            if len(self.recv_order) == 0 or not self.is_plc_connected or self.is_releasing_mtrl_box:
                return
        self.get_logger().info("The received queue stored a order")

        success = self.write_registers(5200, [2])
        if success:
            with self.mutex:
                order = self.recv_order.popleft()
                self.proc_order[order.order_id] = order

                status = self.mtrl_box_status.get(order.order_id)
                if not status:
                    status.start_time = self.get_clock().now().to_msg()
            self.get_logger().info(">>> Added a order to processing queue")
            self.get_logger().info(">>> Removed a order from received queue")
        else:
            self.get_logger().error("Failed to call the PLC to release a material box")
    
    async def order_status_cb(self) -> None:
        """
        update and publish material box status messages.
        """
        curr_time = self.get_clock().now().to_msg()
        try:
            acquired = self.mutex.acquire(timeout=1.0)
            if not acquired:
                self.get_logger().error("Failed to acquire mutex for status update")
                return
            for order_id, status in self.mtrl_box_status.items():
                status.header.stamp = curr_time
                self.mtrl_box_status_pub.publish(status)
        except Exception as e:
            self.get_logger().error(f"Error in order_status_cb: {str(e)}")
        finally:
            if self.mutex.locked():
                self.mutex.release()
        
        values = self.read_registers(5030, 1)
        self.get_logger().error(f"values: {values[0]}  {self.get_clock().now()}")

        success = self.write_registers(5014, [2])
        self.get_logger().error(f"success: {self.get_clock().now()}")

        # req = DispenseDrug.Request()
        # req.content.append(DispenseContent(unit_id=5, amount=1))
        # self.get_logger().error(f"req: {req}")
        # success = self.send_dispense_req(req, 1)
        # self.get_logger().error(f"success: {success}")

    async def elevator_dequeue_cb(self) -> None:
        if self.is_releasing_mtrl_box or len(self.elevator_queue) == 0:
            return
        
        while not self.rel_blocking_cli.wait_for_service(timeout_sec=1.0):
            if not rclpy.ok():
                return None
            self.get_logger().info("rel_blocking Service not available, waiting again...")
        
        req = Trigger.Request()
        try:
            future = await self.rel_blocking_cli.call_async(req)
            self.exec.spin_once_until_future_complete(future, timeout_sec=1.0)
            res = future.result()

            if res and res.success:
                self.get_logger().info(f"Sent the release blocking successfully")
                self.elevator_queue.popleft()
                return
            
            self.get_logger().error("Failed to send rel_blocking")

        except Exception as e:
            self.get_logger().error(f"Error writing registers: {e}")

    async def qr_handle_cb(self) -> None:
        if len(self.qr_scan) == 0:
            return
    
        self.get_logger().info(f"Started to handle the QR scan, Total: {len(self.qr_scan)}")
        qr_scan_handled = []

        for msg in self.qr_scan[:]:
            is_completed = False
            camera_id = msg.camera_id
            material_box_id = msg.material_box_id
            self.get_logger().info(f"camera id: {camera_id}, material box id: {material_box_id}")

            order_id = self.get_order_id_by_mtrl_box(material_box_id)
            if order_id is None:
                self.get_logger().info(f"The material box [{material_box_id}] does not bound to any order!")

            match camera_id:
                case 1: # QR scanner
                    camera_1_result = self.camera_1_action(order_id, material_box_id)
                    order_id = self.get_order_id_by_mtrl_box(material_box_id)
                    if camera_1_result:
                        continue
                    is_completed = await self.camera_1_to_9_action(order_id, material_box_id, camera_id) 
                case 2 | 3 | 4 | 5 | 6 | 7 | 8: # QR scanner           
                    is_completed = await self.camera_1_to_9_action(order_id, material_box_id, camera_id) 
                case 9: # vision inspection
                    is_completed = await self.camera_9_action(order_id, material_box_id)  
                case 10: # packaging machine 1
                    is_completed = await self.camera_10_action(order_id, material_box_id)      
                case 11: # packaging machine 2
                    is_completed = await self.camera_11_action(order_id, material_box_id)      
                case _:
                    self.get_logger().info(f"Received unknown camera ID: {camera_id}")
                   
            if is_completed:
                qr_scan_handled.append(msg)
        
        with self.mutex:
            for msg in qr_scan_handled:
                self.qr_scan.remove(msg)
                self.get_logger().info(f"Removed message camera id: {msg.camera_id}, box: {msg.material_box_id} ")

        self.get_logger().debug(f"Completed to handle the QR scan")

    def station_decision_cb(self, station_id: int):
        self.get_logger().debug(f"Dispenser Station [{station_id}] callback is called")
        
        station = self.conveyor.get_station(station_id)
        if station is None:
            self.get_logger().warning(f"No station found with ID: {station_id}")
            return
        if not station.is_occupied:
            self.get_logger().debug(f"Station {station_id} is not occupied")
            return
        if station.curr_mtrl_box == 0:
            self.get_logger().warning(f"Station {station_id} has no material box")
            return
        if not station.is_platform_ready:
            self.get_logger().debug(f"Station {station_id} sliding platform is not ready")
            return

        self.get_logger().warning(f"Started to make decision for Station {station_id}")

        mtrl_box_id = station.curr_mtrl_box
        order_id = self.get_order_id_by_mtrl_box(mtrl_box_id)
        order = self.proc_order.get(order_id)
        status = self.mtrl_box_status.get(order_id)
        if not status:
            self.get_logger().error(f"No material status found with order id: {order_id}")
            return
        curr_sliding_platform = station.curr_sliding_platform
        cmd_sliding_platform = station.cmd_sliding_platform
        
        try:
            target_cell = station.is_completed.index(False)
            # target_cell = self.const.EXIT_STATION
            # self.get_logger().warning(f"Debug: force to move out Station [{station_id}]")
        except ValueError:
            target_cell = self.const.EXIT_STATION

        self.get_logger().error(f"target_cell: {target_cell}")

        if target_cell == self.const.EXIT_STATION:
            success = self.move_out_station(station, station_id, mtrl_box_id)
        elif curr_sliding_platform == cmd_sliding_platform:
            success = self.dispense_action(station, station_id, status, order, target_cell)


    # Service Server callback
    def new_order_cb(self, req, res):
        order_id = req.request.order_id
        priority = req.request.priority
        
        if not isinstance(order_id, int) or not isinstance(priority, int):
            msg = f"Invalid types: order_id={type(order_id).__name__}"
            self.get_logger().error(msg)
            res.response.success = False
            res.response.message = msg
            return res
        if order_id < 0:
            msg = f"Invalid order_id: {order_id} (must be non-negative)"
            self.get_logger().error(msg)
            res.response.success = False
            res.response.message = msg
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
    
    async def move_sliding_platform_cb(self, req, res):
        """
        Move the sliding platform to a specified cell for a dispense station.
        
        Args:
            req: Request object containing cell_no and dispense_station_id
            res: Response object to be populated with results
            
        Returns:
            MoveResponse: Updated response with success status and message
        """
        cell_no = req.cell_no
        station_id = req.dispense_station_id

        if not isinstance(cell_no, int) or not isinstance(station_id, int):
            self.get_logger().error(f"Invalid parameter types: cell_no={type(cell_no).__name__}, station_id={type(station_id).__name__}")
            res.message = "Parameters must be integers"
            return res
        
        if not (0 < cell_no <= self.const.EXIT_STATION):
            self.get_logger().error(f"Invalid cell_no {cell_no}: must be between 1 and 29")
            res.message = f"Cell number {cell_no} out of range (1-29)"
            return res

        if station_id < 0:
            self.get_logger().error(f"Invalid dispense_station_id {station_id}: must be non-negative")
            res.message = f"Station ID {station_id} must be non-negative"
            return res
        
        address = self.const.MOVEMENT_ADDR + station_id
        values = [cell_no]

        success = self.write_registers(address, values)
        res.success = success

        if success:
            self.get_logger().info(f"Successfully moved sliding platform to cell {cell_no} at station {station_id}")
            res.message = "Platform movement successful"
        else:
            error_msg = f"Failed to write to register {address} with cell_no {cell_no}"
            self.get_logger().error(error_msg)
            res.message = error_msg

        return res
    
    def movement_decision_v1(self, order_id: int, station_ids: List[int]) -> Optional[int]:
        """
        Determine the next station ID for an order based on requirements and availability.
        
        Args:
            order_id: The order to process
            station_ids: List of available dispenser station IDs
            
        Returns:
            Optional[int]: Selected station ID if successful, None if no suitable station found
            
        Raises:
            TypeError: If inputs are of incorrect type
            ValueError: If order_id is negative or station_ids is empty
        """
        if not isinstance(order_id, int):
            raise TypeError(f"Expected integer order_id, got {type(order_id).__name__}")
        if not isinstance(station_ids, list) or not all(isinstance(x, int) for x in station_ids):
            raise TypeError(f"Expected list of integers for station_ids, got {type(station_ids).__name__}")
        if order_id < 0:
            raise ValueError(f"Order ID must be non-negative, got {order_id}")
        if not station_ids:
            raise ValueError("Dispenser station ID list cannot be empty")
        
        try:
            remainder = self.find_remainder(order_id)

            if remainder is None:
                return self.const.GO_STRAIGHT

            with self.mutex:
                for station_id in station_ids:
                    station = self.conveyor.get_station(station_id)
                    if station and not station.is_occupied and station_id in remainder:
                        self.get_logger().info(f"Selected station {station_id} for order {order_id}")
                        return station_id
                
            self.get_logger().info(f"No suitable station found for order {order_id} from {station_ids}")
            return self.const.GO_STRAIGHT
        except AttributeError as e:
            self.get_logger().error(f"Invalid data structure for order {order_id}: {str(e)}")
            return self.const.GO_STRAIGHT
        except ValueError as e:
            self.get_logger().error(f"Set operation error for order {order_id}: {str(e)}")
            return self.const.GO_STRAIGHT
        except Exception as e:
            self.get_logger().error(f"Unexpected error for order {order_id}: {str(e)}")
            return self.const.GO_STRAIGHT

    def remove_occupied_station(self, station_ids: List[int]) -> List[int]:
        """
        Remove occupied station IDs from the input list and return remaining IDs.
        
        Args:
            station_ids: List of station IDs to check
            
        Returns:
            List[int]: List of station IDs that are not occupied
            
        Raises:
            TypeError: If station_ids is not a list or contains non-integer values
        """
        if not isinstance(station_ids, list):
            raise TypeError(f"Expected list, got {type(station_ids).__name__}")
        if not all(isinstance(station_id, int) for station_id in station_ids):
            raise TypeError("All station IDs must be integers")
        
        result = []
        with self.mutex:
            for station_id in station_ids:
                station = self.conveyor.get_station(station_id)
                if station and not station.is_occupied:
                    result.append(station_id)

        return result

    def is_bound(self, material_box_id: int) -> bool:
        """
        Check if a material box is currently bound to any order.
        
        Args:
            material_box_id: The ID of the material box to check
            
        Returns:
            bool: True if the material box is bound, False otherwise
            
        Raises:
            TypeError: If material_box_id is not an integer
            ValueError: If material_box_id is negative
        """
        if not isinstance(material_box_id, int):
            raise TypeError(f"Expected integer id, got {type(material_box_id).__name__}")
        if material_box_id < 0:
            raise ValueError(f"Material box ID must be non-negative, got {material_box_id}")
        if material_box_id == 0:
            return False
        
        try:
            acquired = self.mutex.acquire(timeout=1.0)
            if not acquired:
                self.get_logger().error(f"Failed to acquire lock to check binding for {material_box_id}")
                return False

            for order_id, status in self.mtrl_box_status.items():
                if status.id == material_box_id:
                    self.get_logger().debug(f"Found bound material box {material_box_id} with order {order_id}")
                    return True
            return False
        except Exception as e:
            self.get_logger().error(f"Error checking binding for {material_box_id}: {str(e)}")
            return False
        finally:
            if self.mutex.locked():
                self.mutex.release()
    
    # TODO: find the highest priority order request
    def bind_mtrl_box(self, material_box_id: int) -> Optional[int]:
        """
        Bind a material box to an available order.
        
        Args:
            material_box_id: Integer ID of the material box to bind
            
        Returns:
            bool: True if binding successful, False otherwise
            
        Raises:
            TypeError: If material_box_id is not an integer
            ValueError: If material_box_id is negative
        """
        if not isinstance(material_box_id, int):
            raise TypeError(f"Expected integer id, got {type(material_box_id).__name__}")
        if material_box_id < 0:
            raise ValueError(f"Material box ID must be non-negative, got {material_box_id}")
        
        try:
            acquired = self.mutex.acquire(timeout=1.0)
            if not acquired:
                self.get_logger().error("Failed to acquire lock for material box binding")
                return None
            
            for order_id, status in self.mtrl_box_status.items():
                if status.id == 0:
                    new_status = deepcopy(status)
                    new_status.id = material_box_id
                    new_status.status = MaterialBoxStatus.STATUS_CLEANING
                    self.mtrl_box_status[order_id] = new_status
                    self.get_logger().info(f"Material box [{material_box_id}] has been bound to order [{order_id}]")
                    return order_id

            self.get_logger().info("No available material box found for binding")
            return None
        except Exception as e:
            self.get_logger().error(f"Error in bind_mtrl_box: {str(e)}")
            return None
        finally:
            if self.mutex.locked():
                self.mutex.release()

    def get_curr_gone(self, mtrl_box: MaterialBox) -> set:
        curr_gone = set()
        for cell in mtrl_box.slots:
            for drug in cell.dispensing_detail:
                station_id = drug.location.dispenser_station
                if not isinstance(station_id, int):
                    self.get_logger().error(f"Invalid station_id type: {type(station_id)}")
                    continue
                curr_gone.add(station_id)
        return curr_gone

    def get_req_to_go(self, mtrl_box: MaterialBox) -> set:
        req_to_go = set()
        for cell in mtrl_box.slots:
            for drug in cell.drugs:
                for location in drug.locations:
                    station_id = location.dispenser_station
                    if not isinstance(station_id, int):
                        self.get_logger().error(f"Invalid station_id type: {type(station_id)}")
                        continue
                    req_to_go.add(station_id)
        return req_to_go

    def find_remainder(self, order_id: int) -> Optional[set]:
        with self.mutex:
            status = self.mtrl_box_status.get(order_id)
            proc = self.proc_order.get(order_id)
            
            # if status is None or proc is None:
            #     raise KeyError(f"Order ID {order_id} not found")
            if status is None or proc is None:
                self.get_logger().error(f"Order ID {order_id} not found in status or process data")
                # return None
                raise KeyError(f"Order ID {order_id} not found")
            
            # Track existing dispenser stations in current material box
            curr_mtrl_box = status.material_box
            curr_gone = self.get_curr_gone(curr_mtrl_box)
            # Required stations
            req_mtrl_box = proc.material_box
            req_to_go = self.get_req_to_go(req_mtrl_box)
        
            self.get_logger().warning(f"Order {order_id}: Current stations {sorted(curr_gone)}, Required stations {sorted(req_to_go)}")

            return req_to_go - curr_gone 

    # def _initialize_dis_station_clis(self, cbgs: List[MutuallyExclusiveCallbackGroup]) -> None:
    def _initialize_dis_station_clis(self, cbg) -> None:
        """Initialize dispenser stations service clients."""
        for i in range(1, self.const.NUM_DISPENSER_STATIONS + 1):
            # cbg_index = int(i - 1)
            self.dis_station_clis[i] = self.create_client(
                DispenseDrug, 
                f"/dispenser_station_{i}/dispense_request",
                callback_group=cbg
            )
            # self.get_logger().info(f"Dispenser station [{i}] service client is created w/ {cbgs[cbg_index]}")
            self.get_logger().info(f"Dispenser station [{i}] service client is created")

    def _initialize_dis_station_timers(self, period: float, cbgs: List[MutuallyExclusiveCallbackGroup]) -> None:
        """Initialize dispenser stations timers."""
        for i in range(1, self.const.NUM_DISPENSER_STATIONS + 1):
            cbg_index = int(i - 1)
            self.dis_station_timers[i] = self.create_timer(
                period,
                # lambda station_id=i: run_coroutine_threadsafe(
                #     self.station_decision_cb(station_id), 
                #     self.loop
                # ).result(),
                lambda station_id=i: self.station_decision_cb(station_id),
                callback_group=cbgs[cbg_index]
            )
            self.get_logger().info(f"Dispenser station [{i}] execution timer is created")

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
        self.conveyor.append(9) # Vision
        self.conveyor.append(10) # Packaging Machine 1
        self.conveyor.append(11) # Packaging Machine 2
        self.get_logger().info(f"\n{self.conveyor}")

    def get_order_id_by_mtrl_box(self, mtrl_box_id: int) -> Optional[int]:
        """
        Get the order ID associated with a material box ID.
        
        Args:
            mtrl_box_id: The material box ID to look up
            
        Returns:
            Optional[int]: The associated order ID if found, None if not found
            
        Raises:
            TypeError: If mtrl_box_id is not an integer
            ValueError: If mtrl_box_id is negative
        """
        if not isinstance(mtrl_box_id, int):
            raise TypeError(f"Expected integer id, got {type(mtrl_box_id).__name__}")
        if mtrl_box_id < 0:
            raise ValueError(f"Material box ID must be non-negative, got {mtrl_box_id}")
        
        try:
            acquired = self.mutex.acquire(timeout=1.0)
            if not acquired:
                self.get_logger().error(f"Failed to acquire lock for mtrl_box_id {mtrl_box_id}")
                return None

            for order_id, status in self.mtrl_box_status.items():
                if mtrl_box_id == status.id:
                    self.get_logger().debug(f"Found order {order_id} for material box {mtrl_box_id}")
                    return order_id

            self.get_logger().debug(f"No order found for material box {mtrl_box_id}")
            return None
        except Exception as e:
            self.get_logger().error(f"Error getting order for mtrl_box_id {mtrl_box_id}: {str(e)}")
            return None
        finally:
            if self.mutex.locked():
                self.mutex.release()

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

            for status in self.pkg_mac_status.values():
                if status.packaging_machine_state == PackagingMachineStatus.IDLE:
                    self.get_logger().debug(f"Found idle packaging machine {status.packaging_machine_id}")
                    return True
        
        self.get_logger().warning("No idle packaging machines found")
        return False

    def get_dis_station_cli(self, station_id: int) -> Optional[rclpy.client.Client]:
        """Safely get a dispenser station client by ID."""
        return self.dis_station_clis.get(station_id)
    
    def move_out_station(self, station: DispenserStation, station_id: int, mtrl_box_id: int) -> bool:
        self.get_logger().info(f"Station {station_id} has all cells completed")
            
        conveyor = self.conveyor.get_conveyor_by_station(station_id)
        if conveyor.is_occupied:
            self.get_logger().info("The conveyor is occupied. Try to move out in the next callback")
        else:
            success = self.write_registers(self.const.MOVEMENT_ADDR + station_id, [self.const.EXIT_STATION])
            if success:
                try:
                    acquired = self.mutex.acquire(timeout=3.0)
                    if not acquired:
                        self.get_logger().error(f"Failed to acquire lock for occupancy update")
                        return False
                    
                    conveyor.is_occupied = True
                    conveyor.curr_mtrl_box = mtrl_box_id
                    station.is_occupied = False
                    station.curr_mtrl_box = 0
                    station.is_completed = [False] * self.const.CELLS
                    station.is_dispense_req_sent = [False] * self.const.CELLS
                    return True
                finally:
                    if self.mutex.locked():
                        self.mutex.release()
                self.get_logger().info(f"The material box [{mtrl_box_id}] is moving out in station [{station_id}]")
            else:
                self.get_logger().info(f"Failed to move out material box [{mtrl_box_id}] in station [{station_id}]")

        return False

    def dispense_action(self, 
                        station: DispenserStation, 
                        station_id: int, 
                        status: MaterialBoxStatus, 
                        order: OrderRequest, 
                        start_cell: int) -> bool:
        """
        Process dispensing operations for a specific station across material box cells.
        
        Args:
            station: DispenserStation object tracking completion status
            station_id: ID of the dispensing station
            status: MaterialBoxStatus containing current box state
            order: OrderRequest containing required drugs
            start_cell: Starting cell index
            
        Returns:
            bool: True if dispensing completed successfully, False otherwise
        """
        if not isinstance(station, DispenserStation) or \
           not isinstance(status, MaterialBoxStatus) or \
           not isinstance(order, OrderRequest):
            self.get_logger().error("Invalid object types provided")
            return False
        if not isinstance(station_id, int) or station_id < 0:
            self.get_logger().error(f"Invalid station_id: {station_id}")
            return False
        if not (0 <= start_cell < self.const.CELLS):
            self.get_logger().error(f"Invalid start_cell: {start_cell}")
            return False

        target_cell = start_cell
        while target_cell < self.const.CELLS:
            self.get_logger().error(f"Checking target_cell: {target_cell}")
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
            self.get_logger().warning(f"required: {required}")
            self.get_logger().warning(f"curr: {curr}")

            missing_sets = [req_set for req_set in required if req_set not in curr]
            missing_lists = [list(req_set) for req_set in missing_sets]

            self.get_logger().warning(f"missing_sets: {missing_sets}")
            self.get_logger().warning(f"missing_lists: {missing_lists}")

            filtered_missing = [
                [item for item in lst if item[0] == station_id]
                for lst in missing_lists
                if any(item[0] == station_id for item in lst) 
            ]
            self.get_logger().debug(f"filtered_missing: {filtered_missing}")

            if filtered_missing:
                filtered_missing = filtered_missing[0]

            if filtered_missing:
                self.get_logger().error(f"target_cell: {target_cell} has missing drugs: {filtered_missing}")
                break
            else:
                with self.mutex:
                    station.is_completed[target_cell] = True
                self.get_logger().info(f"Set target_cell: {target_cell} to True")
                self.get_logger().error(f"target_cell: {target_cell} is complete")
                target_cell += 1

        if target_cell >= self.const.CELLS:
            self.get_logger().error(f"The station is completed.")
            return False
        
        self.get_logger().error(f"station.cmd: {station.cmd_sliding_platform}")
        self.get_logger().error(f"station.curr: {station.curr_sliding_platform}")
        self.get_logger().error(f"target_cell + 1: {target_cell + 1}")
        
        write_success = self.write_registers(self.const.MOVEMENT_ADDR + station_id, [target_cell + 1])
        if not write_success or station.cmd_sliding_platform == 0:
            self.get_logger().error(f"write_registers movement failed")
            return False
        
        if station.cmd_sliding_platform > station.curr_sliding_platform:
            self.get_logger().error(f"THe station {[station_id]} movement does not ready")
            return False

        if station.is_dispense_req_sent[target_cell]:
            self.get_logger().error(f"THe station {[station_id]} dispense request sent!")
            return False

        self.get_logger().error(f"Try to send the dispense request")

        req = DispenseDrug.Request()
        for item in filtered_missing:
            req.content.append(DispenseContent(unit_id=item[0], amount=item[1]))
        self.get_logger().error(f"req: {req}")

        try:
            self.get_logger().error(f"start to send")

            with self.mutex:
                station.is_dispense_req_sent[target_cell] = True
            values = self.read_registers(5030, 1)
            success = self.send_dispense_req(req, station_id)
            self.get_logger().error(f"values: {values[0]}  {self.get_clock().now()}")
            self.get_logger().info("Called the Dispense service")

            if success:
                with self.mutex:
                    station.is_completed[target_cell] = True
                    self.get_logger().info(f"Set target_cell: {target_cell} to True")
                    for item in filtered_missing: 
                        new_detail = DispensingDetail()
                        new_detail.location.dispenser_station = station_id
                        new_detail.location.dispenser_unit = item[0]
                        new_detail.amount = item[1]
                        try:
                            status.material_box.slots[target_cell].dispensing_detail.append(new_detail)
                        except Exception as e:
                            self.get_logger().error(f"Can't add drug to status")
                        self.get_logger().info(f"Added a new drug to status >>> {new_detail}")
                        self.get_logger().info(f"detail: >>> {status.material_box.slots[target_cell].dispensing_detail}")

                self.get_logger().info(f"Dispense completed for station {station_id}, cell {target_cell}")
                target_cell += 1
            else:
                self.get_logger().error(f"Dispense failed for station {station_id}, cell {target_cell}")
                return False
            return True
        except KeyError:
            self.get_logger().error(f"No service client for station {station_id}")
            return False
        except Exception as e:
            self.get_logger().error(f"Dispense error for station {station_id}: {str(e)}")
            return False
    
    def camera_1_action(self, order_id: Optional[int | None], material_box_id: int) -> Optional[bool]:
        if not isinstance(material_box_id, int):
            raise TypeError(f"Expected integer id, got {type(material_box_id).__name__}")

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
    
    async def camera_1_to_9_action(self, order_id: int, material_box_id: int, camera_id: int) -> Optional[bool]:
        """
        Process operations for camera 1 to 9 for a given order and material box.
        
        Args:
            order_id: The order being processed
            material_box_id: The material box ID to handle
            camera_id: The camera ID 
            
        Returns:
            bool: True if operation completed successfully, False otherwise
        """
        if not isinstance(order_id, int):
            raise TypeError(f"Expected integer id, got {type(order_id).__name__}")
        if not isinstance(material_box_id, int):
            raise TypeError(f"Expected integer id, got {type(material_box_id).__name__}")
        if not isinstance(camera_id, int):
            raise TypeError(f"Expected integer id, got {type(camera_id).__name__}")
        if camera_id not in self.const.CAMERA_STATION_PLC_MAP:
            self.get_logger().error(f"No station mapping for camera {camera_id}")
            return None
        
        is_completed = False

        station_ids, register_addr = self.const.CAMERA_STATION_PLC_MAP[camera_id]
        available_stations = self.remove_occupied_station(station_ids)

        decision = self.movement_decision_v1(order_id, available_stations)
        self.get_logger().debug(f"Decision {decision} made from available stations {available_stations} for camera {camera_id}")
        
        if decision in available_stations: 
            # try to move into station
            values = self.const.STATION_VALUE_MAP.get(decision)
            
            success = self.write_registers(register_addr, values)
            if success:
                station = self.conveyor.get_station(decision)
                curr_conveyor = self.conveyor.get_conveyor(camera_id)

                if station and curr_conveyor:
                    try:
                        acquired = self.mutex.acquire(timeout=1.0)
                        if not acquired:
                            self.get_logger().error("Failed to acquire mutex for occupancy update")
                            return False
                        
                        station.is_occupied = True
                        station.curr_mtrl_box = material_box_id
                        curr_conveyor.is_occupied = False
                        curr_conveyor.curr_mtrl_box = 0
                        is_completed = True
                    finally:
                        if self.mutex.locked():
                            self.mutex.release()
            else:
                self.get_logger().error(f"Failed to write to register {register_addr}")
        else: 
            # try to go straight
            next_conveyor = self.conveyor.get_next_conveyor(camera_id)

            if next_conveyor and not next_conveyor.is_occupied:
                values = [1]

                success = self.write_registers(register_addr, values)
                if success:
                    curr_conveyor = self.conveyor.get_conveyor(camera_id)
                    if curr_conveyor:
                        try:
                            acquired = self.mutex.acquire(timeout=1.0)
                            if not acquired:
                                self.get_logger().error("Failed to acquire mutex for occupancy update")
                                return False
                            
                            next_conveyor.is_occupied = True
                            next_conveyor.curr_mtrl_box = material_box_id
                            curr_conveyor.is_occupied = False
                            curr_conveyor.curr_mtrl_box = 0
                            is_completed = True
                        finally:
                            if self.mutex.locked():
                                self.mutex.release()
                else:
                    self.get_logger().error(f"Failed to write to register {register_addr}")
            else:
                self.get_logger().warning(f"The next conveyor is unavailable for camera [{camera_id}]")

        self.get_logger().warning(f"decision [{decision}] / {station_ids} is made for material box [{material_box_id}] in camera [{camera_id}]")
        return is_completed
    
    async def camera_9_action(self, order_id: int, material_box_id: int) -> Optional[bool]:
        is_completed = True

        self.get_logger().info(f"Vision inspection triggered for material box [{material_box_id}]")
        return is_completed
        
    async def camera_10_action(self, order_id: int, material_box_id: int) -> Optional[bool]:
        """
        Process operations for camera 10 for a given order and material box.
        
        Args:
            order_id: The order being processed
            material_box_id: The material box ID to handle
            
        Returns:
            bool: True if operation completed successfully, False otherwise
        """
        is_completed = False

        remainder = self.find_remainder(order_id)
        if len(remainder) == 0 and self.get_any_pkc_mac_is_idle():
            pkg_req_success = await self.send_pkg_req(order_id)
            if pkg_req_success:
                self.get_logger().warning(f"Sent a packaging request to packaging machine manager successfully")
                self.proc_order.pop(order_id)
                self.get_logger().warning(f"Removed the order in the proc_order successfully")
                is_completed = True
            else:
                self.get_logger().error(f"Failed to send the packaging request to packaging machine manager") 
        else:
            block_success = await self.send_income_mtrl_box(material_box_id)
            if block_success:
                self.get_logger().warning(f"Sent a income materail box to packaging machine manager successfully")
            else:
                self.get_logger().error(f"Failed to send the income materail box to packaging machine manager")
        
            elevator_success = False

            if self.is_releasing_mtrl_box:
                self.get_logger().debug(f"Skipped elevator movement for box {material_box_id} (currently releasing)")
            else: 
                elevator_success = self.write_registers(5200, [1])
                if elevator_success:
                    is_completed = True
                    self.get_logger().warning(f"Sent elevator request successfully")
                else:
                    self.get_logger().error(f"Failed to write to register for elevator")

            is_completed = block_success and (elevator_success if not self.is_releasing_mtrl_box else True)

        self.get_logger().info(f"Packaging machine 1 triggered for material box [{material_box_id}]")    
        return is_completed
    
    async def camera_11_action(self, order_id: int, material_box_id: int) -> Optional[bool]:
        """
        Process operations for camera 11 for a given order and material box.
        
        Args:
            order_id: The order being processed
            material_box_id: The material box ID to handle
            
        Returns:
            bool: True if operation completed successfully, False otherwise
        """
        is_completed = False

        elevator_success = False

        if self.is_releasing_mtrl_box:
            self.get_logger().debug(f"Skipped elevator movement for box {material_box_id} (currently releasing)")
        else: 
            elevator_success = self.write_registers(5200, [1])
            if elevator_success:
                is_completed = True
                self.get_logger().warning(f"Sent elevator request successfully")
            else:
                self.get_logger().error(f"Failed to write to register for elevator")
        
        success = await self.send_con_mtrl_box(material_box_id)
        is_completed = success and elevator_success

        self.get_logger().info(f"Packaging machine 2 triggered for material box [{material_box_id}]")
        return is_completed

    async def send_income_mtrl_box(self, mtrl_box_id: int) -> Optional[bool]:
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
        
        req = UInt8.Request()
        req.data = mtrl_box_id

        while not self.income_mtrl_box_cli.wait_for_service(timeout_sec=1.0):
            if not rclpy.ok():
                return None
            self.get_logger().info("income_mtrl_box Service not available, waiting again...")

        try:
            res = await self.income_mtrl_box_cli.call_async(req)
            # future = self.income_mtrl_box_cli.call_async(req)
            # self.exec.spin_once_until_future_complete(future, timeout_sec=1.0)
            # res = future.result()
  
            if res and res.success:
                self.get_logger().info(f"Successfully sent material box ID: {mtrl_box_id}")
                return True
        
            self.get_logger().error(f"Service call succeeded but reported failure for ID: {mtrl_box_id}")
            return False

        except AttributeError as e:
            self.get_logger().error(f"Invalid service response for ID {mtrl_box_id}: {str(e)}")
            return False
        except Exception as e:
            self.get_logger().error(f"Service call failed for ID {mtrl_box_id}: {str(e)}")
            return False

    async def send_pkg_req(self, order_id) -> Optional[bool]:
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
        
        proc_order = self.proc_order.get(order_id)
        status = self.mtrl_box_status.get(order_id)
        if proc_order is None or status is None:
            raise ValueError(f"Order {order_id} not found in process or status data")
        
        req = PackagingOrder.Request()
        req.order_id = order_id
        req.material_box_id = status.id
        req.requester_id = 1234

        MEAL_TIME = {
            OrderRequest.MEAL_MORNING: "Morning",
            OrderRequest.MEAL_NOON: "Noon",
            OrderRequest.MEAL_AFTERNOON: "Afternoon",
            OrderRequest.MEAL_EVENING: "Evening"
        }

        for i, info in enumerate(req.print_info):
            if not proc_order.material_box.slots or i >= len(proc_order.material_box.slots):
                continue
            if not proc_order.material_box.slots[i].drugs:
                continue

            info.cn_name = proc_order.patient.institute_name
            info.en_name  = proc_order.patient.name

            curr_meal = (proc_order.start_meal + i) % 4
            info.time = MEAL_TIME.get(curr_meal, "Unknown")

            _date = proc_order.start_date
            try:
                dt = datetime.strptime(_date, "%Y-%m-%d")
                days_to_add = (proc_order.start_meal + i) // 4  # integer division
                new_date = dt + timedelta(days=days_to_add)     # Add the days to the original datetime
                info.date = f"Date: {new_date.strftime('%Y-%m-%d')}"
            except ValueError as e:
                self.get_logger().error(f"Invalid date format for order {order_id}: {str(e)}")

            info.qr_code = "https://www.hkclr.hk"

            for drug in proc_order.material_box.slots[i].drugs:
                drug_str = f"{drug.name}   {drug.amount}"
                self.get_logger().info(f"Added to order {order_id}: {drug_str}")
                req.print_info.drugs.append(drug_str)

        while not self.pkg_order_cli.wait_for_service(timeout_sec=1.0):
            if not rclpy.ok():
                return None
            self.get_logger().info("packaging_order Service not available, waiting again...")

        try:
            res = await self.pkg_order_cli.call_async(req)
            # future = self.pkg_order_cli.call_async(req)
            # self.exec.spin_once_until_future_complete(future, timeout_sec=1.0)
            # res = future.result()
  
            if res and res.success:
                self.get_logger().info(f"Packaging request successful for order {order_id}")
                return True
        
            self.get_logger().error(f"Packaging service failed for order {order_id}")
            return False

        except AttributeError as e:
            self.get_logger().error(f"Invalid service response for order id {order_id}: {str(e)}")
            return False
        except ValueError as e:
            return False  
        except Exception as e:
            self.get_logger().error(f"Service call failed for order id {order_id}: {str(e)}")
            return False

    async def send_con_mtrl_box(self, mtrl_box_id: int) -> Optional[bool]:
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
        
        req = UInt8.Request()
        req.data = mtrl_box_id

        while not self.con_mtrl_box_cli.wait_for_service(timeout_sec=1.0):
            if not rclpy.ok():
                return None
            self.get_logger().info("container_material_box Service not available, waiting again...")

        try:
            res = await self.con_mtrl_box_cli.call_async(req)
            # future = self.con_mtrl_box_cli.call_async(req)
            # self.exec.spin_once_until_future_complete(future, timeout_sec=1.0)
            # res = future.result()
  
            if res and res.success:
                self.get_logger().info(f"Successfully sent material box ID: {mtrl_box_id}")
                return True
        
            self.get_logger().error(f"Service call succeeded but reported failure for ID: {mtrl_box_id}")
            return False

        except AttributeError as e:
            self.get_logger().error(f"Invalid service response for ID {mtrl_box_id}: {str(e)}")
            return False
        except Exception as e:
            self.get_logger().error(f"Service call failed for ID {mtrl_box_id}: {str(e)}")
            return False

    def send_dispense_req(self, req: DispenseDrug.Request, station_id: int) -> Optional[bool]:
        self.get_logger().info("dis_station_clis start")
        if not isinstance(req, DispenseDrug.Request):
            raise TypeError(f"Expected DispenseDrug.Request(), got {type(req).__name__}")

        while not self.get_dis_station_cli(station_id).wait_for_service(timeout_sec=1.0):
            if not rclpy.ok():
                return None
            self.get_logger().info("container_material_box Service not available, waiting again...")

        try:
            self.get_logger().info("dis_station_clis call")
            future = self.get_dis_station_cli(station_id).call_async(req)
            self.exec.spin_until_future_complete(future)
            res = future.result()
            self.get_logger().info("dis_station_clis called")
  
            if res and res.success:
                self.get_logger().info(f"Successfully sent dispenser station: {station_id}")
                return True
        
            self.get_logger().error(f"Service call succeeded but reported failure for dispenser station: {station_id}")
            return False

        except AttributeError as e:
            self.get_logger().error(f"Invalid service response {station_id}: {str(e)}")
            return False
        except Exception as e:
            self.get_logger().error(f"Service call failed {station_id}: {str(e)}")
            return False

    # async def read_registers(self, address: int, count: int) -> Optional[array]:
    def read_registers(self, address: int, count: int) -> Optional[array]:
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
        
        req = ReadRegister.Request()
        req.address = address
        req.count = count

        while not self.read_cli.wait_for_service(timeout_sec=1.0):
            if not rclpy.ok():
                return None
            self.get_logger().info("read_registers Service not available, waiting again...")

        try:
            # res = await self.read_cli.call_async(req)
            future = self.read_cli.call_async(req)
            self.exec.spin_until_future_complete(future, timeout_sec=1.0)
            res = future.result()

            if res.success:
                if not isinstance(res.values, array):
                    self.get_logger().error("Invalid response format: values not a list")
                    return None

                self.get_logger().debug(f"Successfully read {len(res.values)} registers from address {address}")
                return res.values
            
            self.get_logger().error(f"Service reported failure reading registers at {address}")
            return None

        except AttributeError as e:
            self.get_logger().error(f"Invalid response format for address {address}: {str(e)}")
            return None
        except Exception as e:
            self.get_logger().error(f"Failed to read registers at {address}: {str(e)}")
            return None
  
    def write_registers(self, address: int, values: List[int]) -> bool:
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
        
        req = WriteRegister.Request()
        req.address = address
        req.values = values

        while not self.write_cli.wait_for_service(timeout_sec=1.0):
            if not rclpy.ok():
                return None
            self.get_logger().info("write_registers Service not available, waiting again...")

        try:
            # res = await self.write_cli.call_async(req)
            future = self.write_cli.call_async(req)
            self.exec.spin_until_future_complete(future, timeout_sec=1.0)
            res = future.result() 

            if res and res.success:
                self.get_logger().debug(f"Successfully wrote {len(values)} registers at address {address}: {values}")
                return True
                
            self.get_logger().error(f"Service reported failure writing {len(values)} registers at {address}")
            return False

        except AttributeError as e:
            self.get_logger().error(f"Invalid response format for address {address}: {str(e)}")
            return False
        except Exception as e:
            self.get_logger().error(f"Failed to write registers at {address}: {str(e)}")
            return False
    
    # Action Server Callbacks
    def handle_accepted_cb(self, goal_handle):
        """Start or defer execution of an already accepted goal."""
        with self.mutex:
            if self._curr_goal is not None:
                # Put incoming goal in the queue
                self._goal_queue.append(goal_handle)
                self.get_logger().info("Goal put in the queue")
            else:
                # Start goal execution right away
                self._curr_goal = goal_handle
                self._curr_goal.execute()

    def goal_cb(self, goal_request):
        """Accept or reject a client request to begin an action."""
        self.get_logger().info("Received a goal, try to proceess the goal")

        # TODO: try to valid the acceptance later
        if False:
            return GoalResponse.REJECT

        return GoalResponse.ACCEPT

    def cancel_cb(self, goal_handle):
        """Accept or reject a client request to cancel an action."""
        self.get_logger().info("Received cancel request")
        return CancelResponse.ACCEPT

    async def execute_cb(self, goal_handle):
        """Execute a goal for the NewOrderAction."""
        try:
            goal = goal_handle.get_goal()
            req = goal.request
            result = NewOrderAction.Result()
            feedback_msg = NewOrderAction.Feedback()
            
            order_id = req.order_id
            priority = req.priority

            self.get_logger().info(f"Received NewOrder by action: order_id={order_id}, priority={priority}")

            msg = MaterialBoxStatus(
                creation_time=self.get_clock().now().to_msg(),
                id=0,
                status=MaterialBoxStatus.STATUS_INITIALIZING,
                priority=priority,
                order_id=order_id
            )

            with self.mutex:
                self.mtrl_box_status[order_id] = msg
                self.recv_order.append(req)

            feedback_msg.running = True
            retries = 0
            MAX_RETRY = 600

            for _ in range(1, MAX_RETRY):
                if not rclpy.ok():
                    self.get_logger().warn("ROS2 shutdown detected, aborting goal")
                    break

                if goal_handle.is_cancel_requested:
                    goal_handle.canceled()
                    self.get_logger().info(f"Goal canceled for order_id={order_id}")
                    return result

                status = self.mtrl_box_status.get(order_id)
                if status is None:
                    self.get_logger().error(f"Status for order_id={order_id} not found")
                    result.response.success = False
                    result.response.message = "Order status lost"
                    break

                if status.id != 0:
                    result.response.material_box_id = status.id
                    result.response.success = True
                    break

                self.get_logger().warning(f"Waiting for material box ID for order_id={order_id} ({retries} retries)...")
                goal_handle.publish_feedback(feedback_msg)
                retries += 1
                self.waiting_result.sleep(1)

            if retries >= MAX_RETRY:
                result.response.success = False
                result.response.message = "Material box assignment timed out"
                self.get_logger().error(f"Timeout after {retries} retries for order_id={order_id}")

            goal_handle.succeed()
            return result

        finally:
            with self.mutex:
                try:
                    # Start execution of the next goal in the queue.
                    self._curr_goal = self._goal_queue.popleft()
                    self.get_logger().info("Next goal pulled from the queue")
                    self._curr_goal.execute()
                except IndexError:
                    # No goal in the queue.
                    self._curr_goal = None

    def run_loop(self):
        """Run the asyncio event loop in a dedicated thread."""
        asyncio.set_event_loop(self.loop)
        self.get_logger().info("Started the event loop.")
        self.loop.run_forever()

    def destroy_node(self):
        self.new_order_action_ser.destroy()

        if self.loop.is_running():
            self.loop.call_soon_threadsafe(self.loop.stop)
            self.get_logger().info("Removed loop successfully")

        if self.loop_thread is not None:
            self.loop_thread.join()
            self.get_logger().info("Removed loop thread successfully")

        super().destroy_node()


def main(args=None):
    rclpy.init(args=args)
    try:
        executor = MultiThreadedExecutor()
        core_sys = CoreSystem(executor)
        executor.add_node(core_sys)
        try:
            executor.spin()
        finally:
            executor.shutdown()
            core_sys.destroy_node()
    except KeyboardInterrupt:
        pass
    except ExternalShutdownException:
        sys.exit(1)
    finally:
        rclpy.try_shutdown()

if __name__ == "__main__":
    main()

