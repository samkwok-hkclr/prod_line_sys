import sys
import asyncio
from collections import deque
from threading import Thread, Lock
from typing import Dict, List, Optional
from queue import Queue
from copy import deepcopy
import rclpy

from rclpy.node import Node
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
                            MaterialBox)
from smdps_msgs.srv import NewOrder, ReadRegister, WriteRegister, MoveSlidingPlatform, DispenseDrug, UInt8

from .dispenser_station import DispenserStation
from .conveyor_segment import ConveyorSegment
from .conveyor import Conveyor

class Manager(Node):
    NUM_DISPENSER_STATIONS = 14
    CANMERA_STATION_PLC_MAP = {
        1: ([1, 2],   5201),
        2: ([3, 4],   5202),
        3: ([5, 6],   5203),
        4: ([7],      5204),
        5: ([8],      5208),
        6: ([9, 10],  5205),
        7: ([11, 12], 5206),
        8: ([13, 14], 5207)
    }
    STATION_VALUE_MAP = {
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

    def __init__(self, executor):
        super().__init__("prod_line_manage")

        # Local memory storage
        self.recv_order = deque()
        self.proc_order: Dict[int, OrderRequest] = {} # order_id, OrderRequest
        self.mtrl_box_status: Dict[int, MaterialBoxStatus] = {} # order_id, MaterialBoxStatus
        self.qr_scan: List[CameraTrigger] = [] 
        self.elevator_queue = deque()

        self.mutex = Lock()

        # Graph of conveyor structure
        self.conveyor = None
        self._initialize_conveyor()

        # handle async functions
        self.loop = asyncio.new_event_loop()
        self.loop_thread = Thread(target=self.run_loop, daemon=True)
        self.loop_thread.start()
        self.mutex = Lock()

        # State of PLC registers
        self.is_plc_connected = False
        self.is_releasing_mtrl_box = False
        self.is_elevator_ready = False
        # maybe unused
        self.executor = executor

        # Callback groups
        sub_cbg = MutuallyExclusiveCallbackGroup()
        srv_ser_cbg = MutuallyExclusiveCallbackGroup()
        srv_cli_cbg = MutuallyExclusiveCallbackGroup()
        station_srv_cli_cbgs = [MutuallyExclusiveCallbackGroup() for _ in range(self.NUM_DISPENSER_STATIONS)]

        normal_timer_cbg = MutuallyExclusiveCallbackGroup()
        qr_handle_timer_cbg = MutuallyExclusiveCallbackGroup()
        station_timer_cbgs = [MutuallyExclusiveCallbackGroup() for _ in range(self.NUM_DISPENSER_STATIONS)]
        self.get_logger().info("Callback groups are created")

        # Publishers
        self.mtrl_box_status_pub = self.create_publisher(MaterialBoxStatus, "material_box_status", 10)
        self.get_logger().info("Publishers are created")

        # Subscriptions
        self.plc_conn_sub = self.create_subscription(Bool, "plc_connection", self.plc_conn_cb, 10, callback_group=sub_cbg)
        self.rel_mtrl_box_sub = self.create_subscription(Bool, "releasing_material_box", self.releasing_mtrl_box_cb, 10, callback_group=sub_cbg)
        self.sliding_platform_curr_sub = self.create_subscription(UInt8MultiArray, "sliding_platform", self.sliding_platform_curr_cb, 10, callback_group=sub_cbg)
        self.sliding_platform_cmd_sub = self.create_subscription(UInt8MultiArray, "sliding_platform_cmd", self.sliding_platform_cmd_cb, 10, callback_group=sub_cbg)
        self.sliding_platform_ready_sub = self.create_subscription(UInt8MultiArray, "sliding_platform_ready", self.sliding_platform_ready_cb, 10, callback_group=sub_cbg)
        self.elevator_sub = self.create_subscription(Bool, "elevator", self.elevator_cb, 10, callback_group=sub_cbg)
        self.qr_scan_sub = self.create_subscription(CameraTrigger, "qr_camera_scan", self.qr_scan_cb, 10, callback_group=sub_cbg)
        self.get_logger().info("Subscriptions are created")
        
        # Service Servers
        self.new_order_srv = self.create_service(NewOrder, "new_order", self.new_order_cb, callback_group=srv_ser_cbg)
        self.move_sliding_platform = self.create_service(MoveSlidingPlatform, "move_sliding_platform", self.move_sliding_platform_cb, callback_group=srv_ser_cbg)
        self.get_logger().info("Service Servers are created")
        
        # Service Clients
        self.read_cli = self.create_client(ReadRegister, "read_register", callback_group=srv_cli_cbg)
        self.write_cli = self.create_client(WriteRegister, "write_register", callback_group=srv_cli_cbg)
        self.income_mtrl_box_cli = self.create_client(UInt8, "income_material_box", callback_group=srv_cli_cbg)
        self.rel_blocking_cli = self.create_client(Trigger, "release_blocking", callback_group=srv_cli_cbg)
        self.dis_station_clis: Dict[int, rclpy.client.Client] = {} # station_id, Client
        self._initialize_dis_station_clis(station_srv_cli_cbgs)
        self.get_logger().info("Service Clients are created")

        # Timers
        self.qr_handle_timer = self.create_timer(0.5, self.qr_handle_cb, callback_group=qr_handle_timer_cbg)
        self.start_order_timer = self.create_timer(1.0, self.start_order_cb, callback_group=normal_timer_cbg)
        self.order_status_timer = self.create_timer(1.0, self.order_status_cb, callback_group=normal_timer_cbg)
        self.elevator_timer = self.create_timer(
            1.0, 
            lambda: asyncio.run_coroutine_threadsafe(self.elevator_dequeue_cb(), self.loop).result(),
            callback_group=normal_timer_cbg)
        self.dis_station_timers: Dict[int, rclpy.Timer.Timer] = {}
        self._initialize_dis_station_timers(0.5, station_timer_cbgs)
        self.get_logger().info("Timers are created")
        
        self.get_logger().info("Produation Line Manager Node started")

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
                conveyor = self.get_conveyor(conveyor_id)
                if conveyor is None:
                    self.get_logger().error(f"Conveyor {conveyor_id} not found")
                    return
                conveyor.is_occupied = True
        self.get_logger().info(f"Material box release state: {new_state}{' (conveyor marked occupied)' if new_state else ''}")
        
        if new_state != prev_state:
            self.get_logger().info(f"Release state changed: {prev_state} -> {new_state}")

    def sliding_platform_curr_cb(self, msg: UInt8MultiArray) -> None:
        """
        Update current sliding platform positions for all dispenser stations.
        
        Args:
            msg: UInt8MultiArray message containing platform locations for each station
        """
        if len(msg.data) != self.NUM_DISPENSER_STATIONS:
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
        if len(msg.data) != self.NUM_DISPENSER_STATIONS:
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
        if len(msg.data) != self.NUM_DISPENSER_STATIONS:
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

            # Check for state transition (False -> True)
            if msg.data and not prev_elevator_ready:
                state_changed = True
                self.elevator_queue.append(timestamp)

            # Logging outside mutex to avoid holding lock longer than necessary
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

    # Timers callback
    async def start_order_cb(self) -> None:
        with self.mutex:
            if len(self.recv_order) == 0 or not self.is_plc_connected or self.is_releasing_mtrl_box:
                return
        self.get_logger().info("The received queue stored a order")

        success = await self.write_registers(5200, [2])
        if success:
            with self.mutex:
                order = self.recv_order.popleft()
                self.proc_order[order.order_id] = order

                status = self.mtrl_box_status[order.order_id]
                status.start_time = self.get_clock().now().to_msg()
            self.get_logger().info(">>> Added a order to processing queue")
            self.get_logger().info(">>> Removed a order from received queue")
        else:
            self.get_logger().error("Failed to call the PLC to release a material box")
    
    def order_status_cb(self) -> None:
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

    async def elevator_dequeue_cb(self) -> None:
        if self.is_releasing_mtrl_box or len(self.elevator_queue) == 0:
            return
        
        while not self.rel_blocking_cli.wait_for_service(timeout_sec=1.0):
            if not rclpy.ok():
                return None
            self.get_logger().info("rel_blocking Service not available, waiting again...")
        
        req = Trigger.Request()
        try:
            future = self.rel_blocking_cli.call_async(req)
            await future
            res = future.result()
            if res.success:
                self.get_logger().info(f"release blocking successfully")
                self.elevator_queue.popleft()
                return True
            else:
                self.get_logger().error("Failed to send rel_blocking")
                return False
        except Exception as e:
            self.get_logger().error(f"Error writing registers: {e}")
            return False

    async def qr_handle_cb(self) -> None:
        to_remove = []
        total = len(self.qr_scan)
        if total == 0:
            return
        else:
            self.get_logger().info(f"Started to handle the QR scan, Total: {total}")

        for msg in self.qr_scan[:]:
            is_completed = False
            camera_id = msg.camera_id
            material_box_id = msg.material_box_id
            self.get_logger().info(f"camera id: {camera_id}, material box id: {material_box_id}")

            order_id = self.get_order_id_by_mtrl_box(material_box_id)
            if order_id == 0:
                self.get_logger().info(f"The material box [{material_box_id}] does not bound to any order!")

            match camera_id:
                case 1 | 2 | 3 | 4 | 5 | 6 | 7 | 8:
                    decide_station_ids, register_addr = self.CANMERA_STATION_PLC_MAP[camera_id]

                    if camera_id == 1 and not self.is_bound(material_box_id):
                        self.get_logger().error(f"The material box [{material_box_id}] does not bound")
                        success = self.bind_mtrl_box(material_box_id)
                        if success:
                            self.get_logger().error(f"The material box [{material_box_id}] bound to order id [{order_id}]")
                        else:
                            self.get_logger().warning(f"Failed to bind material box {material_box_id}")
                            continue

                    decide_station_ids = self.remove_occupied_station(decide_station_ids)
                    decision = self.movement_decision_v1(order_id, decide_station_ids)
                    
                    if decision in decide_station_ids:
                        values = self.STATION_VALUE_MAP[decision]
                        success = await self.write_registers(register_addr, values)
                        if success:
                            if station := self.conveyor.get_station(decision):
                                with self.mutex:
                                    station.is_occupied = True
                                    station.curr_mtrl_box = material_box_id
                                    is_completed = True
                        else:
                            self.get_logger().error(f"Failed to write to register {register_addr}")
                    else:
                        next_conveyor = self.conveyor.get_next_conveyor(camera_id)
                        if next_conveyor and not next_conveyor.is_occupied:
                            values = [1]
                            success = await self.write_registers(register_addr, values)
                            if success:
                                if conveyor := self.conveyor.get_conveyor(camera_id):
                                    with self.mutex:
                                        conveyor.is_occupied = True
                                        conveyor.curr_mtrl_box = material_box_id
                                        if camera_id != 1:
                                            conveyor.is_occupied = False
                                            conveyor.curr_mtrl_box = 0
                                    is_completed = True
                            else:
                                self.get_logger().error(f"Failed to write to register {register_addr}")
                        else:
                            self.get_logger().warning(f"The next conveyor is unavailable for camera [{camera_id}]")
                    
                    self.get_logger().warning(f"decision [{decision}] / {decide_station_ids} is made for material box [{material_box_id}] in camera [{camera_id}]")
                case 9:
                    # vision inspection
                    is_completed = True
                    self.get_logger().info(f"Vision inspection triggered for material box [{material_box_id}]")
                    pass
                case 10:
                    # packaging machine 1
                    # if order is completed
                    # send pkg req
                    # else do the following
                    success = await self.send_income_mtrl_box(material_box_id)
                    if success:
                        is_completed = True
                        self.get_logger().warning(f"Sent a income materail box to packaging machine manager successfully")
                    else:
                        self.get_logger().error(f"Failed to send the income materail box to packaging machine manager")
                    self.get_logger().info(f"Packaging machine 1 triggered for material box [{material_box_id}]")            
                case 11:
                    # packaging machine 2
                    if not self.is_releasing_mtrl_box:
                        self.get_logger().error(f"A material box is passing the packaging machine 2")
                        address = 5200
                        values = [1]
                        success = await self.write_registers(address, values)
                        if success:
                            is_completed = True
                            self.get_logger().warning(f"retrieve material box successfully")
                        else:
                            self.get_logger().error(f"Failed to write to register {address}")
                    self.get_logger().info(f"Packaging machine 2 triggered for material box [{material_box_id}]")
                    pass
                case _:
                    self.get_logger().info(f"Received unknown camera ID: {camera_id}")
            # end of match case        
            if is_completed:
                to_remove.append(msg)

        # end of for loop self.qr_scan
        with self.mutex:
            for msg in to_remove:
                self.qr_scan.remove(msg)
                self.get_logger().info(f"Removed message camera id: {msg.camera_id}, box: {msg.material_box_id} ")

        self.get_logger().debug(f"Completed to handle the QR scan")

    async def station_decision_cb(self, station_id: int) -> None:
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
            self.get_logger().warning(f"Station {station_id} sliding platform is not ready")
            return

        self.get_logger().warning(f"Started to make decision for Station {station_id}")

        mtrl_box_id = station.curr_mtrl_box
        order_id = self.get_order_id_by_mtrl_box(mtrl_box_id)
        order = self.proc_order[order_id]
        status = self.mtrl_box_status[order_id]
        curr_sliding_platform = station.curr_sliding_platform
        cmd_sliding_platform = station.cmd_sliding_platform

        try:
            # target_cell = station.is_completed.index(False)
            target_cell = 29
        except ValueError:
            target_cell = 29

        if target_cell != 29:
            cell_curr_mtrl_box = status.material_box.slots[target_cell]
            cell_order_mtrl_box = order.request.material_box.slots[target_cell]

        self.get_logger().warning(f"Debug: force to move out Station [{station_id}]")
            
        if target_cell == 29:
            self.get_logger().info(f"Station {station_id} has all cells completed")
            
            conveyor = self.conveyor.get_conveyor_by_station(station_id)
            if conveyor.is_occupied:
                self.get_logger().info("The conveyor is occupied. Try to move out in the next callback")
            else:
                address = 5000 + station_id
                values = [29]
                success = await self.write_registers(address, values)
                if success:
                    with self.mutex:
                        conveyor.is_occupied = True
                        station.is_occupied = False
                        station.curr_mtrl_box = 0
                        station.is_completed = [False] * 28
                    self.get_logger().info(f"The material box [{mtrl_box_id}] is moving out in station [{station_id}]")
                else:
                    self.get_logger().info(f"Failed to move out material box [{mtrl_box_id}] in station [{station_id}]")
        elif target_cell == curr_sliding_platform:
            # TODO: find requested
            req = DispenseDrug.Request()
            # req.content.append(???)

            while not self.dis_station_clis[station_id].wait_for_service(timeout_sec=1.0):
                if not rclpy.ok():
                    break
                self.get_logger().info("Dispense Service not available, waiting again...")

            try:
                future = self.dis_station_clis[station_id].call_async(req)
                await future
                res = future.result()
                if res.success:
                    with self.mutex:
                        station.is_completed[target_cell] = True
                    self.get_logger().info(f"Dispense request is done")
                else:
                    self.get_logger().error("Dispense request return false")
            except Exception as e:
                self.get_logger().error(f"Error reading registers: {e}")
        else:
            if cmd_sliding_platform != curr_sliding_platform:
                address = 5000 + station_id
                success = await self.write_registers(address, [target_cell])
                with self.mutex:
                    station.cmd_sliding_platform = target_cell

    # Service Server callback
    def new_order_cb(self, req, res):
        order_id = req.request.order_id
        priority = req.request.priority
        
        if not isinstance(order_id, int) or not isinstance(priority, int):
            msg = f"Invalid types: order_id={type(order_id).__name__}"
            self.get_logger().error()
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
        Asynchronously move the sliding platform to a specified cell for a dispense station.
        
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
        
        if not (0 < cell_no < 30):
            self.get_logger().error(f"Invalid cell_no {cell_no}: must be between 1 and 29")
            res.message = f"Cell number {cell_no} out of range (1-29)"
            return res

        if station_id < 0:
            self.get_logger().error(f"Invalid dispense_station_id {station_id}: must be non-negative")
            res.message = f"Station ID {station_id} must be non-negative"
            return res
        
        address = 5000 + station_id
        values = [cell_no]

        success = await self.write_registers(address, values)
        res.success = success

        if success:
            self.get_logger().info(f"Successfully moved sliding platform to cell {cell_no} at station {station_id}")
            res.message = "Platform movement successful"
        else:
            error_msg = f"Failed to write to register {address} with cell_no {cell_no}"
            self.get_logger().error(error_msg)
            res.message = error_msg

        return res
    
    def movement_decision_v1(self, order_id: int, dis_station_ids: List[int]) -> Optional[int]:
        """
        Determine the next station ID for an order based on requirements and availability.
        
        Args:
            order_id: The order to process
            dis_station_ids: List of available dispenser station IDs
            
        Returns:
            Optional[int]: Selected station ID if successful, None if no suitable station found
            
        Raises:
            TypeError: If inputs are of incorrect type
            ValueError: If order_id is negative or dis_station_ids is empty
        """
        if not isinstance(order_id, int):
            raise TypeError(f"Expected integer order_id, got {type(order_id).__name__}")
        if not isinstance(dis_station_ids, list) or not all(isinstance(x, int) for x in dis_station_ids):
            raise TypeError(f"Expected list of integers for dis_station_ids, got {type(dis_station_ids).__name__}")
        if order_id < 0:
            raise ValueError(f"Order ID must be non-negative, got {order_id}")
        if not dis_station_ids:
            raise ValueError("Dispenser station ID list cannot be empty")
        
        try:
            with self.mutex:
                status = self.mtrl_box_status.get(order_id)
                proc = self.proc_order.get(order_id)
                
                # if status is None or proc is None:
                #     raise KeyError(f"Order ID {order_id} not found")
                if status is None or proc is None:
                    self.get_logger().error(f"Order ID {order_id} not found in status or process data")
                    return 0
                
                # Track existing dispenser stations in current material box
                curr_mtrl_box = status.material_box
                curr_gone = self.get_curr_gone(curr_mtrl_box)
                # Required stations
                req_mtrl_box = proc.material_box
                req_to_go = self.get_req_to_go(req_mtrl_box)
            
                # Log current state
                self.get_logger().warning(
                    f"Order {order_id}: Current stations {sorted(curr_gone)}, "
                    f"Required stations {sorted(req_to_go)}, Valid the stations {dis_station_ids}"
                )

                remainder = req_to_go - curr_gone

                for station_id in dis_station_ids:
                    station = self.conveyor.get_station(station_id)
                    if station and not station.is_occupied and station_id in remainder:
                        self.get_logger().info(f"Selected station {station_id} for order {order_id}")
                        return station_id
                
            self.get_logger().info(f"No suitable station found for order {order_id} from {dis_station_ids}")
            return 0
        except AttributeError as e:
            self.get_logger().error(f"Invalid data structure for order {order_id}: {str(e)}")
            return 0
        except ValueError as e:
            self.get_logger().error(f"Set operation error for order {order_id}: {str(e)}")
            return 0
        except Exception as e:
            self.get_logger().error(f"Unexpected error for order {order_id}: {str(e)}")
            return 0

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
            acquired = self.mutex.acquire(timeout=1.0)  # 1 second timeout
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
    def bind_mtrl_box(self, material_box_id: int) -> bool:
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
            acquired = self.mutex.acquire(timeout=1.0)  # 1 second timeout
            if not acquired:
                self.get_logger().error("Failed to acquire lock for material box binding")
                return False
            for order_id, status in self.mtrl_box_status.items():
                if status.id == 0:
                    new_status = deepcopy(status)
                    new_status.id = material_box_id
                    new_status.status = MaterialBoxStatus.STATUS_CLEANING
                    self.mtrl_box_status[order_id] = new_status
                    self.get_logger().info(f"Material box [{material_box_id}] has been bound to order [{order_id}]")
                    return True

            self.get_logger().info("No available material box found for binding")
            return False
        except Exception as e:
            self.get_logger().error(f"Error in bind_mtrl_box: {str(e)}")
            return False
            
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

    def _initialize_dis_station_clis(self, cbgs: List[MutuallyExclusiveCallbackGroup]) -> None:
        """Initialize dispenser stations service clients."""
        for i in range(1, self.NUM_DISPENSER_STATIONS + 1):
            self.dis_station_clis[i] = self.create_client(
                DispenseDrug, 
                f"/dispenser_station_{i}/dispense_request",
                callback_group=cbgs[i - 1]
            )
            self.get_logger().info(f"Dispenser station [{i}] service client is created")

    def _initialize_dis_station_timers(self, period: float, cbgs: List[MutuallyExclusiveCallbackGroup]) -> None:
        """Initialize dispenser stations timers."""
        for i in range(1, self.NUM_DISPENSER_STATIONS + 1):
            cbg_index = int(i - 1)
            self.dis_station_timers[i] = self.create_timer(
                period,
                lambda station_id=i: asyncio.run_coroutine_threadsafe(
                    self.station_decision_cb(station_id),
                    self.loop
                ).result(),
                callback_group=cbgs[cbg_index]
            )
            self.get_logger().info(f"Dispenser station [{i}] execution timer is created")

    def _initialize_conveyor(self) -> None:
        """Initialize Conveyor Structure."""
        self.conveyor = Conveyor()
        self.conveyor.append(1)
        self.conveyor.attach_station(1, 'left', DispenserStation(1))
        self.conveyor.attach_station(1, 'right', DispenserStation(2))
        self.conveyor.append(2)
        self.conveyor.attach_station(2, 'left', DispenserStation(3))
        self.conveyor.attach_station(2, 'right', DispenserStation(4))
        self.conveyor.append(3)
        self.conveyor.attach_station(3, 'left', DispenserStation(5))
        self.conveyor.attach_station(3, 'right', DispenserStation(6))
        self.conveyor.append(4)
        self.conveyor.attach_station(4, 'left', DispenserStation(7))
        self.conveyor.append(5)
        self.conveyor.attach_station(5, 'left', DispenserStation(8))
        self.conveyor.append(6)
        self.conveyor.attach_station(6, 'left', DispenserStation(10))
        self.conveyor.attach_station(6, 'right', DispenserStation(9))
        self.conveyor.append(7)
        self.conveyor.attach_station(7, 'left', DispenserStation(12))
        self.conveyor.attach_station(7, 'right', DispenserStation(11))
        self.conveyor.append(8)
        self.conveyor.attach_station(8, 'left', DispenserStation(14))
        self.conveyor.attach_station(8, 'right', DispenserStation(13))
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
            acquired = self.mutex.acquire(timeout=1.0)  # 5 second timeout
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

    def get_dis_station_cli(self, station_id: int) -> Optional[rclpy.client.Client]:
        """Safely get a dispenser station client by ID."""
        return self.dis_station_clis.get(station_id)

    async def send_income_mtrl_box(self, mtrl_box_id: int) -> None:
        """
        Asynchronously send a material box ID to the income service.
        
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
            future = self.income_mtrl_box_cli.call_async(req)
            # await future
            # res = future.result()
            res = await asyncio.wait_for(future, timeout=1.0)

            if res.success:
                self.get_logger().info(f"Successfully sent material box ID: {mtrl_box_id}")
                return True
        
            self.get_logger().error(f"Service call succeeded but reported failure for ID: {mtrl_box_id}")
            return False
        except asyncio.TimeoutError:
            self.get_logger().error(f"Service call timeout for material box ID: {mtrl_box_id}")
            return False
        except AttributeError as e:
            self.get_logger().error(f"Invalid service response for ID {mtrl_box_id}: {str(e)}")
            return False
        except Exception as e:
            self.get_logger().error(f"Service call failed for ID {mtrl_box_id}: {str(e)}")
            return False

    async def read_registers(self, address: int, count: int) -> Optional[List[int]]:
        """
        Asynchronously read a specified number of registers from a given address.
        
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
            future = self.read_cli.call_async(req)
            # await future
            # res = future.result()
            res = await asyncio.wait_for(future, timeout=1.0)
            if res.success:
                if not isinstance(res.values, list):
                    self.get_logger().error("Invalid response format: values not a list")
                    return None
                self.get_logger().info(f"Successfully read {len(res.values)} registers from address {address}")
                return res.values
            
            self.get_logger().error(f"Service reported failure reading registers at {address}")
            return None

        except asyncio.TimeoutError:
            self.get_logger().error(f"Timeout reading registers at address {address}")
            return None
        except AttributeError as e:
            self.get_logger().error(f"Invalid response format for address {address}: {str(e)}")
            return None
        except Exception as e:
            self.get_logger().error(f"Failed to read registers at {address}: {str(e)}")
            return None
  
    async def write_registers(self, address: int, values: List[int]) -> bool:
        """
        Asynchronously write values to registers starting at the specified address.
        
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
            future = self.write_cli.call_async(req)
            # await future
            # res = future.result()
            res = await asyncio.wait_for(future, timeout=1.0) 
            if res.success:
                self.get_logger().info(f"Successfully wrote {len(values)} registers at address {address}: {values}")
                return True
                
            self.get_logger().error(f"Service reported failure writing {len(values)} registers at {address}")
            return False
        except asyncio.TimeoutError:
            self.get_logger().error(f"Timeout writing registers at address {address}")
            return False
        except AttributeError as e:
            self.get_logger().error(f"Invalid response format for address {address}: {str(e)}")
            return False
        except Exception as e:
            self.get_logger().error(f"Failed to write registers at {address}: {str(e)}")
            return False
    
    def run_loop(self):
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


def main(args=None):
    rclpy.init(args=args)
    try:
        executor = MultiThreadedExecutor()
        manager = Manager(executor)
        executor.add_node(manager)
        try:
            executor.spin()
        finally:
            executor.shutdown()
            manager.destroy_node()
    except KeyboardInterrupt:
        pass
    except ExternalShutdownException:
        sys.exit(1)
    finally:
        rclpy.try_shutdown()

if __name__ == "__main__":
    main()

