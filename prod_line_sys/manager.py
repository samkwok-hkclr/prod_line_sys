import sys
import asyncio
from collections import deque
from threading import Thread, Lock
from typing import Dict, List, Optional

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
from smdps_msgs.srv import NewOrder, ReadRegister, WriteRegister, MoveSlidingPlatform, DispenseDrug

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

    def __init__(self, executor):
        super().__init__("prod_line_manage")

        self.recv_order = deque()
        self.proc_order: Dict[int, OrderRequest] = {} # order_id, OrderRequest
        self.mtrl_box_status: Dict[int, MaterialBoxStatus] = {} # order_id, MaterialBoxStatus
        self.qr_scan: List[CameraTrigger] = [] 

        self.mutex = Lock()

        # Graph of conveyor structure
        self.conveyor = None
        self._initialize_conveyor()

        self.loop = asyncio.new_event_loop()
        self.loop_thread = Thread(target=self.run_loop, daemon=True)
        self.loop_thread.start()
        self.mutex = Lock()

        self.is_plc_connected = False
        self.is_releasing_mtrl_box = True

        # maybe unused
        self.executor = executor

        # Callback groups
        sub_cbg = MutuallyExclusiveCallbackGroup()
        srv_ser_cbg = MutuallyExclusiveCallbackGroup()
        srv_cli_cbg = MutuallyExclusiveCallbackGroup()
        station_srv_cli_cbgs = [MutuallyExclusiveCallbackGroup()] * self.NUM_DISPENSER_STATIONS

        normal_timer_cbg = MutuallyExclusiveCallbackGroup()
        qr_handle_timer_cbg = MutuallyExclusiveCallbackGroup()
        station_timer_cbgs = [MutuallyExclusiveCallbackGroup()] * self.NUM_DISPENSER_STATIONS
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
        self.qr_scan_sub = self.create_subscription(CameraTrigger, "qr_camera_scan", self.qr_scan_cb, 10, callback_group=sub_cbg)
        self.get_logger().info("Subscriptions are created")
        
        # Service Servers
        self.new_order_srv = self.create_service(NewOrder, "new_order", self.new_order_cb, callback_group=srv_ser_cbg)
        self.move_sliding_platform = self.create_service(MoveSlidingPlatform, "move_sliding_platform", self.move_sliding_platform_cb, callback_group=srv_ser_cbg)
        self.get_logger().info("Service Servers are created")
        
        # Service Clients
        self.read_cli = self.create_client(ReadRegister, "read_register", callback_group=srv_cli_cbg)
        self.write_cli = self.create_client(WriteRegister, "write_register", callback_group=srv_cli_cbg)
        self.dis_station_clis: Dict[int, rclpy.client.Client] = {} # station_id, Client
        self._initialize_dis_station_clis(station_srv_cli_cbgs)
        self.get_logger().info("Service Clients are created")

        # Timers
        self.qr_handle_timer = self.create_timer(0.5, self.qr_handle_cb, callback_group=qr_handle_timer_cbg)
        self.start_order_timer = self.create_timer(1.0, self.start_order_cb, callback_group=normal_timer_cbg)
        self.order_status_timer = self.create_timer(1.0, self.order_status_cb, callback_group=normal_timer_cbg)
        self.dis_station_timers: Dict[int, rclpy.Timer.Timer] = {}
        self._initialize_dis_station_timers(station_timer_cbgs)
        self.get_logger().info("Timers are created")
        
        self.get_logger().info("Produation Line Manager Node started")

    # Subscription callbacks
    def plc_conn_cb(self, msg: Bool) -> None:
        with self.mutex:
            self.is_plc_connected = msg.data
        if not msg.data:
            self.get_logger().error(f"Received: PLC is disconnected")

    def releasing_mtrl_box_cb(self, msg: Bool) -> None:
        with self.mutex:
            self.is_releasing_mtrl_box = msg.data
            if self.is_releasing_mtrl_box:
                conveyor = self.conveyor.get_conveyor(1)
                conveyor.is_occupied = True
        self.get_logger().debug(f"Received releasing material box message: {msg}")

    def sliding_platform_curr_cb(self, msg: UInt8MultiArray) -> None:
        if len(msg.data) != self.NUM_DISPENSER_STATIONS:
            self.get_logger().warning(f"Message length {len(msg.data)} doesn't match stations {len(self.dis_station)}")
            return
        try:
            with self.mutex:
                for i, platform_location in enumerate(msg.data, start=1):
                    station = self.conveyor.get_station(i)
                    if station is None:
                        self.get_logger().warning(f"No station found for ID {i} <<<< 1")
                        continue
                    station.curr_sliding_platform = platform_location
            
            self.get_logger().debug(f"Received sliding platform message: {msg}")
        except AttributeError as e:
            self.get_logger().error(f"Invalid message format: {e}")
        except Exception as e:
            self.get_logger().error(f"Error processing sliding platform message: {e}")
    
    def sliding_platform_cmd_cb(self, msg: UInt8MultiArray) -> None:
        if len(msg.data) != self.NUM_DISPENSER_STATIONS:
            self.get_logger().warning(f"Message length {len(msg.data)} doesn't match stations {len(self.dis_station)}")
            return
        try:
            with self.mutex:
                for i, state in enumerate(msg.data, start=1):
                    station = self.conveyor.get_station(i)
                    if station is None:
                        self.get_logger().warning(f"No station found for ID {i} <<<< 2")
                        continue
                    station.cmd_sliding_platform = state
            
            self.get_logger().debug(f"Received sliding platform ready message: {msg}")
        except AttributeError as e:
            self.get_logger().error(f"Invalid message format: {e}")
        except Exception as e:
            self.get_logger().error(f"Error processing sliding platform ready message: {e}")
        return

    def sliding_platform_ready_cb(self, msg: UInt8MultiArray) -> None:
        if len(msg.data) != self.NUM_DISPENSER_STATIONS:
            self.get_logger().warning(f"Message length {len(msg.data)} doesn't match stations {len(self.dis_station)}")
            return
        try:
            with self.mutex:
                for i, platform_ready_state in enumerate(msg.data, start=1):
                    station = self.conveyor.get_station(i)
                    if station is None:
                        self.get_logger().warning(f"No station found for ID {i} <<<< 2")
                        continue
                    station.is_platform_ready = platform_ready_state
            
            self.get_logger().debug(f"Received sliding platform ready message: {msg}")
        except AttributeError as e:
            self.get_logger().error(f"Invalid message format: {e}")
        except Exception as e:
            self.get_logger().error(f"Error processing sliding platform ready message: {e}")
        return
    
    def qr_scan_cb(self, msg: CameraTrigger) -> None:
        if not 1 <= msg.camera_id <= 11:
            self.get_logger().error(f"Received undefined camera_id: {msg.camera_id}")
            return
        if not 1 <= msg.material_box_id <= 20:
            self.get_logger().error(f"Received undefined material_box_id: {msg.material_box_id}")
            return
        
        with self.mutex:
            if self.qr_scan.empty():
                self.qr_scan.append(msg)
            else:
                last_scan = self.qr_scan[-1]
                if last_scan.camera_id == msg.camera_id and last_scan.material_box_id == msg.material_box_id:
                    self.get_logger().info(f"Camera {msg.camera_id} repeated the scan")
                else:
                    self.qr_scan.append(msg)
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
            self.get_logger().info("Added a order to processing queue")
            self.get_logger().info("Removed a order from received queue")
        else:
            self.get_logger().error("Failed to call the PLC to release a material box")
    
    async def order_status_cb(self) -> None:
        curr_time = self.get_clock().now().to_msg()
        with self.mutex:
            for status in self.mtrl_box_status.values():
                if isinstance(status, MaterialBoxStatus):
                    status.header.stamp = curr_time
                    self.mtrl_box_status_pub.publish(status)
                else:
                    self.get_logger().warning(f"Invalid status type found: {type(status).__name__}")
        # address = 5030
        # count = 1
        # success = await self.read_registers(address, count)
        # if success:
        #     self.get_logger().info(f"OK to read {address}")
        # else:
        #     self.get_logger().warning(f"Failed to read to register {address}")

    async def qr_handle_cb(self) -> None:
        to_remove = []
        with self.mutex:
            self.get_logger().debug(f"Started to handle the QR scan, Total: {len(self.qr_scan)}")
            for msg in self.qr_scan[:]:
                is_completed = False
                camera_id = msg.camera_id
                material_box_id = msg.material_box_id
                
                order_id = self.get_order_id_by_mtrl_box(material_box_id)
                if order_id == 0:
                    self.get_logger().info(f"The material box [{material_box_id}] does not bound to order")
                
                match camera_id:
                    case 1 | 2 | 3 | 4 | 5 | 6 | 7 | 8:
                        decide_station_ids, register_addr = self.CANMERA_STATION_PLC_MAP[camera_id]

                        if camera_id == 1 and not self.is_bound(material_box_id):
                            success = self.bind_mtrl_box(material_box_id)
                            if not success:
                                self.get_logger().warning(f"Failed to bind material box {material_box_id}")
                                continue

                        decide_station_ids = self.remove_occupied_station(decide_station_ids)
                        decision = self.make_decision_v1(order_id, decide_station_ids)
                        
                        if decision in decide_station_ids:
                            values = [2] if decision == decide_station_ids[0] else [3]
                            success = await self.write_registers(register_addr, values)
                            if success:
                                if station := self.conveyor.get_station(decision):
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
                                        conveyor.is_occupied = True
                                        conveyor.curr_mtrl_box = material_box_id
                                        if camera_id != 1:
                                            conveyor.is_occupied = False
                                            conveyor.curr_mtrl_box = 0
                                        is_completed = True
                                else:
                                    self.get_logger().error(f"Failed to write to register {register_addr}")
                            else:
                                self.get_logger().warning(f"No available next conveyor for camera_id {camera_id}")
                        self.get_logger().info(f"decision [{decision}] is made for material box [{material_box_id}] in camera [{camera_id}]")
                    case 9:
                        # vision inspection
                        is_completed = True
                        self.get_logger().info(f"Vision inspection triggered for material box [{material_box_id}]")
                        pass
                    case 10:
                        # packaging machine 1
                        is_completed = True
                        self.get_logger().info(f"Packaging machine 1 triggered for material box [{material_box_id}]")
                        pass            
                    case 11:
                        # packaging machine 2
                        is_completed = True
                        self.get_logger().info(f"Packaging machine 2 triggered for material box [{material_box_id}]")
                        pass
                    case _:
                        self.get_logger().info(f"Received unknown camera ID: {camera_id}")
                if is_completed:
                    to_remove.append(msg)

            for msg in to_remove:
                self.qr_scan.remove(msg)

        self.get_logger().debug(f"Completed to handle the QR scan")

    async def station_decision_cb(self, station_id: int) -> None:
        self.get_logger().debug(f"Dispenser Station [{station_id}] callback is called")
        with self.mutex:
            station = self.conveyor.get_station(id)
            if station is None:
                self.get_logger().debug(f"No station found with ID: {id}")
                return
            if not station.is_occupied:
                self.get_logger().debug(f"Station {id} is not occupied")
                return
            if station.curr_mtrl_box == 0:
                self.get_logger().debug(f"Station {id} has no material box")
                return
            if not station.is_platform_ready:
                self.get_logger().debug(f"Station {id} sliding platform is not ready")
                return
        
            mtrl_box_id = station.curr_mtrl_box
            order_id = self.get_order_id_by_mtrl_box(mtrl_box_id)
            order = self.proc_order[order_id]
            status = self.mtrl_box_status[order_id]
            curr_sliding_platform = station.curr_sliding_platform
            cmd_sliding_platform = station.cmd_sliding_platform

            try:
                target_cell = station.is_completed.index(False)
            except ValueError:
                target_cell = 29

            if target_cell != 29:
                cell_curr_mtrl_box = status.material_box.slots[target_cell]
                cell_order_mtrl_box = order.request.material_box.slots[target_cell]
            
        if target_cell == 29:
            self.get_logger().info(f"Station {station_id} has all cells completed")
            with self.mutex:
                conveyor = self.conveyor.get_conveyor_by_station(station_id)
                if conveyor.is_occupied:
                    self.get_logger().info("The conveyor is occupied. Try to move out in the next callback")
                else:
                    address = 5000 + station_id
                    values = [29]
                    success = await self.write_registers(address, values)
                    if success:
                        conveyor.is_occupied = True
                        self.get_logger().info(f"The material box [{mtrl_box_id}] is moving out in station [{station_id}]")
                    else:
                        conveyor.is_occupied = False
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
                address = 5000 + id
                success = await self.write_registers(address, [target_cell])
                with self.mutex:
                    station.cmd_sliding_platform = target_cell

    # Service Server callback
    def new_order_cb(self, req, res):
        self.get_logger().info(f"Received NewOrder request: order_id={req.order_id}")
        self.recv_order.append(req.request)
        msg = MaterialBoxStatus()
        msg.creation_time = self.get_clock().now().to_msg()
        msg.id = 0
        msg.status = MaterialBoxStatus.STATUS_INITIALIZING
        msg.priority = req.requset.priority
        msg.order_id = req.requset.order_id
        with self.mutex:
            self.mtrl_box_status[req.requset.order_id] = msg
        res.success = True
        return res
    
    async def move_sliding_platform_cb(self, req, res):
        if 0 <= req.cell_no or req.cell_no >= 30:
            self.get_logger().error("Invalid cell_no!")
            return res
        address = 5000 + req.dispense_station_id
        values = [req.cell_no]
        success = await self.write_registers(address, values)
        if success:
            res.success = True
        else:
            res.message = f"Failed to write to register {address} with cell_no {req.cell_no}"
        return res
    
    def make_decision_v1(self, order_id: int, dis_station_id: List[int]) -> int:
        try:
            with self.mutex:
                status = self.mtrl_box_status.get(order_id)
                proc = self.proc_order.get(order_id)
                
                if status is None or proc is None:
                    raise KeyError(f"Order ID {order_id} not found")
                    
                # Track existing dispenser stations in current material box
                curr_mtrl_box = status.material_box
                curr_gone = self.get_curr_gone(curr_mtrl_box)
                # Required stations
                req_mtrl_box = proc.material_box
                req_to_go = self.get_req_to_go(req_mtrl_box)
            
            # Log current state
            self.get_logger().info(
                f"Order {order_id}: Current stations [{sorted(curr_gone)}], "
                f"Required stations [{sorted(req_to_go)}], Valid the stations {dis_station_id}"
            )

            remainder = req_to_go - curr_gone
            with self.mutex:
                for id in dis_station_id:
                    station = self.conveyor.get_station(id)
                    if station and not station.is_occupied and id in remainder:
                        self.get_logger().info(f"Selected station {id} for order {order_id}")
                        return id
                
            self.get_logger().info(f"The stations [{dis_station_id}] are not requested for order {order_id}")
            return 0
        except AttributeError as e:
            self.get_logger().error(f"Invalid material box structure: {e}")
            return 0
        except KeyError as e:
            self.get_logger().error(f"Data access error: {e}")
            return 0
        except Exception as e:
            self.get_logger().error(f"Unexpected error in station decision: {e}")
            return 0

    def remove_occupied_station(self, station_ids: List[int]) -> List[int]:
        to_remove = []
        for id in station_ids[:]:
            station = self.conveyor.get_station(id)
            if station and station.is_occupied:
                to_remove.append(id)
        for id in station_ids:
            station_ids.remove(id)
        return station_ids

    def is_bound(self, material_box_id: int) -> bool:
        if not isinstance(material_box_id, int):
            raise TypeError(f"Unexpected id >>> {type(material_box_id).__name__}")
        
        with self.mutex:
            for status in self.mtrl_box_status.values():
                if not isinstance(status, MaterialBoxStatus):
                    self.get_logger().warning(f"Invalid status type: {type(status).__name__}")
                    continue
                if status.id == material_box_id and status.id != 0:
                    self.get_logger().debug(
                        f"Found bound material box {material_box_id} "
                        f"with order {status.order_id}"
                    )
                    return True
        return False

    def bind_mtrl_box(self, material_box_id: int) -> bool:
        if not isinstance(material_box_id, int):
            raise TypeError(f"Expected integer id, got {type(material_box_id).__name__}")
        
        with self.mutex:
            for status in self.mtrl_box_status.values():
                if status.id == 0:
                    status.id = material_box_id
                    status.status = MaterialBoxStatus.STATUS_CLEANING
                    order_id = status.order_id if status.order_id is not None else 0
                    self.get_logger().info(f"Material box [{material_box_id}] has binded to order [{order_id}]")
                    return True

        self.get_logger().info("No available material box found for binding")
        return False

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
    
    def _initialize_dis_station_timers(self, cbgs: List[MutuallyExclusiveCallbackGroup]) -> None:
        """Initialize dispenser stations timers."""
        for i in range(1, self.NUM_DISPENSER_STATIONS + 1):
            self.dis_station_timers[i] = self.create_timer(
                0.5,
                lambda station_id=i: asyncio.run_coroutine_threadsafe(
                    self.station_decision_cb(station_id),
                    self.loop
                ).result(),
                callback_group=cbgs[i - 1]
            )
            self.get_logger().debug(f"Dispenser station [{i}] timer is created")

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
        self.conveyor.attach_station(5, 'right', DispenserStation(8))
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

    def get_order_id_by_mtrl_box(self, mtrl_box_id: int) -> int:
        with self.mutex:
            for status in self.mtrl_box_status.values:
                if mtrl_box_id == status.id:
                    return status.order_id
        return 0

    def get_dis_station_cli(self, id: int) -> Optional[rclpy.client.Client]:
        """Safely get a dispenser station client by ID."""
        return self.dis_station_clis.get(id)

    async def read_registers(self, address: int, count: int) -> List[int]:
        req = ReadRegister.Request()
        req.address = address
        req.count = count

        while not self.read_cli.wait_for_service(timeout_sec=1.0):
            if not rclpy.ok():
                return None
            self.get_logger().info("read_registers Service not available, waiting again...")

        try:
            future = self.read_cli.call_async(req)
            await future
            res = future.result()
            if res.success:
                self.get_logger().info(f"Read registers: {address}, count: {count}")
                return res.values
            else:
                self.get_logger().error("Failed to read registers")
                return None
        except Exception as e:
            self.get_logger().error(f"Error reading registers: {e}")
            return None
        
    async def write_registers(self, address: int, values: List[int]) -> None:
        req = WriteRegister.Request()
        req.address = address
        req.values = values

        while not self.write_cli.wait_for_service(timeout_sec=1.0):
            if not rclpy.ok():
                return None
            self.get_logger().info("write_registers Service not available, waiting again...")

        try:
            future = self.write_cli.call_async(req)
            await future
            res = future.result()
            if res.success:
                self.get_logger().info(f"write registers: {address}, values: {values}")
                return True
            else:
                self.get_logger().error("Failed to write registers")
                return False
        except Exception as e:
            self.get_logger().error(f"Error writing registers: {e}")
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

