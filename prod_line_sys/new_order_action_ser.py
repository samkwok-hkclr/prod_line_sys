import sys
from typing import Dict, Optional
from collections import deque
from threading import Lock
from functools import partial
from copy import deepcopy
from datetime import datetime, timedelta

import rclpy

from rclpy.node import Node
from rclpy.time import Time
from rclpy.action import ActionServer, GoalResponse, CancelResponse
from rclpy.callback_groups import MutuallyExclusiveCallbackGroup, ReentrantCallbackGroup

from rclpy.executors import ExternalShutdownException, MultiThreadedExecutor

from smdps_msgs.msg import OrderRequest, OrderResponse, MaterialBoxStatus
from smdps_msgs.srv import NewOrder, PackagingOrder, SimplifiedPackagingOrder
from smdps_msgs.action import NewOrder as NewOrderAction

from .pkg_info import PkgInfo

class NewOrderActionServer(Node):
    def __init__(self):
        super().__init__("new_order_ser")

        self._status_lock = Lock()
        self.mtrl_box_status: Dict[int, (MaterialBoxStatus, rclpy.time.Time)] = {} # order_id, MaterialBoxStatus
        self.order_sent: Dict[int, (bool, rclpy.time.Time)] = {}
        self.pkg_order: Dict[int, OrderRequest] = {} # order_id, OrderRequest
        self.waiting_result = self.create_rate(1.0, self.get_clock())

         # Action server goal
        self._goal_queue = deque()
        self._goal_queue_lock = Lock()
        self._curr_goal = None

        # Callback groups
        sub_cbg = MutuallyExclusiveCallbackGroup()
        srv_cli_cbg = MutuallyExclusiveCallbackGroup()
        srv_ser_cbg = MutuallyExclusiveCallbackGroup()
        action_ser_cbg = ReentrantCallbackGroup()

        normal_timer_cbg = MutuallyExclusiveCallbackGroup()

        # Subscriptions
        self.mtrl_box_status_sub = self.create_subscription(
            MaterialBoxStatus, 
            "material_box_status", 
            self.mtrl_box_status_cb, 
            10, 
            callback_group=sub_cbg
        )

        # Service clients
        self.new_order_cli = self.create_client(NewOrder, "new_order", callback_group=srv_cli_cbg)
        self.pkg_order_cli = self.create_client(PackagingOrder, "packaging_order", callback_group=srv_cli_cbg)

        self.sim_pkg_order_srv_ser = self.create_service(SimplifiedPackagingOrder, "simplified_packaging_order", self.pkg_pkg_cb, callback_group=srv_ser_cbg)

        # Action server
        self.new_order_action_ser = ActionServer(
            self,
            NewOrderAction,
            "new_order",
            handle_accepted_callback=self.handle_accepted_cb,
            goal_callback=self.goal_cb,
            cancel_callback=self.cancel_cb,
            execute_callback=self.execute_cb,
            callback_group=action_ser_cbg,
            result_timeout=60
        )

        # Timers
        self.cleaner_timer = self.create_timer(10.0, self.cleaner_cb, callback_group=normal_timer_cbg)

        self.get_logger().info("New Order Action Server Node is started")

    # Subscription callbacks
    def mtrl_box_status_cb(self, msg: MaterialBoxStatus) -> None:
        if not isinstance(msg, MaterialBoxStatus):
            self.get_logger().error(f"Invalid status type: {type(msg).__name__}")
            return
        
        with self._status_lock:
            self.mtrl_box_status[msg.order_id] = (msg, self.get_clock().now())

    def new_order_done_cb(self, order_id: int, future) -> None:
        res = future.result()

        if res.response.success:
            with self._status_lock:
                self.order_sent[order_id] = (True, self.get_clock().now())
            self.get_logger().info(f"Sent the new order successfully")
        else:
            self.get_logger().error(f"Service call succeeded but reported failure for order: {order_id}")
            
    def send_new_order(self, request: OrderRequest) -> Optional[bool]:
        if not isinstance(request, OrderRequest):
            raise TypeError(f"Expected OrderRequest, got: {type(request).__name__}")

        req = NewOrder.Request()
        req.request = request

        while not self.new_order_cli.wait_for_service(timeout_sec=1.0):
            if not rclpy.ok():
                return None
            self.get_logger().error("new_order Service not available, waiting again...")

        try:
            future = self.new_order_cli.call_async(req)
            future.add_done_callback(partial(self.new_order_done_cb, request.order_id))
            
            return True
        except AttributeError as e:
            self.get_logger().error(f"Invalid response format: {str(e)}")
            return False
        except Exception as e:
            self.get_logger().error(f"Failed to send order request: {str(e)}")
            return False
        
    def cleaner_cb(self) -> None:
        THRESHOLD_SECONDS = 900
        current_time = self.get_clock().now()
        status_to_remove = []
        sent_to_remove = []

        with self._status_lock:
            for order_id, status_tuple in self.mtrl_box_status.items():
                last_time = status_tuple[1]
                if last_time is not None:
                    time_gap = current_time - last_time
                    time_gap_seconds = time_gap.nanoseconds * 1e-9
                    if time_gap_seconds >= THRESHOLD_SECONDS:
                        self.get_logger().info(f"Order ID {order_id} expired, removing from mtrl_box_status")
                        status_to_remove.append(order_id)
        
            for order_id in status_to_remove:
                self.mtrl_box_status.pop(order_id)

            for order_id, sent_tuple in self.order_sent.items():
                last_time = sent_tuple[1]
                if last_time is not None:
                    time_gap = current_time - last_time
                    time_gap_seconds = time_gap.nanoseconds * 1e-9
                    if time_gap_seconds >= THRESHOLD_SECONDS:
                        self.get_logger().info(f"Order ID {order_id} expired, removing from order_sent")
                        sent_to_remove.append(order_id)
            
            for order_id in sent_to_remove:
                self.order_sent.pop(order_id)

    def pkg_pkg_cb(self, req, res):
        try:
            self.get_logger().info(f"{req}")
            if self.send_pkg_req(req):
                self.get_logger().info(f"Sent packaging order to manager")
                res.success = True
            else:
                res.message = "Sent packaging order failed"
        except Exception as e:
            self.get_logger().error(f"Unexpected error in packaging order: {str(e)}")
        finally:
            return res

    def send_pkg_req(self, req) -> Optional[bool]:
        if not isinstance(req, SimplifiedPackagingOrder.Request):
            raise TypeError(f"Expected Type SimplifiedPackagingOrder, got {type(order_id).__name__}")

        order_id = req.order_id
        
        proc_order = self.pkg_order.get(order_id)
        if proc_order is None:
            raise ValueError(f"Order {order_id} not found")
        
        pkg_req = PackagingOrder.Request()
        pkg_req.order_id = req.order_id
        pkg_req.material_box_id = req.material_box_id
        pkg_req.requester_id = 1234

        for i, info in enumerate(pkg_req.print_info):
            if not proc_order.material_box.slots or i >= len(proc_order.material_box.slots):
                continue
            if not proc_order.material_box.slots[i].drugs:
                continue

            info.cn_name = proc_order.patient.institute_name
            info.en_name  = proc_order.patient.name

            curr_meal = (proc_order.start_meal + i) % 4
            info.time = PkgInfo.MEAL_TIME.get(curr_meal, "Unknown")

            _date = proc_order.start_date
            try:
                dt = datetime.strptime(_date, PkgInfo.DATE_FORMAT)
                self.get_logger().warning(f"dt: {dt}")
                days_to_add = (proc_order.start_meal + i) // 4  # integer division
                new_date = dt + timedelta(days=days_to_add)     # Add the days to the original datetime
                self.get_logger().warning(f"new_date: {new_date}")
                self.get_logger().warning(f"new_date.strftime({PkgInfo.DATE_FORMAT}): {new_date.strftime(PkgInfo.DATE_FORMAT)}")
                info.date = f"Date: {new_date.strftime(PkgInfo.DATE_FORMAT)}"
            except ValueError as e:
                info.date = "ERROR"
                self.get_logger().error(f"Invalid date format for order {order_id}: {str(e)}")

            info.qr_code = PkgInfo.QR_CODE

            for drug in proc_order.material_box.slots[i].drugs:
                drug_str = f"{drug.name}   {drug.amount}"
                self.get_logger().info(f"Added to order {order_id}: {drug_str}")
                info.drugs.append(drug_str)

        while not self.pkg_order_cli.wait_for_service(timeout_sec=1.0):
            if not rclpy.ok():
                return None
            self.get_logger().info("packaging_order Service not available, waiting again...")

        try:
            future = self.pkg_order_cli.call_async(pkg_req)
            future.add_done_callback(partial(self.pkg_req_done_cb, order_id))
            
            return True
        except AttributeError as e:
            self.get_logger().error(f"Invalid service response for order id {order_id}: {str(e)}")
            return False
        except ValueError as e:
            return False  
        except Exception as e:
            self.get_logger().error(f"Service call failed for order id {order_id}: {str(e)}")
            return False

    def pkg_req_done_cb(self, order_id: int, future) -> None:
        res = future.result()

        if res and res.success:
            self.get_logger().info(f"Packaging request successful for order {order_id}")

            order = self.pkg_order.pop(order_id)
            if order:
                self.get_logger().info(f"Removed the order in pkg_order: {order_id}")
            else:
                self.get_logger().info(f"Order not found in pkg_order: {order_id}")
        else:         
            self.get_logger().error(f"Packaging service failed for order {order_id}")


    # Action Server Callbacks
    def handle_accepted_cb(self, goal_handle):
        """Start or defer execution of an already accepted goal."""
        with self._goal_queue_lock:
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
            goal = goal_handle.request
            req = goal.request
            result = NewOrderAction.Result()
            feedback_msg = NewOrderAction.Feedback()

            self.pkg_order[req.order_id] = deepcopy(req)
            
            order_id = req.order_id
            priority = req.priority

            self.get_logger().info(f"Received NewOrder by action: order_id={order_id}, priority={priority}")

            success = self.send_new_order(req)
            if success:
                self.get_logger().info(f"Sent NewOrder by Service: order_id={order_id}, priority={priority}")
                self.get_logger().info(f"Waiting the Service: order_id={order_id}, priority={priority}")

            feedback_msg.running = True
            retries = 0
            MAX_RETRY = 600 # 3

            for _ in range(1, MAX_RETRY):
                if not rclpy.ok():
                    self.get_logger().warn("ROS2 shutdown detected, aborting goal")
                    break

                if goal_handle.is_cancel_requested:
                    goal_handle.canceled()
                    self.get_logger().info(f"Goal canceled for order_id={order_id}")
                    return result

                with self._status_lock:
                    sent_tuple = self.order_sent.get(order_id)
                    if sent_tuple and sent_tuple[0]:
                        status_tuple = self.mtrl_box_status.get(order_id)
                        if status_tuple is None:
                            self.get_logger().debug(f"Status for order_id {order_id} does not publish yet")
                        else:
                            if status_tuple[0].id != 0:
                                result.response.material_box_id = status_tuple[0].id
                                result.response.success = True
                                break

                self.get_logger().warning(f"Waiting for material box ID for order_id={order_id} ({retries} retries)...")
                goal_handle.publish_feedback(feedback_msg)
                retries += 1
                self.waiting_result.sleep()

            if retries >= MAX_RETRY:
                result.response.success = False
                result.response.message = "Material box assignment timed out"
                self.get_logger().error(f"Timeout after {retries} retries for order_id={order_id}")

            self.get_logger().warning(f"Oh yeah! The action are handled order_id={order_id}")
            goal_handle.succeed()
            return result
        
        except Exception as e:
            self.get_logger().error(f"Error processing message: {e}")

        finally:
            with self._status_lock:
                if self.mtrl_box_status.get(order_id):
                    self.mtrl_box_status.pop(order_id)

                if self.order_sent.get(order_id):
                    self.order_sent.pop(order_id)

            with self._goal_queue_lock:
                try:
                    # Start execution of the next goal in the queue.
                    self._curr_goal = self._goal_queue.popleft()
                    self.get_logger().info("Next goal pulled from the queue")
                    self._curr_goal.execute()
                except IndexError:
                    # No goal in the queue.
                    self._curr_goal = None
    
    def destroy_node(self):
        self.new_order_action_ser.destroy()

        super().destroy_node()

def main(args=None):
    rclpy.init(args=args)
    try:
        new_order_action_server = NewOrderActionServer()
        executor = MultiThreadedExecutor()
        executor.add_node(new_order_action_server)
        try:
            executor.spin()
        finally:
            executor.shutdown()
            new_order_action_server.destroy_node()
    except KeyboardInterrupt:
        pass
    except ExternalShutdownException:
        sys.exit(1)
    finally:
        rclpy.try_shutdown()


if __name__ == '__main__':
    main()