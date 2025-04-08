
from typing import Dict, Optional
from functools import partial

import rclpy

from rclpy.node import Node
from rclpy.executors import MultiThreadedExecutor
from rclpy.callback_groups import MutuallyExclusiveCallbackGroup

from smdps_msgs.msg import DispenseContent
from smdps_msgs.srv import DispenseDrug


class FakeDispenseClient(Node):
    def __init__(self):
        super().__init__(f"fake_dis_cli")

        station_srv_cli_cbgs = [MutuallyExclusiveCallbackGroup() for _ in range(1, 15)]

        self.dis_station_clis: Dict[int, rclpy.client.Client] = dict() # station_id, Client

        for i in range(1, 15):
            cbg_index = int(i - 1)
            self.dis_station_clis[i] = self.create_client(
                DispenseDrug, 
                f"/dispenser_station_{i}/dispense_request",
                callback_group=station_srv_cli_cbgs[cbg_index]
            )
            self.get_logger().info(f"Station [{i}] service client is created")

        self.DISPENSE = {
            1: [2, 3],
            2: [7, 1],
            3: [6, 1],
            4: [3, 1],
            5: [1, 1],
            6: [7, 1],
            7: [12, 1],
            8: [10, 1],
            9: [2, 1],
            10: [3, 1],
            11: [1, 1],
            12: [7, 1],
            13: [2, 1],
            14: [12, 1],
        }

        self.test_timer = self.create_timer(1.0, self.timer_cb, callback_group=MutuallyExclusiveCallbackGroup())

    def timer_cb(self) -> None:
        self.test_timer.cancel()
        for i in range(1, 15):
            success = self.start_action(i)
            if success:
                self.get_logger().info(f"Station [{i}] service client is sent")
            else:
                self.get_logger().error(f"Station [{i}] service client is error")

    def start_action(self, station_id: int) -> None:
        try:
            success = self.send_dispense_req(self.DISPENSE.get(station_id), station_id)

            if success:
                return True
            
            self.get_logger().error(f"Dispense failed for station {station_id}, cell {target_cell}")
            return False
        except KeyError:
            self.get_logger().error(f"No service client for station {station_id}")
            return False
        except Exception as e:
            self.get_logger().error(f"Dispense error for station {station_id}: {str(e)}")
            return False
        return res

    def send_dispense_req(self, filtered_missing, station_id: int) -> Optional[bool]:
        req = DispenseDrug.Request()
        req.content.append(DispenseContent(unit_id=filtered_missing[0], amount=filtered_missing[1]))

        while not self._get_dis_station_cli(station_id).wait_for_service(timeout_sec=1.0):
            if not rclpy.ok():
                return None
            self.get_logger().error("dispense request Service not available, waiting again...")

        try:
            future = self._get_dis_station_cli(station_id).call_async(req)
            future.add_done_callback(partial(self.dispense_done_cb))

            self.get_logger().debug(f"dis_station_clis [{station_id}] is called, waiting for future done")
            return True
        except AttributeError as e:
            self.get_logger().error(f"Invalid service response {station_id}: {str(e)}")
            return False
        except Exception as e:
            self.get_logger().error(f"Service call failed {station_id}: {str(e)}")
            return False

    def dispense_done_cb(self, future) -> None:
        res = future.result()

        if res and res.success:
           self.get_logger().error(f"OK: {res}")
        else:
            self.get_logger().error(f"Service call succeeded but reported failure for dispenser station: {station_id}")

    def _get_dis_station_cli(self, station_id: int) -> Optional[rclpy.client.Client]:
        """Safely get a dispenser station client by ID."""
        return self.dis_station_clis.get(station_id)


def main(args=None):
    rclpy.init(args=args)
    
    executor = MultiThreadedExecutor()

    cli_node = FakeDispenseClient()
    executor.add_node(cli_node)

    executor.spin()
    rclpy.shutdown()


if __name__ == '__main__':
    main()