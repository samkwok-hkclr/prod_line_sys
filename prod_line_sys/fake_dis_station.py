import time
import rclpy
from rclpy.node import Node

from smdps_msgs.srv import DispenseDrug

class FakeDispenserStation(Node):
    def __init__(self, station_id: int):
        super().__init__('fake_dis_station')
        self.station_id = station_id
        self.sleep_rate = self.create_rate(1.0, self.get_clock())
        self.srv = self.create_service(DispenseDrug, f"/dispenser_station_{station_id}/dispense_request", self.dis_drug_cb)
        self.get_logger().info(f"/dispenser_station_{station_id}/dispense_request service server is started")

    def dis_drug_cb(self, req, res):
        self.get_logger().info(f"/dispenser_station_{self.station_id}/dispense_request service call received")
        self.sleep_rate.sleep()
        res.success = True
        self.get_logger().info(f"/dispenser_station_{self.station_id}/dispense_request service call done")
        return res


def main(args=None):
    rclpy.init(args=args)
    fake_dis_station = FakeDispenserStation(1)
    rclpy.spin(fake_dis_station)
    rclpy.shutdown()


if __name__ == '__main__':
    main()