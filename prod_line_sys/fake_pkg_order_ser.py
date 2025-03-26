import rclpy
from rclpy.node import Node

from smdps_msgs.srv import PackagingOrder

class FakePackagingOrder(Node):
    def __init__(self):
        super().__init__("fake_pkg_order")
        self.srv = self.create_service(PackagingOrder, "packaging_order", self.pkg_order_cb)
        self.get_logger().info("/packaging_order service server is started")

    def pkg_order_cb(self, req, res):
        self.get_logger().info("packaging_order service received")

        res.success = True
        return res


def main(args=None):
    rclpy.init(args=args)
    fake_pkg_order = FakePackagingOrder()
    rclpy.spin(fake_pkg_order)
    rclpy.shutdown()


if __name__ == "__main__":
    main()