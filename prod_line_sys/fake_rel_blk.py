import rclpy
from rclpy.node import Node

from std_srvs.srv import Trigger

class FakeRelBlk(Node):
    def __init__(self):
        super().__init__("fake_rel_blk")
        self.srv = self.create_service(Trigger, "release_blocking", self.rel_blk_cb)
        self.get_logger().info("/release_blocking service server is started")

    def rel_blk_cb(self, req, res):
        self.get_logger().info("release_blocking service received")

        res.success = True
        return res


def main(args=None):
    rclpy.init(args=args)
    fake_rel_blk = FakeRelBlk()
    rclpy.spin(fake_rel_blk)
    rclpy.shutdown()


if __name__ == "__main__":
    main()