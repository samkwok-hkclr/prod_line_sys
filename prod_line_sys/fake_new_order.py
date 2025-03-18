import random

import rclpy
from rclpy.node import Node

from smdps_msgs.msg import OrderRequest, MaterialBox, MaterialBoxSlot, Drug, DrugLocation
from smdps_msgs.srv import NewOrder

class FakeNewOrder(Node):
    def __init__(self):
        super().__init__('fake_new_order_node')
        self.cli = self.create_client(NewOrder, 'new_order')
        while not self.cli.wait_for_service(timeout_sec=1.0):
            self.get_logger().info('service not available, waiting again...')
        self.req = NewOrder.Request()

    def send_request(self, req):
        self.req.request = req
        self.future = self.cli.call_async(self.req)
        rclpy.spin_until_future_complete(self, self.future)
        return self.future.result()


def main(args=None):
    rclpy.init(args=args)

    fake_new_order = FakeNewOrder()
    req = OrderRequest()

    mtrl_box = MaterialBox()

    location = DrugLocation()
    location.dispenser_station = random.randrange(3, 14)
    location.dispenser_unit = 5

    drug_1 = Drug()
    drug_1.drug_id = "123"
    drug_1.amount = 1
    drug_1.locations.append(location)

    mtrl_box.slots[0].drugs.append(drug_1)
    
    random.seed()
    req.order_id = int(random.random()*10000000000)

    req.material_box = mtrl_box
    response = fake_new_order.send_request(req)
    fake_new_order.get_logger().info("sent!")
    fake_new_order.destroy_node()
    rclpy.shutdown()


if __name__ == '__main__':
    main()