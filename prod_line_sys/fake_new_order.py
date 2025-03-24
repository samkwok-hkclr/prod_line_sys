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
    req.patient.name = "Sam Kwok"
    req.patient.institute_name = "HKCLR"

    mtrl_box = MaterialBox()

    location_1 = DrugLocation()
    location_1.dispenser_station = 1
    location_1.dispenser_unit = 5

    location_2 = DrugLocation()
    location_2.dispenser_station = 5
    location_2.dispenser_unit = 8

    drug_1 = Drug()
    drug_1.drug_id = "123"
    drug_1.amount = 1
    drug_1.locations.append(location_1)

    drug_2 = Drug()
    drug_2.drug_id = "123"
    drug_2.amount = 1
    drug_2.locations.append(location_2)


    mtrl_box.slots[0].drugs.append(drug_1)
    mtrl_box.slots[24].drugs.append(drug_1)
    mtrl_box.slots[4].drugs.append(drug_2)
    mtrl_box.slots[24].drugs.append(drug_2)
    random.seed()
    req.order_id = int(random.random()*1000000)

    req.material_box = mtrl_box
    response = fake_new_order.send_request(req)
    fake_new_order.get_logger().info("sent!")
    fake_new_order.destroy_node()
    rclpy.shutdown()


if __name__ == '__main__':
    main()