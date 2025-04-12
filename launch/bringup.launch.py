import os

from launch import LaunchDescription
from launch_ros.actions import Node

from ament_index_python.packages import get_package_share_directory

def generate_launch_description():

    pkg_name = "prod_line_sys"

    # params_file = os.path.join(
    #     get_package_share_directory("prod_line_sys"),
    #     "params",
    #     "config.yaml"
    # )
    
    ld = LaunchDescription()
    
    core = Node(
        package=pkg_name,
        executable="bringup_node",
        output="screen",
        emulate_tty=True
    )

    action_ser = Node(
        package=pkg_name,
        executable="new_order_action_ser_node",
        name="new_order_action_ser_node",
        output="screen",
        emulate_tty=True
    )

    ld.add_action(core)
    ld.add_action(action_ser)

    return ld