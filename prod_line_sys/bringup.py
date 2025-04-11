import sys
import rclpy

from rclpy.executors import ExternalShutdownException, MultiThreadedExecutor

from .core_sys import CoreSystem
from .init_vision import InitVision
from .container import Container

def main(args=None):
    rclpy.init(args=args)

    try:
        executor = MultiThreadedExecutor()

        core_sys_node = CoreSystem(executor)
        init_vision_node = InitVision()
        container_node = Container()

        executor.add_node(core_sys_node)
        executor.add_node(init_vision_node)
        executor.add_node(container_node)

        try:
            executor.spin()
        finally:
            for node in executor.get_nodes():
                node.destroy_node()
            executor.shutdown()
    except KeyboardInterrupt:
        pass
    except ExternalShutdownException:
        sys.exit(1)
    finally:
        rclpy.try_shutdown()

if __name__ == "__main__":
    main()

