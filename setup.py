import os
from glob import glob
from setuptools import find_packages, setup

package_name = 'prod_line_sys'

setup(
    name=package_name,
    version='0.0.0',
    packages=find_packages(exclude=['test']),
    data_files=[
        ('share/ament_index/resource_index/packages', ['resource/' + package_name]),
        ('share/' + package_name, ['package.xml']),
        (os.path.join('share', package_name, 'launch'), glob('launch/*.launch.py')),
    ],
    install_requires=['setuptools'],
    zip_safe=True,
    maintainer='mskwok',
    maintainer_email='mskwok@hkclr.hk',
    description='TODO: Package description',
    license='TODO: License declaration',
    tests_require=['pytest'],
    entry_points={
        'console_scripts': [
            'core_sys_node = prod_line_sys.core_sys:main',
            'new_order_action_ser_node = prod_line_sys.new_order_action_ser:main',
            'fake_new_order_node = prod_line_sys.fake_new_order:main',
            'fake_pkg_order_ser_node = prod_line_sys.fake_pkg_order_ser:main',
            'fake_rel_blk_ser_node = prod_line_sys.fake_rel_blk_ser:main',
            'fake_dis_station_node = prod_line_sys.fake_dis_station:main',
        ],
    },
)
