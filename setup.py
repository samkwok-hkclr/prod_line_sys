from setuptools import find_packages, setup

package_name = 'prod_line_sys'

setup(
    name=package_name,
    version='0.0.0',
    packages=find_packages(exclude=['test']),
    data_files=[
        ('share/ament_index/resource_index/packages', ['resource/' + package_name]),
        ('share/' + package_name, ['package.xml']),
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
            'core_sys = prod_line_sys.core_sys:main',
            'fake_new_order_node = prod_line_sys.fake_new_order:main'
        ],
    },
)
