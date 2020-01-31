# Copyright 2019 Open Source Robotics Foundation, Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import socket
import struct
import threading
import time

import rclpy
from rclpy.executors import MultiThreadedExecutor
from rclpy.node import Node
from ros2doctor.verb import VerbExtension

from std_msgs.msg import String

DEFAULT_GROUP = '225.0.0.1'
DEFAULT_PORT = 49150
summary_table = {}  # need to be put into a thread safe object


class CallVerb(VerbExtension):
    """
    Check network connectivity between multiple hosts.

    This command can be invoked on multiple hosts to confirm that they can talk to each other.

    TODO <describe how pub/sub is used to test ROS comms>
    TODO <describe how multicast send/receive is used to confirm multicast UDP comms>
    """

    def add_arguments(self, parser, cli_name):
        arg = parser.add_argument(
            'topic_name', nargs='?', default='/canyouhearme',
            help="Name of ROS topic to publish to (e.g. '/canyouhearme')")
        arg = parser.add_argument(
            'time_period', nargs='?', default=0.1,
            help='time period to publish one message')
        arg = parser.add_argument(
            'qos', nargs='?', default=10,
            help="quality of service profile to publish message")
        parser.add_argument(
            '-r', '--rate', metavar='N', type=float, default=1.0,
            help='Emitting rate in Hz (default: 1.0)')
        parser.add_argument(
            '-1', '--once', action='store_true',
            help='Emit one message and exit')


    def main(self, *, args):
        rclpy.init()
        pub_node = Talker(args.topic_name, args.time_period, args.qos)
        sub_node = Listener(args.topic_name, args.qos)

        executor = MultiThreadedExecutor()
        executor.add_node(pub_node)
        executor.add_node(sub_node)
        try:
            count = 0
            _zero_init_summary_table()
            while True:
                if (count % 20 == 0 and count != 0):
                    _format_print_summary(summary_table, args.topic_name)
                    _zero_init_summary_table()
                # pub/sub threads
                executor.spin_once()
                executor.spin_once()
                # multicast threads
                send_thread = threading.Thread(target=_send, args=())
                send_thread.daemon = True
                receive_thread = threading.Thread(target=_receive, args=())
                receive_thread.daemon = True
                receive_thread.start()
                send_thread.start()
                count += 1
                time.sleep(0.1)
        except KeyboardInterrupt:
            executor.shutdown()
            pub_node.destroy_node()
            sub_node.destroy_node()


class Talker(Node):
    """Initialize talker node."""

    def __init__(self, topic, period, qos):
        super().__init__('ros2doctor_talker')
        self.i = 0
        self.pub = self.create_publisher(String, topic, qos)
        self.timer = self.create_timer(period, self.timer_callback)

    def timer_callback(self):
        msg = String()
        hostname = socket.gethostname()
        # publish
        msg.data = f"hello, it's me {hostname}"
        summary_table['pub'] += 1
        self.pub.publish(msg)
        self.i += 1


class Listener(Node):
    """Initialize listener node."""

    def __init__(self, topic, qos):
        super().__init__('ros2doctor_listener')
        self.sub = self.create_subscription(
            String,
            topic,
            self.sub_callback,
            qos)

    def sub_callback(self, msg):
        # subscribe
        msg_data = msg.data.split()
        caller_hostname = msg_data[-1]
        if caller_hostname != socket.gethostname():
            if caller_hostname not in summary_table['sub']:
                summary_table['sub'][caller_hostname] = 1
            else:
                summary_table['sub'][caller_hostname] += 1


def _send(*, group=DEFAULT_GROUP, port=DEFAULT_PORT, ttl=None):
    """Multicast send."""
    hostname = socket.gethostname()
    s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM, socket.IPPROTO_UDP)
    if ttl is not None:
        packed_ttl = struct.pack('b', ttl)
        s.setsockopt(socket.IPPROTO_IP, socket.IP_MULTICAST_TTL, packed_ttl)
    try:
        summary_table['send'] += 1
        s.sendto(f"hello, it's me {hostname}".encode('utf-8'), (group, port))
    finally:
        s.close()


def _receive(*, group=DEFAULT_GROUP, port=DEFAULT_PORT, timeout=None):
    """Multicast receive."""
    s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM, socket.IPPROTO_UDP)
    try:
        s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        try:
            s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEPORT, 1)
        except AttributeError:
            # not available on Windows
            pass
        s.bind(('', port))

        s.settimeout(timeout)

        mreq = struct.pack('4sl', socket.inet_aton(group), socket.INADDR_ANY)
        s.setsockopt(socket.IPPROTO_IP, socket.IP_ADD_MEMBERSHIP, mreq)
        try:
            data, _ = s.recvfrom(4096)
            data = data.decode('utf-8')
            sender_hostname = data.split()[-1]
            if sender_hostname != socket.gethostname():
                if sender_hostname not in summary_table['receive']:
                    summary_table['receive'][sender_hostname] = 1
                else:
                    summary_table['receive'][sender_hostname] += 1
        finally:
            s.setsockopt(socket.IPPROTO_IP, socket.IP_DROP_MEMBERSHIP, mreq)
    finally:
        s.close()


def _zero_init_summary_table():
    """Spawn summary table with new content after each print."""
    summary_table['pub'] = 0
    summary_table['sub'] = {}
    summary_table['send'] = 0
    summary_table['receive'] = {}


def _format_print_summary_helper(table):
    """Format summary table."""
    print('{:<15} {:<20} {:<10}'.format('', 'Hostname', 'Msg Count /2s'))
    for name, count in table.items():
        print('{:<15} {:<20} {:<10}'.format('', name, count))


def _format_print_summary(table, topic):
    """Print content in summary table."""
    pub_count = table['pub']
    send_count = table['send']
    print('MULTIMACHINE COMMUNICATION SUMMARY')
    print(f'Topic: {topic}, Published Msg Count: {pub_count}')
    print('Subscribed from:')
    _format_print_summary_helper(table['sub'])
    print(f'Multicast Group/Port: {DEFAULT_GROUP}/{DEFAULT_PORT}, Sent Msg Count: {send_count}')
    print('Received from:')
    _format_print_summary_helper(table['receive'])
    print('-'*60)
