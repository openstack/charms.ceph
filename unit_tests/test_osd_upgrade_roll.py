# Copyright 2016 Canonical Ltd
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#  http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import time
import unittest

from mock import patch, call

import ceph
from ceph import CrushLocation

TO_PATCH = [
    'apt_install',
    'apt_update',
    'add_source',
    'config',
    'ceph',
    'get_conf',
    'hookenv',
    'host',
    'log',
    'service_start',
    'service_stop',
    'socket',
    'status_set',
    'chownr',
]


def config_side_effect(*args):
    if args[0] == 'source':
        return 'cloud:trusty-kilo'
    elif args[0] == 'key':
        return 'key'
    elif args[0] == 'release-version':
        return 'cloud:trusty-kilo'


previous_node_start_time = time.time() - (9 * 60)


def monitor_key_side_effect(*args):
    if args[1] == \
            'ip-192-168-1-2_0.94.1_done':
        return False
    elif args[1] == \
            'ip-192-168-1-2_0.94.1_start':
        # Return that the previous node started 9 minutes ago
        return previous_node_start_time


class UpgradeRollingTestCase(unittest.TestCase):
    @patch('ceph.apt_install')
    @patch('ceph.chownr')
    @patch('ceph.service_stop')
    @patch('ceph.service_start')
    @patch('ceph.log')
    @patch('ceph.status_set')
    @patch('ceph.apt_update')
    @patch('ceph.add_source')
    @patch('ceph.get_local_osd_ids')
    @patch('ceph.systemd')
    @patch('ceph.ceph_user')
    @patch('ceph.get_version')
    @patch('ceph.config')
    def test_upgrade_osd(self, config, get_version, ceph_user, systemd,
                         local_osds, add_source, apt_update, status_set,
                         log, service_start, service_stop, chownr,
                         apt_install):
        config.side_effect = config_side_effect
        get_version.return_value = "0.80"
        ceph_user.return_value = "ceph"
        systemd.return_value = False
        local_osds.return_value = [0, 1, 2]

        ceph.upgrade_osd('hammer')
        service_stop.assert_called_with('ceph-osd-all')
        service_start.assert_called_with('ceph-osd-all')
        status_set.assert_has_calls([
            call('maintenance', 'Upgrading osd'),
        ])
        log.assert_has_calls(
            [
                call('Current ceph version is 0.80'),
                call('Upgrading to: hammer')
            ]
        )
        chownr.assert_has_calls(
            [
                call(group='ceph', owner='ceph', path='/var/lib/ceph',
                     follow_links=True)
            ]
        )

    @patch('ceph.socket')
    @patch('ceph.get_osd_tree')
    @patch('ceph.log')
    @patch('ceph.lock_and_roll')
    @patch('ceph.get_upgrade_position')
    def test_roll_osd_cluster_first(self,
                                    get_upgrade_position,
                                    lock_and_roll,
                                    log,
                                    get_osd_tree,
                                    socket):
        socket.gethostname.return_value = "ip-192-168-1-2"
        get_osd_tree.return_value = ""
        get_upgrade_position.return_value = 0

        ceph.roll_osd_cluster(new_version='0.94.1',
                              upgrade_key='osd-upgrade')
        log.assert_has_calls(
            [
                call('roll_osd_cluster called with 0.94.1'),
                call('osd_sorted_list: []'),
                call('upgrade position: 0')
            ]
        )
        lock_and_roll.assert_called_with(my_name="ip-192-168-1-2",
                                         version="0.94.1",
                                         upgrade_key='osd-upgrade',
                                         service='osd')

    @patch('ceph.get_osd_tree')
    @patch('ceph.socket')
    @patch('ceph.status_set')
    @patch('ceph.lock_and_roll')
    @patch('ceph.get_upgrade_position')
    @patch('ceph.wait_on_previous_node')
    def test_roll_osd_cluster_second(self,
                                     wait_on_previous_node,
                                     get_upgrade_position,
                                     lock_and_roll,
                                     status_set,
                                     socket,
                                     get_osd_tree):
        wait_on_previous_node.return_value = None
        socket.gethostname.return_value = "ip-192-168-1-3"
        get_osd_tree.return_value = [
            CrushLocation(
                name="ip-192-168-1-2",
                identifier='a',
                host='host-a',
                rack='rack-a',
                row='row-a',
                datacenter='dc-1',
                chassis='chassis-a',
                root='ceph'),
            CrushLocation(
                name="ip-192-168-1-3",
                identifier='a',
                host='host-b',
                rack='rack-a',
                row='row-a',
                datacenter='dc-1',
                chassis='chassis-a',
                root='ceph')
        ]
        get_upgrade_position.return_value = 1

        ceph.roll_osd_cluster(new_version='0.94.1',
                              upgrade_key='osd-upgrade')
        status_set.assert_called_with(
            'blocked',
            'Waiting on ip-192-168-1-2 to finish upgrading')
        lock_and_roll.assert_called_with(my_name='ip-192-168-1-3',
                                         service='osd',
                                         upgrade_key='osd-upgrade',
                                         version='0.94.1')


"""
    @patch('ceph.log')
    @patch('time.time', lambda *args: previous_node_start_time + 10 * 60 + 1)
    @patch('ceph.monitor_key_get')
    @patch('ceph.monitor_key_exists')
    def test_wait_on_previous_node(self,
                                   monitor_key_exists,
                                   monitor_key_get,
                                   log):
        monitor_key_get.side_effect = monitor_key_side_effect
        monitor_key_exists.return_value = False

        ceph.wait_on_previous_node(previous_node="ip-192-168-1-2",
                                   version='0.94.1',
                                   service='osd',
                                   upgrade_key='osd-upgrade')

        # Make sure we checked to see if the previous node started
        monitor_key_get.assert_has_calls(
            [call('osd-upgrade', 'ip-192-168-1-2_0.94.1_start')]
        )
        # Make sure we checked to see if the previous node was finished
        monitor_key_exists.assert_has_calls(
            [call('osd-upgrade', 'ip-192-168-1-2_0.94.1_done')]
        )
        # Make sure we waited at last once before proceeding
        log.assert_has_calls(
            [call('Previous node is: ip-192-168-1-2')],
            [call('ip-192-168-1-2 is not finished. Waiting')],
        )
"""
