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
import unittest
import sys
import time

from mock import patch, call, MagicMock

import ceph

# python-apt is not installed as part of test-requirements but is imported by
# some charmhelpers modules so create a fake import.
mock_apt = MagicMock()
sys.modules['apt'] = mock_apt
mock_apt.apt_pkg = MagicMock()


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
            'mon_ip-192-168-1-2_0.94.1_done':
        return False
    elif args[1] == \
            'mon_ip-192-168-1-2_0.94.1_start':
        # Return that the previous node started 9 minutes ago
        return previous_node_start_time


class UpgradeRollingTestCase(unittest.TestCase):

    @patch('time.time')
    @patch('ceph.log')
    @patch('ceph.upgrade_monitor')
    @patch('ceph.monitor_key_set')
    def test_lock_and_roll(self, monitor_key_set, upgrade_monitor, log, time):
        time.return_value = 1473279502.69
        monitor_key_set.monitor_key_set.return_value = None
        ceph.lock_and_roll(my_name='ip-192-168-1-2',
                           version='hammer',
                           service='mon',
                           upgrade_key='admin')
        upgrade_monitor.assert_called_once_with('hammer')
        log.assert_has_calls(
            [
                call('monitor_key_set '
                     'mon_ip-192-168-1-2_hammer_start 1473279502.69'),
                call('Rolling'),
                call('Done'),
                call('monitor_key_set '
                     'mon_ip-192-168-1-2_hammer_done 1473279502.69'),
            ])

    @patch('ceph.apt_install')
    @patch('ceph.chownr')
    @patch('ceph.service_stop')
    @patch('ceph.service_start')
    @patch('ceph.log')
    @patch('ceph.status_set')
    @patch('ceph.apt_update')
    @patch('ceph.add_source')
    @patch('ceph.get_local_mon_ids')
    @patch('ceph.systemd')
    @patch('ceph.get_version')
    @patch('ceph.config')
    def test_upgrade_monitor_hammer(self, config, get_version,
                                    systemd, local_mons, add_source,
                                    apt_update, status_set, log,
                                    service_start, service_stop, chownr,
                                    apt_install):
        get_version.side_effect = [0.80, 0.94]
        config.side_effect = config_side_effect
        systemd.return_value = False
        local_mons.return_value = ['a']

        ceph.upgrade_monitor('hammer')
        service_stop.assert_called_with('ceph-mon-all')
        service_start.assert_called_with('ceph-mon-all')
        add_source.assert_called_with('cloud:trusty-kilo', 'key')

        log.assert_has_calls(
            [
                call('Current ceph version is 0.8'),
                call('Upgrading to: hammer')
            ]
        )
        status_set.assert_has_calls([
            call('maintenance', 'Upgrading monitor'),
        ])
        assert not chownr.called

    @patch('ceph.apt_install')
    @patch('ceph.chownr')
    @patch('ceph.service_stop')
    @patch('ceph.service_start')
    @patch('ceph.log')
    @patch('ceph.status_set')
    @patch('ceph.apt_update')
    @patch('ceph.add_source')
    @patch('ceph.get_local_mon_ids')
    @patch('ceph.systemd')
    @patch('ceph.get_version')
    @patch('ceph.config')
    def test_upgrade_monitor_jewel(self, config, get_version,
                                   systemd, local_mons, add_source,
                                   apt_update, status_set, log,
                                   service_start, service_stop, chownr,
                                   apt_install):
        get_version.side_effect = [0.94, 10.1]
        config.side_effect = config_side_effect
        systemd.return_value = False
        local_mons.return_value = ['a']

        ceph.upgrade_monitor('jewel')
        service_stop.assert_called_with('ceph-mon-all')
        service_start.assert_called_with('ceph-mon-all')
        add_source.assert_called_with('cloud:trusty-kilo', 'key')

        log.assert_has_calls(
            [
                call('Current ceph version is 0.94'),
                call('Upgrading to: jewel')
            ]
        )
        status_set.assert_has_calls([
            call('maintenance', 'Upgrading monitor'),
        ])
        chownr.assert_has_calls(
            [
                call(group='ceph', owner='ceph', path='/var/lib/ceph',
                     follow_links=True)
            ]
        )

    @patch('ceph.get_version')
    @patch('ceph.status_set')
    @patch('ceph.lock_and_roll')
    @patch('ceph.wait_on_previous_node')
    @patch('ceph.get_mon_map')
    @patch('ceph.socket')
    def test_roll_monitor_cluster_second(self,
                                         socket,
                                         get_mon_map,
                                         wait_on_previous_node,
                                         lock_and_roll,
                                         status_set,
                                         get_version):
        get_version.return_value = "0.94.1"
        wait_on_previous_node.return_value = None
        socket.gethostname.return_value = "ip-192-168-1-3"
        get_mon_map.return_value = {
            'monmap': {
                'mons': [
                    {
                        'name': 'ip-192-168-1-2',
                    },
                    {
                        'name': 'ip-192-168-1-3',
                    },
                ]
            }
        }
        ceph.roll_monitor_cluster(new_version='0.94.1',
                                  upgrade_key='admin')
        status_set.assert_called_with(
            'waiting',
            'Waiting on ip-192-168-1-2 to finish upgrading')
        lock_and_roll.assert_called_with(my_name='ip-192-168-1-3',
                                         service='mon',
                                         upgrade_key='admin',
                                         version='0.94.1')

    @patch('ceph.log')
    @patch.object(ceph, 'time')
    @patch('ceph.monitor_key_get')
    @patch('ceph.monitor_key_exists')
    def test_wait_on_previous_node(self, monitor_key_exists, monitor_key_get,
                                   mock_time, log):
        tval = [previous_node_start_time]

        def fake_time():
            tval[0] += 100
            return tval[0]

        mock_time.time.side_effect = fake_time
        monitor_key_get.side_effect = monitor_key_side_effect
        monitor_key_exists.return_value = False

        ceph.wait_on_previous_node(previous_node="ip-192-168-1-2",
                                   version='0.94.1',
                                   service='mon',
                                   upgrade_key='admin')

        # Make sure we checked to see if the previous node started
        monitor_key_get.assert_has_calls(
            [call('admin', 'mon_ip-192-168-1-2_0.94.1_start')]
        )
        # Make sure we checked to see if the previous node was finished
        monitor_key_exists.assert_has_calls(
            [call('admin', 'mon_ip-192-168-1-2_0.94.1_done')]
        )
        # Make sure we waited at last once before proceeding
        log.assert_has_calls(
            [call('Previous node is: ip-192-168-1-2')],
            [call('ip-192-168-1-2 is not finished. Waiting')],
        )
        self.assertEqual(tval[0], previous_node_start_time + 700)
