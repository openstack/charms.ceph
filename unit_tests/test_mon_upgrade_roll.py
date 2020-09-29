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

import sys
import time
import unittest

from unittest.mock import patch, call, MagicMock, ANY

import charms_ceph.utils

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
        # NOTE(jamespage):
        # Pass back as string as this is what we actually get
        # from the monitor cluster
        return str(previous_node_start_time)


def monitor_key_exists_side_effect(*args):
    if args[1] == 'mon_ip-192-168-1-2_0.94.1_start':
        return True
    if args[1] == 'mon_ip-192-168-1-2_0.94.1_done':
        return False
    raise Exception("Unexpected test argument")


class UpgradeRollingTestCase(unittest.TestCase):

    @patch('time.time')
    @patch.object(charms_ceph.utils, 'log')
    @patch.object(charms_ceph.utils, 'upgrade_monitor')
    @patch.object(charms_ceph.utils, 'monitor_key_set')
    def test_lock_and_roll(self, monitor_key_set, upgrade_monitor, log, time):
        time.return_value = 1473279502.69
        monitor_key_set.monitor_key_set.return_value = None
        charms_ceph.utils.lock_and_roll(my_name='ip-192-168-1-2',
                                        version='hammer',
                                        service='mon',
                                        upgrade_key='admin')
        upgrade_monitor.assert_called_once_with('hammer',
                                                kick_function=ANY)
        log.assert_has_calls(
            [
                call('monitor_key_set '
                     'mon_ip-192-168-1-2_hammer_start 1473279502.69'),
                call('Rolling'),
                call('Done'),
                call('monitor_key_set '
                     'mon_ip-192-168-1-2_hammer_done 1473279502.69'),
            ])

    @patch.object(charms_ceph.utils, 'cmp_pkgrevno')
    @patch.object(charms_ceph.utils, 'determine_packages')
    @patch.object(charms_ceph.utils, 'ceph_user')
    @patch.object(charms_ceph.utils, 'socket')
    @patch.object(charms_ceph.utils, 'mkdir')
    @patch.object(charms_ceph.utils, 'apt_install')
    @patch.object(charms_ceph.utils, 'chownr')
    @patch.object(charms_ceph.utils, 'service_stop')
    @patch.object(charms_ceph.utils, 'service_start')
    @patch.object(charms_ceph.utils, 'log')
    @patch.object(charms_ceph.utils, 'status_set')
    @patch.object(charms_ceph.utils, 'apt_update')
    @patch.object(charms_ceph.utils, 'add_source')
    @patch.object(charms_ceph.utils, 'get_local_mon_ids')
    @patch.object(charms_ceph.utils, 'systemd')
    @patch.object(charms_ceph.utils, 'get_version')
    @patch.object(charms_ceph.utils, 'config')
    def test_upgrade_monitor_hammer(self, config, get_version,
                                    systemd, local_mons, add_source,
                                    apt_update, status_set, log,
                                    service_start, service_stop, chownr,
                                    apt_install, mkdir, socket,
                                    ceph_user, _determine_packages,
                                    mock_cmp_pkgrevno):
        get_version.side_effect = [0.80, 0.94]
        config.side_effect = config_side_effect
        systemd.return_value = False
        socket.gethostname.return_value = 'testmon'
        ceph_user.return_value = 'root'
        local_mons.return_value = ['a']
        mock_cmp_pkgrevno.return_value = -1

        mock_kick_function = MagicMock()

        charms_ceph.utils.upgrade_monitor('hammer',
                                          kick_function=mock_kick_function)
        service_stop.assert_called_with('ceph-mon-all')
        service_start.assert_called_with('ceph-mon-all')
        add_source.assert_called_with('cloud:trusty-kilo', 'key')
        mock_kick_function.assert_called()

        log.assert_has_calls(
            [
                call('Current ceph version is 0.8'),
                call('Upgrading to: hammer')
            ]
        )
        status_set.assert_has_calls([
            call('maintenance', 'Upgrading monitor'),
        ])
        mkdir.assert_called_with('/var/lib/ceph/mon/ceph-testmon',
                                 owner='root',
                                 group='root',
                                 perms=0o755)
        chownr.assert_not_called()

    @patch.object(charms_ceph.utils, 'cmp_pkgrevno')
    @patch.object(charms_ceph.utils, 'determine_packages')
    @patch.object(charms_ceph.utils, 'ceph_user')
    @patch.object(charms_ceph.utils, 'socket')
    @patch.object(charms_ceph.utils, 'mkdir')
    @patch.object(charms_ceph.utils, 'apt_install')
    @patch.object(charms_ceph.utils, 'chownr')
    @patch.object(charms_ceph.utils, 'service_stop')
    @patch.object(charms_ceph.utils, 'service_start')
    @patch.object(charms_ceph.utils, 'log')
    @patch.object(charms_ceph.utils, 'status_set')
    @patch.object(charms_ceph.utils, 'apt_update')
    @patch.object(charms_ceph.utils, 'add_source')
    @patch.object(charms_ceph.utils, 'get_local_mon_ids')
    @patch.object(charms_ceph.utils, 'systemd')
    @patch.object(charms_ceph.utils, 'get_version')
    @patch.object(charms_ceph.utils, 'config')
    def test_upgrade_monitor_jewel(self, config, get_version,
                                   systemd, local_mons, add_source,
                                   apt_update, status_set, log,
                                   service_start, service_stop, chownr,
                                   apt_install, mkdir, socket,
                                   ceph_user, _determine_packages,
                                   mock_cmp_pkgrevno):
        get_version.side_effect = [0.94, 10.1]
        config.side_effect = config_side_effect
        systemd.return_value = False
        socket.gethostname.return_value = 'testmon'
        ceph_user.return_value = 'ceph'
        local_mons.return_value = ['a']
        mock_cmp_pkgrevno.return_value = -1

        charms_ceph.utils.upgrade_monitor('jewel')
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
        mkdir.assert_called_with('/var/lib/ceph/mon/ceph-testmon',
                                 owner='ceph',
                                 group='ceph',
                                 perms=0o755)

    @patch.object(charms_ceph.utils, 'cmp_pkgrevno')
    @patch.object(charms_ceph.utils, 'determine_packages')
    @patch.object(charms_ceph.utils, 'ceph_user')
    @patch.object(charms_ceph.utils, 'socket')
    @patch.object(charms_ceph.utils, 'mkdir')
    @patch.object(charms_ceph.utils, 'apt_install')
    @patch.object(charms_ceph.utils, 'chownr')
    @patch.object(charms_ceph.utils, 'service_stop')
    @patch.object(charms_ceph.utils, 'service_start')
    @patch.object(charms_ceph.utils, 'service_restart')
    @patch.object(charms_ceph.utils, 'log')
    @patch.object(charms_ceph.utils, 'status_set')
    @patch.object(charms_ceph.utils, 'apt_update')
    @patch.object(charms_ceph.utils, 'add_source')
    @patch.object(charms_ceph.utils, 'systemd')
    @patch.object(charms_ceph.utils, 'get_version')
    @patch.object(charms_ceph.utils, 'config')
    def test_upgrade_monitor_luminous(self, config, get_version,
                                      systemd, add_source,
                                      apt_update, status_set, log,
                                      service_restart,
                                      service_start, service_stop, chownr,
                                      apt_install, mkdir, socket,
                                      ceph_user, _determine_packages,
                                      mock_cmp_pkgrevno):
        get_version.side_effect = [10.2, 12.2]
        config.side_effect = config_side_effect
        socket.gethostname.return_value = 'testmon'
        ceph_user.return_value = 'ceph'
        systemd.return_value = True
        mock_cmp_pkgrevno.return_value = 0   # it is luminous

        charms_ceph.utils.upgrade_monitor('luminous')
        service_stop.assert_any_call('ceph-mon')
        service_stop.assert_any_call('ceph-mgr.target')
        service_restart.assert_any_call('ceph-mon')
        service_restart.assert_any_call('ceph-mgr.target')
        add_source.assert_called_with('cloud:trusty-kilo', 'key')

        log.assert_has_calls(
            [
                call('Current ceph version is 10.2'),
                call('Upgrading to: luminous')
            ]
        )
        status_set.assert_has_calls([
            call('maintenance', 'Upgrading monitor'),
        ])
        chownr.assert_not_called()
        mkdir.assert_called_with('/var/lib/ceph/mon/ceph-testmon',
                                 owner='ceph',
                                 group='ceph',
                                 perms=0o755)

    @patch.object(charms_ceph.utils, 'upgrade_monitor')
    @patch.object(charms_ceph.utils, 'bootstrap_manager')
    @patch.object(charms_ceph.utils, 'wait_for_all_monitors_to_upgrade')
    @patch.object(charms_ceph.utils, 'status_set')
    @patch.object(charms_ceph.utils, 'lock_and_roll')
    @patch.object(charms_ceph.utils, 'wait_on_previous_node')
    @patch.object(charms_ceph.utils, 'get_mon_map')
    @patch.object(charms_ceph.utils, 'socket')
    def _test_roll_monitor_cluster(self,
                                   socket,
                                   get_mon_map,
                                   wait_on_previous_node,
                                   lock_and_roll,
                                   status_set,
                                   wait_for_all_monitors_to_upgrade,
                                   bootstrap_manager,
                                   upgrade_monitor,
                                   new_version):
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
        charms_ceph.utils.roll_monitor_cluster(new_version=new_version,
                                               upgrade_key='admin')
        get_mon_map.assert_called_once_with('admin')
        wait_on_previous_node.assert_called_with(
            upgrade_key='admin',
            service='mon',
            previous_node='ip-192-168-1-2',
            version=new_version,
        )
        status_set.assert_called_with(
            'waiting',
            'Waiting on ip-192-168-1-2 to finish upgrading')
        lock_and_roll.assert_called_with(my_name='ip-192-168-1-3',
                                         service='mon',
                                         upgrade_key='admin',
                                         version=new_version)
        if new_version == 'luminous':
            wait_for_all_monitors_to_upgrade.assert_called_with(
                new_version=new_version,
                upgrade_key='admin',
            )
            bootstrap_manager.assert_called_once_with()
        else:
            wait_for_all_monitors_to_upgrade.assert_not_called()
            bootstrap_manager.assert_not_called()

        upgrade_monitor.assert_has_calls([
            call(new_version, restart_daemons=False)])

    def test_roll_monitor_cluster_luminous(self):
        self._test_roll_monitor_cluster(new_version='luminous')

    def test_roll_monitor_cluster_jewel(self):
        self._test_roll_monitor_cluster(new_version='jewel')

    def test_roll_monitor_cluster_hammer(self):
        self._test_roll_monitor_cluster(new_version='hammer')

    @patch.object(charms_ceph.utils, 'log')
    @patch.object(charms_ceph.utils, 'time')
    @patch.object(charms_ceph.utils, 'monitor_key_get')
    @patch.object(charms_ceph.utils, 'monitor_key_exists')
    def test_wait_on_previous_node(self, monitor_key_exists, monitor_key_get,
                                   mock_time, log):
        tval = [previous_node_start_time]

        def fake_time():
            tval[0] += 100
            return tval[0]

        mock_time.time.side_effect = fake_time
        monitor_key_get.side_effect = monitor_key_side_effect
        monitor_key_exists.side_effect = monitor_key_exists_side_effect

        charms_ceph.utils.wait_on_previous_node(
            previous_node="ip-192-168-1-2",
            version='0.94.1',
            service='mon',
            upgrade_key='admin'
        )
        # Make sure the function tested that start exists.
        monitor_key_exists.assert_any_call('admin',
                                           'mon_ip-192-168-1-2_0.94.1_start')
        # Make sure the Watchdog checked at least once for alive.
        monitor_key_get.assert_any_call('admin',
                                        'mon_ip-192-168-1-2_0.94.1_alive')

        # Make sure we checked to see if the previous node was finished
        monitor_key_exists.assert_has_calls(
            [call('admin', 'mon_ip-192-168-1-2_0.94.1_done')]
        )

        # Make sure we waited at last once before proceeding
        log.assert_has_calls(
            [call('Previous node is: ip-192-168-1-2')],
            [call('ip-192-168-1-2 is not finished. Waiting')],
        )

        self.assertGreaterEqual(tval[0], previous_node_start_time + 600)
