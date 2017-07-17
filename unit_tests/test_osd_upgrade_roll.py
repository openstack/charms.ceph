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

import os
import sys
import time
import unittest

from mock import patch, call, mock_open

import ceph.utils

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
    @patch.object(ceph.utils, 'dirs_need_ownership_update')
    @patch.object(ceph.utils, 'apt_install')
    @patch.object(ceph.utils, 'chownr')
    @patch.object(ceph.utils, 'service_restart')
    @patch.object(ceph.utils, 'log')
    @patch.object(ceph.utils, 'status_set')
    @patch.object(ceph.utils, 'apt_update')
    @patch.object(ceph.utils, 'add_source')
    @patch.object(ceph.utils, 'get_local_osd_ids')
    @patch.object(ceph.utils, 'systemd')
    @patch.object(ceph.utils, 'get_version')
    @patch.object(ceph.utils, 'config')
    def test_upgrade_osd_hammer(self, config, get_version, systemd, local_osds,
                                add_source, apt_update, status_set, log,
                                service_restart, chownr, apt_install,
                                dirs_need_ownership_update):
        config.side_effect = config_side_effect
        get_version.side_effect = [0.80, 0.94]
        systemd.return_value = False
        local_osds.return_value = [0, 1, 2]
        dirs_need_ownership_update.return_value = False

        ceph.utils.upgrade_osd('hammer')
        service_restart.assert_called_with('ceph-osd-all')
        status_set.assert_has_calls([
            call('maintenance', 'Upgrading osd'),
        ])
        log.assert_has_calls(
            [
                call('Current ceph version is 0.8'),
                call('Upgrading to: hammer')
            ]
        )
        # Make sure on an Upgrade to Hammer that chownr was NOT called.
        assert not chownr.called

    @patch.object(ceph.utils, '_upgrade_single_osd')
    @patch.object(ceph.utils, 'update_owner')
    @patch('os.listdir')
    @patch.object(ceph.utils, '_get_child_dirs')
    @patch.object(ceph.utils, 'dirs_need_ownership_update')
    @patch.object(ceph.utils, 'apt_install')
    @patch.object(ceph.utils, 'log')
    @patch.object(ceph.utils, 'status_set')
    @patch.object(ceph.utils, 'apt_update')
    @patch.object(ceph.utils, 'add_source')
    @patch.object(ceph.utils, 'get_local_osd_ids')
    @patch.object(ceph.utils, 'systemd')
    @patch.object(ceph.utils, 'get_version')
    @patch.object(ceph.utils, 'config')
    def test_upgrade_osd_jewel(self, config, get_version, systemd,
                               local_osds, add_source, apt_update, status_set,
                               log, apt_install, dirs_need_ownership_update,
                               _get_child_dirs, listdir, update_owner,
                               _upgrade_single_osd):
        config.side_effect = config_side_effect
        get_version.side_effect = [0.94, 10.1]
        systemd.return_value = False
        local_osds.return_value = [0, 1, 2]
        listdir.return_value = ['osd', 'mon', 'fs']
        _get_child_dirs.return_value = ['ceph-0', 'ceph-1', 'ceph-2']
        dirs_need_ownership_update.return_value = True

        ceph.utils.upgrade_osd('jewel')
        update_owner.assert_has_calls([
            call(ceph.utils.CEPH_BASE_DIR, recurse_dirs=False),
            call(os.path.join(ceph.utils.CEPH_BASE_DIR, 'mon')),
            call(os.path.join(ceph.utils.CEPH_BASE_DIR, 'fs')),
        ])
        _upgrade_single_osd.assert_has_calls([
            call('0', 'ceph-0'),
            call('1', 'ceph-1'),
            call('2', 'ceph-2'),
        ])
        status_set.assert_has_calls([
            call('maintenance', 'Upgrading osd'),
            call('maintenance', 'Upgrading packages to jewel')
        ])
        log.assert_has_calls(
            [
                call('Current ceph version is 0.94'),
                call('Upgrading to: jewel')
            ]
        )

    @patch.object(ceph.utils, 'stop_osd')
    @patch.object(ceph.utils, 'disable_osd')
    @patch.object(ceph.utils, 'update_owner')
    @patch.object(ceph.utils, 'enable_osd')
    @patch.object(ceph.utils, 'start_osd')
    def test_upgrade_single_osd(self, start_osd, enable_osd, update_owner,
                                disable_osd, stop_osd):
        ceph.utils._upgrade_single_osd(1, '/var/lib/ceph/osd/ceph-1')
        stop_osd.assert_called_with(1)
        disable_osd.assert_called_with(1)
        update_owner.assert_called_with('/var/lib/ceph/osd/ceph-1')
        enable_osd.assert_called_with(1)
        start_osd.assert_called_with(1)

    @patch.object(ceph.utils, 'systemd')
    @patch.object(ceph.utils, 'service_stop')
    def test_stop_osd(self, service_stop, systemd):
        systemd.return_value = False
        ceph.utils.stop_osd(1)
        service_stop.assert_called_with('ceph-osd', id=1)

        systemd.return_value = True
        ceph.utils.stop_osd(2)
        service_stop.assert_called_with('ceph-osd@2')

    @patch.object(ceph.utils, 'systemd')
    @patch.object(ceph.utils, 'service_start')
    def test_start_osd(self, service_start, systemd):
        systemd.return_value = False
        ceph.utils.start_osd(1)
        service_start.assert_called_with('ceph-osd', id=1)

        systemd.return_value = True
        ceph.utils.start_osd(2)
        service_start.assert_called_with('ceph-osd@2')

    @patch('subprocess.check_call')
    @patch('os.path.exists')
    @patch('os.unlink')
    @patch.object(ceph.utils, 'systemd')
    def test_disable_osd(self, systemd, unlink, exists, check_call):
        systemd.return_value = True
        ceph.utils.disable_osd(4)
        check_call.assert_called_with(['systemctl', 'disable', 'ceph-osd@4'])

        exists.return_value = True
        systemd.return_value = False
        ceph.utils.disable_osd(3)
        unlink.assert_called_with('/var/lib/ceph/osd/ceph-3/ready')

    @patch('subprocess.check_call')
    @patch.object(ceph.utils, 'update_owner')
    @patch.object(ceph.utils, 'systemd')
    def test_enable_osd(self, systemd, update_owner, check_call):
        systemd.return_value = True
        ceph.utils.enable_osd(5)
        check_call.assert_called_with(['systemctl', 'enable', 'ceph-osd@5'])

        systemd.return_value = False
        mo = mock_open()
        # Detect which builtin open version we need to mock based on
        # the python version.
        bs = 'builtins' if sys.version_info > (3, 0) else '__builtin__'
        with patch('%s.open' % bs, mo):
            ceph.utils.enable_osd(6)
        mo.assert_called_once_with('/var/lib/ceph/osd/ceph-6/ready', 'w')
        handle = mo()
        handle.write.assert_called_with('ready')
        update_owner.assert_called_with('/var/lib/ceph/osd/ceph-6/ready')

    @patch.object(ceph.utils, 'socket')
    @patch.object(ceph.utils, 'get_osd_tree')
    @patch.object(ceph.utils, 'log')
    @patch.object(ceph.utils, 'lock_and_roll')
    @patch.object(ceph.utils, 'get_upgrade_position')
    def test_roll_osd_cluster_first(self,
                                    get_upgrade_position,
                                    lock_and_roll,
                                    log,
                                    get_osd_tree,
                                    socket):
        socket.gethostname.return_value = "ip-192-168-1-2"
        get_osd_tree.return_value = ""
        get_upgrade_position.return_value = 0

        ceph.utils.roll_osd_cluster(new_version='0.94.1',
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

    @patch.object(ceph.utils, 'get_osd_tree')
    @patch.object(ceph.utils, 'socket')
    @patch.object(ceph.utils, 'status_set')
    @patch.object(ceph.utils, 'lock_and_roll')
    @patch.object(ceph.utils, 'get_upgrade_position')
    @patch.object(ceph.utils, 'wait_on_previous_node')
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
            ceph.utils.CrushLocation(
                name="ip-192-168-1-2",
                identifier='a',
                host='host-a',
                rack='rack-a',
                row='row-a',
                datacenter='dc-1',
                chassis='chassis-a',
                root='ceph'),
            ceph.utils.CrushLocation(
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

        ceph.utils.roll_osd_cluster(new_version='0.94.1',
                                    upgrade_key='osd-upgrade')
        status_set.assert_called_with(
            'blocked',
            'Waiting on ip-192-168-1-2 to finish upgrading')
        lock_and_roll.assert_called_with(my_name='ip-192-168-1-3',
                                         service='osd',
                                         upgrade_key='osd-upgrade',
                                         version='0.94.1')

    @patch('os.path.exists')
    @patch('os.listdir')
    @patch('os.path.isdir')
    def test__get_child_dirs(self, isdir, listdir, exists):
        isdir.side_effect = [True, True, True, False, True]
        listdir.return_value = ['mon', 'bootstrap-osd', 'foo', 'bootstrap-mon']
        exists.return_value = True

        child_dirs = ceph.utils._get_child_dirs('/var/lib/ceph')
        isdir.assert_has_calls([call('/var/lib/ceph'),
                                call('/var/lib/ceph/mon'),
                                call('/var/lib/ceph/bootstrap-osd'),
                                call('/var/lib/ceph/foo'),
                                call('/var/lib/ceph/bootstrap-mon')])
        self.assertListEqual(['/var/lib/ceph/mon',
                              '/var/lib/ceph/bootstrap-osd',
                              '/var/lib/ceph/bootstrap-mon'], child_dirs)

    @patch('os.path.exists')
    @patch('os.path.isdir')
    def test__get_child_dirs_not_dir(self, isdir, exists):
        isdir.return_value = False
        exists.return_value = False

        with self.assertRaises(ValueError):
            ceph.utils._get_child_dirs('/var/lib/ceph')

    @patch('os.path.exists')
    def test__get_child_dirs_no_exist(self, exists):
        exists.return_value = False

        with self.assertRaises(ValueError):
            ceph.utils._get_child_dirs('/var/lib/ceph')

    @patch.object(ceph.utils, 'ceph_user')
    @patch('os.path.isdir')
    @patch('subprocess.check_call')
    @patch.object(ceph.utils, 'status_set')
    def test_update_owner_no_recurse(self, status_set, check_call,
                                     isdir, ceph_user):
        ceph_user.return_value = 'ceph'
        isdir.return_value = True
        ceph.utils.update_owner('/var/lib/ceph', False)
        check_call.assert_called_with(['chown', 'ceph:ceph', '/var/lib/ceph'])

    @patch.object(ceph.utils, 'ceph_user')
    @patch('os.path.isdir')
    @patch('subprocess.check_call')
    @patch.object(ceph.utils, 'status_set')
    def test_update_owner_recurse_file(self, status_set, check_call,
                                       isdir, ceph_user):
        ceph_user.return_value = 'ceph'
        isdir.return_value = False
        ceph.utils.update_owner('/var/lib/ceph', True)
        check_call.assert_called_with(['chown', 'ceph:ceph', '/var/lib/ceph'])

    @patch.object(ceph.utils, 'ceph_user')
    @patch('os.path.isdir')
    @patch('subprocess.check_call')
    @patch.object(ceph.utils, 'status_set')
    def test_update_owner_recurse(self, status_set, check_call,
                                  isdir, ceph_user):
        ceph_user.return_value = 'ceph'
        isdir.return_value = True
        ceph.utils.update_owner('/var/lib/ceph', True)
        check_call.assert_called_with(['chown', '-R', 'ceph:ceph',
                                       '/var/lib/ceph'])

"""
    @patch.object(ceph.utils, 'log')
    @patch('time.time', lambda *args: previous_node_start_time + 10 * 60 + 1)
    @patch.object(ceph.utils, 'monitor_key_get')
    @patch.object(ceph.utils, 'monitor_key_exists')
    def test_wait_on_previous_node(self,
                                   monitor_key_exists,
                                   monitor_key_get,
                                   log):
        monitor_key_get.side_effect = monitor_key_side_effect
        monitor_key_exists.return_value = False

        ceph.utils.wait_on_previous_node(previous_node="ip-192-168-1-2",
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
