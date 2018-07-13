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

from mock import (
    call,
    mock_open,
    MagicMock,
    patch,
)

import ceph.utils as utils

from subprocess import CalledProcessError


class TestDevice():
    """Test class to mock out pyudev Device"""

    def __getitem__(**kwargs):
        """
        Mock [].

        We need this method to be present in the test class mock even
        though we mock the return value with the MagicMock later
        """
        return "Some device type"

    def device_node():
        "/dev/test_device"


class CephTestCase(unittest.TestCase):
    def setUp(self):
        super(CephTestCase, self).setUp()

    @patch.object(utils, 'cmp_pkgrevno')
    @patch.object(utils.subprocess, 'call')
    @patch.object(utils.os.path, 'exists')
    @patch.object(utils.os.path, 'isdir')
    def test_start_osd(self,
                       _isdir,
                       _exists,
                       _call,
                       _pkgrevno):
        _pkgrevno.return_value = True
        _isdir.return_value = False
        utils.start_osds(['/dev/sdb'])
        _isdir.assert_called_once_with('/dev/sdb')
        _exists.assert_called_once_with('/dev/sdb')
        _call.assert_has_calls([
            call(['udevadm', 'trigger',
                  '--subsystem-match=block', '--action=add'
                  ]),
            call(['udevadm', 'settle']),
        ])

    @patch.object(utils, 'kv')
    @patch.object(utils.subprocess, 'check_call')
    @patch.object(utils, '_ceph_disk')
    @patch.object(utils, 'is_mapped_luks_device')
    @patch.object(utils, 'is_active_bluestore_device')
    @patch.object(utils.os.path, 'exists')
    @patch.object(utils, 'is_device_mounted')
    @patch.object(utils, 'cmp_pkgrevno')
    @patch.object(utils, 'is_block_device')
    def test_osdize_dev_ceph_disk(self, _is_blk, _cmp, _mounted, _exists,
                                  _is_active_bluestore_device,
                                  _is_mapped_luks_device, _ceph_disk,
                                  _check_call, _kv):
        """Test that _ceph_disk is called for < Luminous 12.2.4"""
        db = MagicMock()
        _kv.return_value = db
        db.get.return_value = []
        _is_blk.return_value = True
        _mounted.return_value = False
        _exists.return_value = True
        _cmp.return_value = -1
        _ceph_disk.return_value = ['ceph-disk', 'prepare']
        _is_mapped_luks_device.return_value = False
        _is_active_bluestore_device.return_value = False
        utils.osdize('/dev/sdb', osd_format='xfs', osd_journal=None,
                     bluestore=False)
        _ceph_disk.assert_called_with('/dev/sdb', 'xfs', None, False, False)
        _check_call.assert_called_with(['ceph-disk', 'prepare'])
        db.get.assert_called_with('osd-devices', [])
        db.set.assert_called_with('osd-devices', ['/dev/sdb'])
        db.flush.assert_called_once()

    @patch.object(utils, 'kv')
    @patch.object(utils.subprocess, 'check_call')
    @patch.object(utils, '_ceph_volume')
    @patch.object(utils, 'is_mapped_luks_device')
    @patch.object(utils, 'is_active_bluestore_device')
    @patch.object(utils.os.path, 'exists')
    @patch.object(utils, 'is_device_mounted')
    @patch.object(utils, 'cmp_pkgrevno')
    @patch.object(utils, 'is_block_device')
    def test_osdize_dev_ceph_volume(self, _is_blk, _cmp, _mounted, _exists,
                                    _is_mapped_luks_device,
                                    _is_active_bluestore_device, _ceph_volume,
                                    _check_call, _kv):
        """Test that _ceph_volume is called for >= Luminous 12.2.4"""
        db = MagicMock()
        _kv.return_value = db
        db.get.return_value = []
        _is_blk.return_value = True
        _mounted.return_value = False
        _exists.return_value = True
        _cmp.return_value = 1
        _ceph_volume.return_value = ['ceph-volume', 'prepare']
        _is_mapped_luks_device.return_value = False
        _is_active_bluestore_device.return_value = False
        utils.osdize('/dev/sdb', osd_format='xfs', osd_journal=None,
                     bluestore=False)
        _ceph_volume.assert_called_with('/dev/sdb', None, False, False, 'ceph')
        _check_call.assert_called_with(['ceph-volume', 'prepare'])
        db.get.assert_called_with('osd-devices', [])
        db.set.assert_called_with('osd-devices', ['/dev/sdb'])
        db.flush.assert_called_once()

    @patch.object(utils, 'kv')
    def test_osdize_dev_already_processed(self, _kv):
        """Ensure that previously processed disks are skipped"""
        db = MagicMock()
        _kv.return_value = db
        db.get.return_value = ['/dev/sdb']
        utils.osdize('/dev/sdb', osd_format='xfs', osd_journal=None,
                     bluestore=False)
        db.get.assert_called_with('osd-devices', [])
        db.set.assert_not_called()

    @patch.object(utils, 'kv')
    @patch.object(utils.subprocess, 'check_call')
    @patch.object(utils, '_ceph_volume')
    @patch.object(utils, 'is_mapped_luks_device')
    @patch.object(utils, 'is_active_bluestore_device')
    @patch.object(utils.os.path, 'exists')
    @patch.object(utils, 'is_device_mounted')
    @patch.object(utils, 'cmp_pkgrevno')
    @patch.object(utils, 'is_block_device')
    def test_osdize_already_processed_luks_bluestore(
            self, _is_blk, _cmp, _mounted,
            _exists,
            _is_active_bluestore_device, _is_mapped_luks_device,
            _ceph_volume, _check_call, _kv):
        """Test that _ceph_volume is called for >= Luminous 12.2.4"""
        db = MagicMock()
        _kv.return_value = db
        db.get.return_value = []
        _is_blk.return_value = True
        _mounted.return_value = False
        _exists.return_value = True
        _cmp.return_value = 1
        _ceph_volume.return_value = ['ceph-volume', 'prepare']
        _is_active_bluestore_device.return_value = False
        _is_mapped_luks_device.return_value = True
        utils.osdize('/dev/sdb', encrypt=True, osd_format=None,
                     osd_journal=None, bluestore=True, key_manager='vault')
        db.get.assert_called_with('osd-devices', [])
        db.set.assert_not_called()

    @patch.object(utils.subprocess, 'check_call')
    @patch.object(utils.os.path, 'exists')
    @patch.object(utils, 'is_device_mounted')
    @patch.object(utils, 'cmp_pkgrevno')
    @patch.object(utils, 'mkdir')
    @patch.object(utils, 'chownr')
    @patch.object(utils, 'ceph_user')
    def test_osdize_dir(self, _ceph_user, _chown, _mkdir,
                        _cmp, _mounted, _exists, _call):
        """Test that the dev osd is initialized correctly"""
        _ceph_user.return_value = "ceph"
        _mounted.return_value = False
        _exists.return_value = False
        _cmp.return_value = True
        utils.osdize('/srv/osd', osd_format='xfs', osd_journal=None,
                     bluestore=False)
        _call.assert_called_with(['sudo', '-u', 'ceph', 'ceph-disk', 'prepare',
                                  '--data-dir', '/srv/osd', '--filestore'])

    @patch.object(utils.subprocess, 'check_output')
    def test_get_osd_weight(self, output):
        """It gives an OSD's weight"""
        output.return_value = b"""{
    "nodes": [{
        "id": -1,
        "name": "default",
        "type": "root",
        "type_id": 10,
        "children": [-4, -3, -2]
    }, {
        "id": -2,
        "name": "ip-172-31-11-147",
        "type": "host",
        "type_id": 1,
        "children": [0]
    }, {
        "id": 0,
        "name": "osd.0",
        "type": "osd",
        "type_id": 0,
        "crush_weight": 0.002899,
        "depth": 2,
        "exists": 1,
        "status": "up",
        "reweight": 1.000000,
        "primary_affinity": 1.000000
    }, {
        "id": -3,
        "name": "ip-172-31-56-198",
        "type": "host",
        "type_id": 1,
        "children": [2]
    }, {
        "id": 2,
        "name": "osd.2",
        "type": "osd",
        "type_id": 0,
        "crush_weight": 0.002899,
        "depth": 2,
        "exists": 1,
        "status": "up",
        "reweight": 1.000000,
        "primary_affinity": 1.000000
    }, {
        "id": -4,
        "name": "ip-172-31-24-103",
        "type": "host",
        "type_id": 1,
        "children": [1]
    }, {
        "id": 1,
        "name": "osd.1",
        "type": "osd",
        "type_id": 0,
        "crush_weight": 0.002899,
        "depth": 2,
        "exists": 1,
        "status": "up",
        "reweight": 1.000000,
        "primary_affinity": 1.000000
    }],
    "stray": []
}"""
        weight = utils.get_osd_weight('osd.0')
        self.assertEqual(weight, 0.002899)

    @patch.object(utils.subprocess, 'check_output')
    @patch.object(utils, "ceph_user", lambda: "ceph")
    @patch.object(utils.socket, "gethostname", lambda: "osd001")
    def test_get_named_key_with_pool(self, mock_check_output):
        mock_check_output.side_effect = [CalledProcessError(0, 0, 0), b""]
        utils.get_named_key(name="rgw001", pool_list=["rbd", "block"])
        mock_check_output.assert_has_calls([
            call(['sudo', '-u', 'ceph', 'ceph', '--name',
                  'mon.', '--keyring',
                  '/var/lib/ceph/mon/ceph-osd001/keyring',
                  'auth', 'get', 'client.rgw001']),
            call(['sudo', '-u', 'ceph', 'ceph', '--name',
                  'mon.', '--keyring',
                  '/var/lib/ceph/mon/ceph-osd001/keyring',
                  'auth', 'get-or-create', 'client.rgw001',
                  'mon', 'allow r; allow command "osd blacklist"',
                  'osd', 'allow rwx pool=rbd pool=block'])])

    @patch.object(utils.subprocess, 'check_output')
    @patch.object(utils, 'ceph_user', lambda: "ceph")
    @patch.object(utils.socket, "gethostname", lambda: "osd001")
    def test_get_named_key(self, mock_check_output):
        mock_check_output.side_effect = [CalledProcessError(0, 0, 0), b""]
        utils.get_named_key(name="rgw001")
        mock_check_output.assert_has_calls([
            call(['sudo', '-u', 'ceph', 'ceph', '--name',
                  'mon.', '--keyring',
                  '/var/lib/ceph/mon/ceph-osd001/keyring',
                  'auth', 'get', 'client.rgw001']),
            call(['sudo', '-u', 'ceph', 'ceph', '--name',
                  'mon.', '--keyring',
                  '/var/lib/ceph/mon/ceph-osd001/keyring',
                  'auth', 'get-or-create', 'client.rgw001',
                  'mon', 'allow r; allow command "osd blacklist"',
                  'osd', 'allow rwx'])])

    def test_parse_key_with_caps_existing_key(self):
        expected = "AQCm7aVYQFXXFhAAj0WIeqcag88DKOvY4UKR/g=="
        with_caps = "[client.osd-upgrade]\n" \
                    "	key = AQCm7aVYQFXXFhAAj0WIeqcag88DKOvY4UKR/g==\n" \
                    "	caps mon = \"allow command \"config-key\";"
        key = utils.parse_key(with_caps)
        self.assertEqual(key, expected)

    def test_parse_key_without_caps(self):
        expected = "AQCm7aVYQFXXFhAAj0WIeqcag88DKOvY4UKR/g=="
        without_caps = "[client.osd-upgrade]\n" \
                       "	key = AQCm7aVYQFXXFhAAj0WIeqcag88DKOvY4UKR/g=="
        key = utils.parse_key(without_caps)
        self.assertEqual(key, expected)

    def test_list_unmounted_devices(self):
        dev1 = MagicMock(spec=TestDevice)
        dev1.__getitem__.return_value = "block"
        dev1.device_node = '/dev/sda'
        dev2 = MagicMock(spec=TestDevice)
        dev2.__getitem__.return_value = "block"
        dev2.device_node = '/dev/sdb'
        dev3 = MagicMock(spec=TestDevice)
        dev3.__getitem__.return_value = "block"
        dev3.device_node = '/dev/loop1'
        devices = [dev1, dev2, dev3]
        with patch(
                'pyudev.Context.list_devices',
                return_value=devices):
            with patch.object(utils, 'is_device_mounted',
                              return_value=False):
                devices = utils.unmounted_disks()
                self.assertEqual(devices, ['/dev/sda', '/dev/sdb'])
            with patch.object(utils, 'is_device_mounted',
                              return_value=True):
                devices = utils.unmounted_disks()
                self.assertEqual(devices, [])

    @patch.object(utils.subprocess, 'check_output')
    def test_get_partition_list(self, output):
        with open('unit_tests/partx_output', 'r') as partx_out:
            output.return_value = partx_out.read().encode('UTF-8')
        partition_list = utils.get_partition_list('/dev/xvdb')
        self.assertEqual(len(partition_list), 4)

    @patch.object(utils.subprocess, 'check_output')
    def test_get_ceph_pg_stat(self, output):
        """It returns the current PG stat"""
        output.return_value = b"""{
  "num_pg_by_state": [
    {
      "name": "active+clean",
      "num": 320
    }
  ],
  "version": 7111,
  "num_pgs": 320,
  "num_bytes": 118111608230,
  "raw_bytes_used": 355042729984,
  "raw_bytes_avail": 26627104956416,
  "raw_bytes": 26982147686400
}"""
        pg_stat = utils.get_ceph_pg_stat()
        self.assertEqual(pg_stat['num_pgs'], 320)

    @patch.object(utils.subprocess, 'check_output')
    def test_get_ceph_health(self, output):
        """It gives the current Ceph health"""
        output.return_value = b"""{
  "health": {
    "health_services": [
      {
        "mons": [
          {
            "name": "node1",
            "kb_total": 2883598592,
            "kb_used": 61728860,
            "kb_avail": 2675368308,
            "avail_percent": 92,
            "last_updated": "2017-04-25 22:17:36.966046",
            "store_stats": {
              "bytes_total": 18612017,
              "bytes_sst": 0,
              "bytes_log": 2172670,
              "bytes_misc": 16439347,
              "last_updated": "0.000000"
            },
            "health": "HEALTH_OK"
          },
          {
            "name": "node2",
            "kb_total": 2883598592,
            "kb_used": 79776472,
            "kb_avail": 2657320696,
            "avail_percent": 92,
            "last_updated": "2017-04-25 22:18:27.915641",
            "store_stats": {
              "bytes_total": 18517923,
              "bytes_sst": 0,
              "bytes_log": 3340129,
              "bytes_misc": 15177794,
              "last_updated": "0.000000"
            },
            "health": "HEALTH_OK"
          },
          {
            "name": "node3",
            "kb_total": 2883598592,
            "kb_used": 77399744,
            "kb_avail": 2659697424,
            "avail_percent": 92,
            "last_updated": "2017-04-25 22:18:27.934053",
            "store_stats": {
              "bytes_total": 18517892,
              "bytes_sst": 0,
              "bytes_log": 3340129,
              "bytes_misc": 15177763,
              "last_updated": "0.000000"
            },
            "health": "HEALTH_OK"
          }
        ]
      }
    ]
  },
  "timechecks": {
    "epoch": 8,
    "round": 3022,
    "round_status": "finished",
    "mons": [
      {
        "name": "node1",
        "skew": 0,
        "latency": 0,
        "health": "HEALTH_OK"
      },
      {
        "name": "node2",
        "skew": 0,
        "latency": 0.000765,
        "health": "HEALTH_OK"
      },
      {
        "name": "node3",
        "skew": 0,
        "latency": 0.000765,
        "health": "HEALTH_OK"
      }
    ]
  },
  "summary": [],
  "overall_status": "HEALTH_OK",
  "detail": []
}"""
        health = utils.get_ceph_health()
        self.assertEqual(health['overall_status'], "HEALTH_OK")

    @patch.object(utils.subprocess, 'check_output')
    def test_reweight_osd(self, mock_reweight):
        """It changes the weight of an OSD"""
        mock_reweight.return_value = b"reweighted item id 0 name 'osd.0' to 1"
        reweight_result = utils.reweight_osd('0', '1')
        self.assertEqual(reweight_result, True)
        mock_reweight.assert_called_once_with(
            ['ceph', 'osd', 'crush', 'reweight', 'osd.0', '1'], stderr=-2)

    @patch.object(utils, 'is_container')
    def test_determine_packages(self, mock_is_container):
        mock_is_container.return_value = False
        self.assertTrue('ntp' in utils.determine_packages())
        self.assertEqual(utils.PACKAGES,
                         utils.determine_packages())

        mock_is_container.return_value = True
        self.assertFalse('ntp' in utils.determine_packages())

    @patch.object(utils, 'chownr')
    @patch.object(utils, 'cmp_pkgrevno')
    @patch.object(utils, 'ceph_user')
    @patch.object(utils, 'os')
    @patch.object(utils, 'systemd')
    @patch.object(utils, 'log')
    @patch.object(utils, 'mkdir')
    @patch.object(utils.subprocess, 'check_output')
    @patch.object(utils.subprocess, 'check_call')
    @patch.object(utils, 'service_restart')
    @patch.object(utils.socket, 'gethostname', lambda: 'TestHost')
    def _test_bootstrap_monitor_cluster(self,
                                        mock_service_restart,
                                        mock_check_call,
                                        mock_check_output,
                                        mock_mkdir,
                                        mock_log,
                                        mock_systemd,
                                        mock_os,
                                        mock_ceph_user,
                                        mock_cmp_pkgrevno,
                                        mock_chownr,
                                        luminous=False):
        test_hostname = utils.socket.gethostname()
        test_secret = 'mysecret'
        test_keyring = '/var/lib/ceph/tmp/{}.mon.keyring'.format(test_hostname)
        test_path = '/var/lib/ceph/mon/ceph-{}'.format(test_hostname)
        test_done = '{}/done'.format(test_path)
        test_init_marker = '{}/systemd'.format(test_path)

        mock_os.path.exists.return_value = False
        mock_systemd.return_value = True
        mock_cmp_pkgrevno.return_value = 1 if luminous else -1
        mock_ceph_user.return_value = 'ceph'

        test_calls = [
            call(
                ['ceph-authtool', test_keyring,
                 '--create-keyring', '--name=mon.',
                 '--add-key={}'.format(test_secret),
                 '--cap', 'mon', 'allow *']
            ),
            call(
                ['ceph-mon', '--mkfs',
                 '-i', test_hostname,
                 '--keyring', test_keyring]
            ),
            call(['systemctl', 'enable', 'ceph-mon']),
        ]
        if luminous:
            test_calls.append(
                call(['ceph-create-keys', '--id', test_hostname, '--timeout',
                      '1800'])
            )

        fake_open = mock_open()
        with patch('ceph.utils.open', fake_open, create=True):
            utils.bootstrap_monitor_cluster(test_secret)

        mock_check_call.assert_has_calls(test_calls)
        mock_service_restart.assert_called_with('ceph-mon')
        mock_mkdir.assert_has_calls([
            call('/var/run/ceph', owner='ceph',
                 group='ceph', perms=0o755),
            call(test_path, owner='ceph', group='ceph',
                 perms=0o755),
        ])
        fake_open.assert_has_calls([call(test_done, 'w'),
                                    call(test_init_marker, 'w')],
                                   any_order=True)
        mock_os.unlink.assert_called_with(test_keyring)

    def test_bootstrap_monitor_cluster(self):
        self._test_bootstrap_monitor_cluster(luminous=False)

    def test_bootstrap_monitor_cluster_luminous(self):
        self._test_bootstrap_monitor_cluster(luminous=True)

    @patch.object(utils, 'chownr')
    @patch.object(utils, 'cmp_pkgrevno')
    @patch.object(utils, 'ceph_user')
    @patch.object(utils, 'os')
    @patch.object(utils, 'log')
    @patch.object(utils, 'mkdir')
    @patch.object(utils, 'subprocess')
    @patch.object(utils, 'service_restart')
    def test_bootstrap_manager(self,
                               mock_service_restart,
                               mock_subprocess,
                               mock_mkdir,
                               mock_log,
                               mock_os,
                               mock_ceph_user,
                               mock_cmp_pkgrevno,
                               mock_chownr):
        test_hostname = utils.socket.gethostname()
        test_path = '/var/lib/ceph/mgr/ceph-{}'.format(test_hostname)
        test_keyring = '/var/lib/ceph/mgr/ceph-{}/keyring'.format(
            test_hostname)
        test_unit = 'ceph-mgr@{}'.format(test_hostname)

        mock_os.path.exists.return_value = False
        mock_os.path.join.return_value = test_keyring
        mock_ceph_user.return_value = 'ceph'

        test_calls = [
            call(
                ['ceph', 'auth', 'get-or-create',
                 'mgr.{}'.format(test_hostname), 'mon',
                 'allow profile mgr', 'osd', 'allow *',
                 'mds', 'allow *', '--out-file',
                 test_keyring]
            ),
            call(['systemctl', 'enable', test_unit]),
        ]

        fake_open = mock_open()
        with patch('ceph.open', fake_open, create=True):
            utils.bootstrap_manager()

        self.assertEqual(
            mock_subprocess.check_call.mock_calls,
            test_calls
        )
        mock_service_restart.assert_called_with(test_unit)
        mock_mkdir.assert_has_calls([
            call(test_path, owner='ceph', group='ceph'),
        ])

    @patch.object(utils.subprocess, 'check_call')
    def test_osd_set_noout(self, mock_check_call):
        """It changes the setting of ceph osd noout"""
        utils.osd_noout(True)
        mock_check_call.assert_called_once_with(
            ['ceph', '--id', 'admin', 'osd', 'set', 'noout'])

    @patch.object(utils.subprocess, 'check_call')
    def test_osd_unset_noout(self, mock_check_call):
        utils.osd_noout(False)
        mock_check_call.assert_called_once_with(
            ['ceph', '--id', 'admin', 'osd', 'unset', 'noout'])

    @patch.object(utils.subprocess, 'check_call')
    def test_osd_set_noout_fail(self, mock_check_call):
        mock_check_call.side_effect = CalledProcessError
        with self.assertRaises(Exception):
            utils.osd_noout(True)

    def test_pretty_print_upgrade_paths(self):
        expected = ([
            'firefly -> hammer',
            'hammer -> jewel',
            'jewel -> luminous',
        ])
        self.assertEqual(utils.pretty_print_upgrade_paths(), expected)

    @patch.object(utils.subprocess, 'check_output')
    def test_get_conf(self, _check_output):
        _check_output.return_value = '12345\n'
        self.assertEqual(utils.get_conf('bluestore_block_db_size'),
                         '12345')
        _check_output.assert_called_with([
            'ceph-osd',
            '--show-config-value=bluestore_block_db_size',
        ])

    def test_partition_name(self):
        self.assertEqual(utils._partition_name('/dev/sdb'),
                         '/dev/sdb1')
        self.assertEqual(utils._partition_name('/dev/mmcblk0'),
                         '/dev/mmcblk0p1')


class CephVolumeSizeCalculatorTestCase(unittest.TestCase):

    @patch.object(utils, 'get_conf')
    def test_calculate_volume_size_journal(self, _get_conf):
        _get_conf.return_value = 0
        self.assertEqual(utils.calculate_volume_size('journal'),
                         1024)
        _get_conf.assert_called_with('osd_journal_size')

        _get_conf.return_value = 2048
        self.assertEqual(utils.calculate_volume_size('journal'),
                         2048)

    @patch.object(utils, 'get_conf')
    def test_calculate_volume_size_db(self, _get_conf):
        _get_conf.return_value = 0
        self.assertEqual(utils.calculate_volume_size('db'),
                         1024)
        _get_conf.assert_called_with('bluestore_block_db_size')

        _get_conf.return_value = 2048 * 1048576
        self.assertEqual(utils.calculate_volume_size('db'),
                         2048)

    @patch.object(utils, 'get_conf')
    def test_calculate_volume_size_wal(self, _get_conf):
        _get_conf.return_value = 0
        self.assertEqual(utils.calculate_volume_size('wal'),
                         576)
        _get_conf.assert_called_with('bluestore_block_wal_size')

        _get_conf.return_value = 512 * 1048576
        self.assertEqual(utils.calculate_volume_size('wal'),
                         512)

    def test_calculate_volume_size_invalid(self):
        with self.assertRaises(KeyError):
            utils.calculate_volume_size('invalid')


class CephInitializeDiskTestCase(unittest.TestCase):

    @patch.object(utils, '_luks_uuid')
    @patch.object(utils.subprocess, 'check_call')
    @patch.object(utils.os.path, 'exists')
    def test_initialize_disk(self, _exists, _check_call,
                             _luks_uuid):
        _exists.return_value = False
        _luks_uuid.return_value = None
        self.assertEqual(utils._initialize_disk('/dev/sdb',
                                                'test-UUID'),
                         '/dev/sdb')
        _check_call.assert_not_called()

    @patch.object(utils, '_luks_uuid')
    @patch.object(utils.subprocess, 'check_call')
    @patch.object(utils.os.path, 'exists')
    def test_initialize_disk_vaultlocker(self, _exists, _check_call,
                                         _luks_uuid):
        _exists.return_value = False
        _luks_uuid.return_value = None
        self.assertEqual(utils._initialize_disk('/dev/sdb',
                                                'test-UUID',
                                                True,
                                                'vault'),
                         '/dev/mapper/crypt-test-UUID')
        _check_call.assert_called_once_with(
            ['vaultlocker', 'encrypt',
             '--uuid', 'test-UUID',
             '/dev/sdb']
        )

    @patch.object(utils, '_luks_uuid')
    @patch.object(utils.subprocess, 'check_call')
    @patch.object(utils.os.path, 'exists')
    def test_initialize_disk_vaultlocker_encrypted(self,
                                                   _exists,
                                                   _check_call,
                                                   _luks_uuid):
        _exists.return_value = True
        _luks_uuid.return_value = 'existing-UUID'
        self.assertEqual(utils._initialize_disk('/dev/sdb',
                                                'test-UUID',
                                                True,
                                                'vault'),
                         '/dev/mapper/crypt-existing-UUID')
        _check_call.assert_not_called()


class CephActiveBlueStoreDeviceTestCase(unittest.TestCase):

    _test_pvs = {
        '/dev/sdb': 'ceph-1234',
        '/dev/sdc': 'ceph-5678',
        '/dev/sde': 'ceph-9101',
    }

    _test_vgs = {
        'ceph-1234': ['osd-block-1234'],
        'ceph-5678': ['osd-block-5678'],
        'ceph-9101': ['osd-block-9101'],
    }

    _test_links = {
        '/var/lib/ceph/osd/ceph-4/block': '/dev/ceph-1234/osd-block-1234',
        '/var/lib/ceph/osd/ceph-6/block': '/dev/ceph-5678/osd-block-5678',
    }

    @patch.object(utils, 'glob')
    @patch.object(utils, 'os')
    @patch.object(utils, 'lvm')
    def _test_active_bluestore_device(self, _lvm, _os, _glob, device, active):
        _os.path.is_link.return_value = True
        _glob.glob.return_value = self._test_links.keys()
        _lvm.is_lvm_physical_volume.side_effect = (
            lambda dev: dev in self._test_pvs
        )
        _lvm.list_lvm_volume_group.side_effect = (
            lambda pv: self._test_pvs.get(pv)
        )
        _lvm.list_logical_volumes.side_effect = (
            lambda vg: self._test_vgs.get(vg.lstrip('vg_name='), [])
        )
        _os.readlink.side_effect = (
            lambda link: self._test_links.get(link)
        )

        self.assertEqual(utils.is_active_bluestore_device(device),
                         active)
        _lvm.is_lvm_physical_volume.assert_called_with(device)
        if device in self._test_pvs:
            _lvm.list_lvm_volume_group.assert_called_with(device)
            _lvm.list_logical_volumes.assert_called_with(
                'vg_name={}'.format(self._test_pvs.get(device))
            )
            _glob.glob.assert_called_with('/var/lib/ceph/osd/ceph-*/block')
        else:
            _lvm.list_lvm_volume_group.assert_not_called()
            _lvm.list_logical_volumes.assert_not_called()
            _glob.glob.assert_not_called()

    def test_active_bluestore_device_active(self):
        self._test_active_bluestore_device(device='/dev/sdb', active=True)
        self._test_active_bluestore_device(device='/dev/sdc', active=True)

    def test_active_bluestore_device_inactive_not_pv(self):
        self._test_active_bluestore_device(device='/dev/sdd', active=False)

    def test_active_bluestore_device_inactive_not_inuse(self):
        self._test_active_bluestore_device(device='/dev/sde', active=False)


class CephLUKSDeviceTestCase(unittest.TestCase):

    @patch.object(utils, '_luks_uuid')
    def test_no_luks_header(self, _luks_uuid):
        _luks_uuid.return_value = None
        self.assertEqual(utils.is_luks_device('/dev/sdb'), False)

    @patch.object(utils, '_luks_uuid')
    def test_luks_header(self, _luks_uuid):
        _luks_uuid.return_value = '5e1e4c89-4f68-4b9a-bd93-e25eec34e80f'
        self.assertEqual(utils.is_luks_device('/dev/sdb'), True)


class CephMappedLUKSDeviceTestCase(unittest.TestCase):

    @patch.object(utils.os, 'walk')
    @patch.object(utils, '_luks_uuid')
    def test_no_luks_header_not_mapped(self, _luks_uuid, _walk):
        _luks_uuid.return_value = None

        def os_walk_side_effect(path):
            return {
                '/sys/class/block/sdb/holders/': iter([('', [], [])]),
            }[path]
        _walk.side_effect = os_walk_side_effect

        self.assertEqual(utils.is_mapped_luks_device('/dev/sdb'), False)

    @patch.object(utils.os, 'walk')
    @patch.object(utils, '_luks_uuid')
    def test_luks_header_mapped(self, _luks_uuid, _walk):
        _luks_uuid.return_value = 'db76d142-4782-42f2-84c6-914f9db889a0'

        def os_walk_side_effect(path):
            return {
                '/sys/class/block/sdb/holders/': iter([('', ['dm-0'], [])]),
            }[path]
        _walk.side_effect = os_walk_side_effect

        self.assertEqual(utils.is_mapped_luks_device('/dev/sdb'), True)

    @patch.object(utils.os, 'walk')
    @patch.object(utils, '_luks_uuid')
    def test_luks_header_not_mapped(self, _luks_uuid, _walk):
        _luks_uuid.return_value = 'db76d142-4782-42f2-84c6-914f9db889a0'

        def os_walk_side_effect(path):
            return {
                '/sys/class/block/sdb/holders/': iter([('', [], [])]),
            }[path]
        _walk.side_effect = os_walk_side_effect

        self.assertEqual(utils.is_mapped_luks_device('/dev/sdb'), False)

    @patch.object(utils.os, 'walk')
    @patch.object(utils, '_luks_uuid')
    def test_no_luks_header_mapped(self, _luks_uuid, _walk):
        """
        This is an edge case where a device is mapped (i.e. used for something
        else) but has no LUKS header. Should be handled by other checks.
        """
        _luks_uuid.return_value = None

        def os_walk_side_effect(path):
            return {
                '/sys/class/block/sdb/holders/': iter([('', ['dm-0'], [])]),
            }[path]
        _walk.side_effect = os_walk_side_effect

        self.assertEqual(utils.is_mapped_luks_device('/dev/sdb'), False)


class CephAllocateVolumeTestCase(unittest.TestCase):

    _lvs = ['osd-data-1234', 'osd-block-1234', 'osd-journal-1234']
    _vgs = {
        '/dev/sdb': 'ceph-1234'
    }

    @patch.object(utils, '_initialize_disk')
    @patch.object(utils.uuid, 'uuid4')
    @patch.object(utils, 'lvm')
    def _test_allocate_logical_volume(self, _lvm, _uuid4, _initialize_disk,
                                      dev, lv_type, osd_fsid,
                                      size=None, shared=False, encrypt=False,
                                      key_manager='ceph'):
        test_uuid = '1234-1234-1234-1234'
        pv_dev = utils._partition_name(dev)

        _lvm.list_logical_volumes.return_value = self._lvs
        _initialize_disk.return_value = pv_dev
        _lvm.is_lvm_physical_volume.side_effect = lambda pv: pv in self._vgs
        _lvm.list_lvm_volume_group.side_effect = lambda pv: self._vgs.get(pv)

        _uuid4.return_value = test_uuid

        lv_name = 'osd-{}-{}'.format(lv_type, osd_fsid)
        if shared:
            vg_name = 'ceph-{}-{}'.format(lv_type, test_uuid)
        else:
            vg_name = 'ceph-{}'.format(osd_fsid)

        self.assertEqual(utils._allocate_logical_volume(dev, lv_type, osd_fsid,
                                                        size, shared),
                         '{}/{}'.format(vg_name, lv_name))

        if pv_dev not in self._vgs:
            _lvm.create_lvm_physical_volume.assert_called_with(pv_dev)
            _lvm.create_lvm_volume_group.assert_called_with(vg_name, pv_dev)
        else:
            _lvm.create_lvm_physical_volume.assert_not_called()
            _lvm.create_lvm_volume_group.assert_not_called()
            _lvm.list_lvm_volume_group.assert_called_with(pv_dev)

        if lv_name not in self._lvs:
            _lvm.create_logical_volume.assert_called_with(lv_name, vg_name,
                                                          size)
        else:
            _lvm.create_logical_volume.assert_not_called()

        _initialize_disk.assert_called_with(
            dev,
            osd_fsid if not shared else test_uuid,
            encrypt,
            key_manager
        )

    def test_allocate_lv_already_pv(self):
        self._test_allocate_logical_volume(dev='/dev/sdb', lv_type='data',
                                           osd_fsid='1234')

    def test_allocate_lv_new_pv(self):
        self._test_allocate_logical_volume(dev='/dev/sdc', lv_type='data',
                                           osd_fsid='5678')

    def test_allocate_lv_shared_type(self):
        self._test_allocate_logical_volume(dev='/dev/sdc', lv_type='wal',
                                           osd_fsid='5678', shared=True)

    def test_allocate_lv_already_exists(self):
        self._test_allocate_logical_volume(dev='/dev/sdd', lv_type='data',
                                           osd_fsid='1234')


class CephDiskTestCase(unittest.TestCase):

    @patch.object(utils, 'cmp_pkgrevno')
    @patch.object(utils, 'find_least_used_utility_device')
    @patch.object(utils, 'get_devices')
    def test_ceph_disk_filestore(self, _get_devices,
                                 _find_least_used_utility_device,
                                 _cmp_pkgrevno):
        # >= Jewel < Luminous RC
        _cmp_pkgrevno.side_effect = [1, -1]
        _get_devices.return_value = []
        self.assertEqual(
            utils._ceph_disk('/dev/sdb',
                             osd_format='xfs',
                             osd_journal=None,
                             encrypt=False,
                             bluestore=False),
            ['ceph-disk', 'prepare',
             '--fs-type', 'xfs',
             '/dev/sdb']
        )

    @patch.object(utils, 'cmp_pkgrevno')
    @patch.object(utils, 'find_least_used_utility_device')
    @patch.object(utils, 'get_devices')
    def test_ceph_disk_filestore_luminous(self, _get_devices,
                                          _find_least_used_utility_device,
                                          _cmp_pkgrevno):
        # >= Jewel
        _cmp_pkgrevno.return_value = 1
        _get_devices.return_value = []
        self.assertEqual(
            utils._ceph_disk('/dev/sdb',
                             osd_format='xfs',
                             osd_journal=None,
                             encrypt=False,
                             bluestore=False),
            ['ceph-disk', 'prepare',
             '--fs-type', 'xfs',
             '--filestore', '/dev/sdb']
        )

    @patch.object(utils, 'cmp_pkgrevno')
    @patch.object(utils, 'find_least_used_utility_device')
    @patch.object(utils, 'get_devices')
    def test_ceph_disk_filestore_journal(self, _get_devices,
                                         _find_least_used_utility_device,
                                         _cmp_pkgrevno):
        # >= Jewel
        _cmp_pkgrevno.return_value = 1
        _get_devices.return_value = []
        _find_least_used_utility_device.side_effect = \
            lambda x, lvs=False: x[0]
        self.assertEqual(
            utils._ceph_disk('/dev/sdb',
                             osd_format='xfs',
                             osd_journal=['/dev/sdc'],
                             encrypt=False,
                             bluestore=False),
            ['ceph-disk', 'prepare',
             '--fs-type', 'xfs',
             '--filestore', '/dev/sdb',
             '/dev/sdc']
        )

    @patch.object(utils, 'cmp_pkgrevno')
    @patch.object(utils, 'find_least_used_utility_device')
    @patch.object(utils, 'get_devices')
    def test_ceph_disk_bluestore(self, _get_devices,
                                 _find_least_used_utility_device,
                                 _cmp_pkgrevno):
        # >= Jewel
        _cmp_pkgrevno.return_value = 1
        _get_devices.return_value = []
        _find_least_used_utility_device.side_effect = \
            lambda x, lvs=False: x[0]
        self.assertEqual(
            utils._ceph_disk('/dev/sdb',
                             osd_format='xfs',
                             osd_journal=None,
                             encrypt=False,
                             bluestore=True),
            ['ceph-disk', 'prepare',
             '--bluestore', '/dev/sdb']
        )

    @patch.object(utils, 'cmp_pkgrevno')
    @patch.object(utils, 'find_least_used_utility_device')
    @patch.object(utils, 'get_devices')
    def test_ceph_disk_bluestore_dbwal(self, _get_devices,
                                       _find_least_used_utility_device,
                                       _cmp_pkgrevno):
        # >= Jewel
        _cmp_pkgrevno.return_value = 1
        _bluestore_devs = {
            'bluestore-db': ['/dev/sdc'],
            'bluestore-wal': ['/dev/sdd'],
        }
        _get_devices.side_effect = lambda x: _bluestore_devs.get(x, [])
        _find_least_used_utility_device.side_effect = \
            lambda x, lvs=False: x[0]
        self.assertEqual(
            utils._ceph_disk('/dev/sdb',
                             osd_format='xfs',
                             osd_journal=None,
                             encrypt=False,
                             bluestore=True),
            ['ceph-disk', 'prepare',
             '--bluestore',
             '--block.wal', '/dev/sdd',
             '--block.db', '/dev/sdc',
             '/dev/sdb']
        )


class CephVolumeTestCase(unittest.TestCase):

    _osd_uuid = '22b371a5-0db9-4154-b011-23f8f03c4d8c'

    @patch.object(utils.uuid, 'uuid4')
    @patch.object(utils, 'calculate_volume_size')
    @patch.object(utils, 'find_least_used_utility_device')
    @patch.object(utils, 'get_devices')
    @patch.object(utils, '_allocate_logical_volume')
    def test_ceph_volume_filestore(self, _allocate_logical_volume,
                                   _get_devices,
                                   _find_least_used_utility_device,
                                   _calculate_volume_size, _uuid4):
        _get_devices.return_value = []
        _calculate_volume_size.return_value = 1024
        _uuid4.return_value = self._osd_uuid
        _allocate_logical_volume.side_effect = (
            lambda *args, **kwargs: (
                'ceph-{osd_fsid}/osd-{lv_type}-{osd_fsid}'.format(**kwargs)
            )
        )
        self.assertEqual(
            utils._ceph_volume('/dev/sdb',
                               osd_journal=None,
                               encrypt=False,
                               bluestore=False),
            ['ceph-volume',
             'lvm',
             'create',
             '--osd-fsid',
             self._osd_uuid,
             '--filestore',
             '--journal',
             ('ceph-{fsid}/'
              'osd-journal-{fsid}').format(fsid=self._osd_uuid),
             '--data',
             ('ceph-{fsid}/'
              'osd-data-{fsid}').format(fsid=self._osd_uuid)]
        )
        _allocate_logical_volume.assert_has_calls([
            call(dev='/dev/sdb', lv_type='journal',
                 osd_fsid=self._osd_uuid, size='1024M',
                 encrypt=False, key_manager='ceph'),
            call(dev='/dev/sdb', lv_type='data',
                 osd_fsid=self._osd_uuid,
                 encrypt=False, key_manager='ceph'),
        ])
        _find_least_used_utility_device.assert_not_called()
        _calculate_volume_size.assert_called_with('journal')

    @patch.object(utils.uuid, 'uuid4')
    @patch.object(utils, 'calculate_volume_size')
    @patch.object(utils, 'find_least_used_utility_device')
    @patch.object(utils, 'get_devices')
    @patch.object(utils, '_allocate_logical_volume')
    def test_ceph_volume_filestore_db_and_wal(self, _allocate_logical_volume,
                                              _get_devices,
                                              _find_least_used_utility_device,
                                              _calculate_volume_size, _uuid4):
        _find_least_used_utility_device.side_effect = \
            lambda x, lvs=False: x[0]
        _calculate_volume_size.return_value = 1024
        _uuid4.return_value = self._osd_uuid
        _allocate_logical_volume.side_effect = (
            lambda *args, **kwargs: (
                'ceph-{osd_fsid}/osd-{lv_type}-{osd_fsid}'.format(**kwargs)
            )
        )
        self.assertEqual(
            utils._ceph_volume('/dev/sdb',
                               osd_journal=['/dev/sdc'],
                               encrypt=False,
                               bluestore=False),
            ['ceph-volume',
             'lvm',
             'create',
             '--osd-fsid',
             self._osd_uuid,
             '--filestore',
             '--data',
             ('ceph-{fsid}/'
              'osd-data-{fsid}').format(fsid=self._osd_uuid),
             '--journal',
             ('ceph-{fsid}/'
              'osd-journal-{fsid}').format(fsid=self._osd_uuid)]
        )
        _allocate_logical_volume.assert_has_calls([
            call(dev='/dev/sdb', lv_type='data',
                 osd_fsid=self._osd_uuid,
                 encrypt=False, key_manager='ceph'),
            call(dev='/dev/sdc', lv_type='journal',
                 osd_fsid=self._osd_uuid,
                 shared=True, size='1024M',
                 encrypt=False, key_manager='ceph'),
        ])
        _find_least_used_utility_device.assert_has_calls([
            call(['/dev/sdc'], lvs=True),
        ])
        _calculate_volume_size.assert_has_calls([
            call('journal'),
        ])

    @patch.object(utils.uuid, 'uuid4')
    @patch.object(utils, 'calculate_volume_size')
    @patch.object(utils, 'find_least_used_utility_device')
    @patch.object(utils, 'get_devices')
    @patch.object(utils, '_allocate_logical_volume')
    def test_ceph_volume_bluestore(self, _allocate_logical_volume,
                                   _get_devices,
                                   _find_least_used_utility_device,
                                   _calculate_volume_size, _uuid4):
        _get_devices.return_value = []
        _calculate_volume_size.return_value = 1024
        _uuid4.return_value = self._osd_uuid
        _allocate_logical_volume.side_effect = (
            lambda *args, **kwargs: (
                'ceph-{osd_fsid}/osd-{lv_type}-{osd_fsid}'.format(**kwargs)
            )
        )
        self.assertEqual(
            utils._ceph_volume('/dev/sdb',
                               osd_journal=None,
                               encrypt=False,
                               bluestore=True),
            ['ceph-volume',
             'lvm',
             'create',
             '--osd-fsid',
             self._osd_uuid,
             '--bluestore',
             '--data',
             ('ceph-{fsid}/'
              'osd-block-{fsid}').format(fsid=self._osd_uuid)]
        )
        _allocate_logical_volume.assert_has_calls([
            call(dev='/dev/sdb', lv_type='block',
                 osd_fsid=self._osd_uuid,
                 encrypt=False, key_manager='ceph'),
        ])
        _find_least_used_utility_device.assert_not_called()
        _calculate_volume_size.assert_not_called()

    @patch.object(utils.uuid, 'uuid4')
    @patch.object(utils, 'calculate_volume_size')
    @patch.object(utils, 'find_least_used_utility_device')
    @patch.object(utils, 'get_devices')
    @patch.object(utils, '_allocate_logical_volume')
    def test_ceph_volume_bluestore_db_and_wal(self, _allocate_logical_volume,
                                              _get_devices,
                                              _find_least_used_utility_device,
                                              _calculate_volume_size, _uuid4):
        _bluestore_devs = {
            'bluestore-db': ['/dev/sdc'],
            'bluestore-wal': ['/dev/sdd'],
        }
        _get_devices.side_effect = lambda x: _bluestore_devs.get(x, [])
        _find_least_used_utility_device.side_effect = \
            lambda x, lvs=False: x[0]
        _calculate_volume_size.return_value = 1024
        _uuid4.return_value = self._osd_uuid
        _allocate_logical_volume.side_effect = (
            lambda *args, **kwargs: (
                'ceph-{osd_fsid}/osd-{lv_type}-{osd_fsid}'.format(**kwargs)
            )
        )
        self.assertEqual(
            utils._ceph_volume('/dev/sdb',
                               osd_journal=None,
                               encrypt=False,
                               bluestore=True),
            ['ceph-volume',
             'lvm',
             'create',
             '--osd-fsid',
             self._osd_uuid,
             '--bluestore',
             '--data',
             ('ceph-{fsid}/'
              'osd-block-{fsid}').format(fsid=self._osd_uuid),
             '--block.wal',
             ('ceph-{fsid}/'
              'osd-wal-{fsid}').format(fsid=self._osd_uuid),
             '--block.db',
             ('ceph-{fsid}/'
              'osd-db-{fsid}').format(fsid=self._osd_uuid)]
        )
        _allocate_logical_volume.assert_has_calls([
            call(dev='/dev/sdb', lv_type='block',
                 osd_fsid=self._osd_uuid,
                 encrypt=False, key_manager='ceph'),
            call(dev='/dev/sdd', lv_type='wal',
                 osd_fsid=self._osd_uuid,
                 shared=True, size='1024M',
                 encrypt=False, key_manager='ceph'),
            call(dev='/dev/sdc', lv_type='db',
                 osd_fsid=self._osd_uuid,
                 shared=True, size='1024M',
                 encrypt=False, key_manager='ceph'),
        ])
        _find_least_used_utility_device.assert_has_calls([
            call(['/dev/sdd'], lvs=True),
            call(['/dev/sdc'], lvs=True),
        ])
        _calculate_volume_size.assert_has_calls([
            call('wal'),
            call('db'),
        ])


class CephVersionTestCase(unittest.TestCase):
    @patch.object(utils, 'get_os_codename_install_source')
    def test_resolve_ceph_version_trusty(self, get_os_codename_install_source):
        get_os_codename_install_source.return_value = 'juno'
        self.assertEqual(utils.resolve_ceph_version('cloud:trusty-juno'),
                         'firefly')
        get_os_codename_install_source.return_value = 'kilo'
        self.assertEqual(utils.resolve_ceph_version('cloud:trusty-kilo'),
                         'hammer')
        get_os_codename_install_source.return_value = 'liberty'
        self.assertEqual(utils.resolve_ceph_version(
                         'cloud:trusty-liberty'), 'hammer')
        get_os_codename_install_source.return_value = 'mitaka'
        self.assertEqual(utils.resolve_ceph_version(
                         'cloud:trusty-mitaka'), 'jewel')
        get_os_codename_install_source.return_value = 'newton'
        self.assertEqual(utils.resolve_ceph_version(
                         'cloud:xenial-newton'), 'jewel')
        get_os_codename_install_source.return_value = 'ocata'
        self.assertEqual(utils.resolve_ceph_version(
                         'cloud:xenial-ocata'), 'jewel')


class CephFindLeastUsedDeviceTestCase(unittest.TestCase):

    _parts = {
        '/dev/sdb': ['1', '2', '3'],
        '/dev/sdc': ['1']
    }

    @patch.object(utils, 'get_partitions')
    def test_find_least_used_utility_device(self, _get_partitions):
        _get_partitions.side_effect = lambda dev: self._parts.get(dev, [])
        self.assertEqual(
            utils.find_least_used_utility_device(['/dev/sdb',
                                                  '/dev/sdx',
                                                  '/dev/sdc']),
            '/dev/sdx'
        )
        self.assertEqual(
            utils.find_least_used_utility_device(['/dev/sdb', '/dev/sdc']),
            '/dev/sdc'
        )
        self.assertEqual(
            utils.find_least_used_utility_device(['/dev/sdb']),
            '/dev/sdb'
        )
        _get_partitions.assert_called()

    @patch.object(utils, 'get_lvs')
    def test_find_least_used_utility_device_lvs(self, _get_lvs):
        _get_lvs.side_effect = lambda dev: self._parts.get(dev, [])
        self.assertEqual(
            utils.find_least_used_utility_device(['/dev/sdb',
                                                  '/dev/sdx',
                                                  '/dev/sdc'],
                                                 lvs=True),
            '/dev/sdx'
        )
        self.assertEqual(
            utils.find_least_used_utility_device(['/dev/sdb', '/dev/sdc'],
                                                 lvs=True),
            '/dev/sdc'
        )
        self.assertEqual(
            utils.find_least_used_utility_device(['/dev/sdb'],
                                                 lvs=True),
            '/dev/sdb'
        )
        _get_lvs.assert_called()


class CephGetLVSTestCase(unittest.TestCase):

    _lvs = {
        'testvg': ['lv1', 'lv2', 'lv3']
    }

    @patch.object(utils, 'lvm')
    def test_get_lvs(self, _lvm):
        _lvm.is_lvm_physical_volume.return_value = True
        _lvm.list_lvm_volume_group.return_value = 'testvg'
        _lvm.list_logical_volumes.side_effect = (
            lambda vg: self._lvs.get(vg.lstrip('vg_name='), [])
        )
        self.assertEqual(utils.get_lvs('/dev/sdb'),
                         self._lvs['testvg'])
        _lvm.is_lvm_physical_volume.assert_called_with(
            '/dev/sdb'
        )
        _lvm.list_lvm_volume_group.assert_called_with(
            '/dev/sdb'
        )
        _lvm.list_logical_volumes.assert_called_with('vg_name=testvg')

    @patch.object(utils, 'lvm')
    def test_get_lvs_no_lvs(self, _lvm):
        _lvm.is_lvm_physical_volume.return_value = True
        _lvm.list_lvm_volume_group.return_value = 'missingvg'
        _lvm.list_logical_volumes.side_effect = (
            lambda vg: self._lvs.get(vg.lstrip('vg_name='), [])
        )
        self.assertEqual(utils.get_lvs('/dev/sdb'), [])
        _lvm.is_lvm_physical_volume.assert_called_with(
            '/dev/sdb'
        )
        _lvm.list_lvm_volume_group.assert_called_with(
            '/dev/sdb'
        )
        _lvm.list_logical_volumes.assert_called_with('vg_name=missingvg')

    @patch.object(utils, 'lvm')
    def test_get_lvs_no_pv(self, _lvm):
        _lvm.is_lvm_physical_volume.return_value = False
        self.assertEqual(utils.get_lvs('/dev/sdb'), [])
        _lvm.is_lvm_physical_volume.assert_called_with(
            '/dev/sdb'
        )

    @patch.object(utils, 'log')
    def test_is_pristine_disk(self, _log):
        data = b'\0' * 2048
        fake_open = mock_open(read_data=data)
        with patch('ceph.utils.open', fake_open):
            result = utils.is_pristine_disk('/dev/vdz')
        fake_open.assert_called_with('/dev/vdz', 'rb')
        self.assertFalse(_log.called)
        self.assertEqual(result, True)

    @patch.object(utils, 'log')
    def test_is_pristine_disk_short_read(self, _log):
        data = b'\0' * 2047
        fake_open = mock_open(read_data=data)
        with patch('ceph.utils.open', fake_open):
            result = utils.is_pristine_disk('/dev/vdz')
        fake_open.assert_called_with('/dev/vdz', 'rb')
        _log.assert_called_with(
            '/dev/vdz: short read, got 2047 bytes expected 2048.',
            level='WARNING')
        self.assertEqual(result, False)

    def test_is_pristine_disk_dirty_disk(self):
        data = b'\0' * 2047
        data = data + b'\42'
        fake_open = mock_open(read_data=data)
        with patch('ceph.utils.open', fake_open):
            result = utils.is_pristine_disk('/dev/vdz')
        fake_open.assert_called_with('/dev/vdz', 'rb')
        self.assertEqual(result, False)
