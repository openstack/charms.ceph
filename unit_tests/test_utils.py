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

    @patch.object(utils.subprocess, 'check_call')
    @patch.object(utils.os.path, 'exists')
    @patch.object(utils, 'is_device_mounted')
    @patch.object(utils, 'cmp_pkgrevno')
    @patch.object(utils, 'is_block_device')
    def test_osdize_dev(self, _is_blk, _cmp, _mounted, _exists, _call):
        """Test that the dev osd is initialized correctly"""
        _is_blk.return_value = True
        _mounted.return_value = False
        _exists.return_value = True
        _cmp.return_value = True
        utils.osdize('/dev/sdb', osd_format='xfs', osd_journal=None,
                     reformat_osd=True, bluestore=False)
        _call.assert_called_with(['ceph-disk', 'prepare', '--fs-type', 'xfs',
                                  '--zap-disk', '--filestore', '/dev/sdb'])

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
                  'mon', 'allow r', 'osd',
                  'allow rwx pool=rbd pool=block'])])

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
                  'mon', 'allow r', 'osd',
                  'allow rwx'])])

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
                call(['ceph-create-keys', '--id', test_hostname])
            )

        fake_open = mock_open()
        with patch('ceph.utils.open', fake_open, create=True):
            utils.bootstrap_monitor_cluster(test_secret)

        mock_check_call.assert_has_calls(test_calls)
        mock_service_restart.assert_called_with('ceph-mon')
        mock_mkdir.assert_has_calls([
            call('/var/run/ceph', owner='ceph',
                 group='ceph', perms=0o755),
            call(test_path, owner='ceph', group='ceph'),
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
        expected = (['firefly -> hammer', 'jewel -> luminous',
                     'hammer -> jewel'])
        self.assertEqual(utils.pretty_print_upgrade_paths(), expected)


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
