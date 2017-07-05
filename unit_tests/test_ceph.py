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

import mock
import unittest
import ceph
from subprocess import CalledProcessError
import subprocess


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

    @mock.patch.object(ceph, 'check_output')
    def test_get_osd_weight(self, output):
        """It gives an OSD's weight"""
        output.return_value = """{
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
        weight = ceph.get_osd_weight('osd.0')
        self.assertEqual(weight, 0.002899)

    def test_get_named_key_with_pool(self):
        with mock.patch.object(ceph, "ceph_user", return_value="ceph"):
            with mock.patch.object(ceph, "check_output") \
                    as subprocess:
                with mock.patch.object(ceph.socket, "gethostname",
                                       return_value="osd001"):
                    subprocess.side_effect = [
                        CalledProcessError(0, 0, 0), ""]
                    ceph.get_named_key(name="rgw001",
                                       pool_list=["rbd", "block"])
                    subprocess.assert_has_calls([
                        mock.call(['sudo', '-u', 'ceph', 'ceph', '--name',
                                   'mon.', '--keyring',
                                   '/var/lib/ceph/mon/ceph-osd001/keyring',
                                   'auth', 'get', 'client.rgw001']),
                        mock.call(['sudo', '-u', 'ceph', 'ceph', '--name',
                                   'mon.', '--keyring',
                                   '/var/lib/ceph/mon/ceph-osd001/keyring',
                                   'auth', 'get-or-create', 'client.rgw001',
                                   'mon', 'allow r', 'osd',
                                   'allow rwx pool=rbd pool=block'])])

    def test_get_named_key(self):
        with mock.patch.object(ceph, "ceph_user", return_value="ceph"):
            with mock.patch.object(ceph, "check_output") \
                    as subprocess:
                subprocess.side_effect = [
                    CalledProcessError(0, 0, 0),
                    ""]
                with mock.patch.object(ceph.socket, "gethostname",
                                       return_value="osd001"):
                    ceph.get_named_key(name="rgw001")
                    for call in subprocess.mock_calls:
                        print("Subprocess: {}".format(call))
                    subprocess.assert_has_calls([
                        mock.call(['sudo', '-u', 'ceph', 'ceph', '--name',
                                   'mon.', '--keyring',
                                   '/var/lib/ceph/mon/ceph-osd001/keyring',
                                   'auth', 'get', 'client.rgw001']),
                        mock.call(['sudo', '-u', 'ceph', 'ceph', '--name',
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
        key = ceph.parse_key(with_caps)
        print("key: {}".format(key))
        self.assertEqual(key, expected)

    def test_parse_key_without_caps(self):
        expected = "AQCm7aVYQFXXFhAAj0WIeqcag88DKOvY4UKR/g=="
        without_caps = "[client.osd-upgrade]\n" \
                       "	key = AQCm7aVYQFXXFhAAj0WIeqcag88DKOvY4UKR/g=="
        key = ceph.parse_key(without_caps)
        print("key: {}".format(key))
        self.assertEqual(key, expected)

    def test_list_unmounted_devices(self):
        dev1 = mock.MagicMock(spec=TestDevice)
        dev1.__getitem__.return_value = "block"
        dev1.device_node = '/dev/sda'
        dev2 = mock.MagicMock(spec=TestDevice)
        dev2.__getitem__.return_value = "block"
        dev2.device_node = '/dev/sdb'
        dev3 = mock.MagicMock(spec=TestDevice)
        dev3.__getitem__.return_value = "block"
        dev3.device_node = '/dev/loop1'
        devices = [dev1, dev2, dev3]
        with mock.patch(
                'pyudev.Context.list_devices',
                return_value=devices):
            with mock.patch.object(ceph,
                                   'is_device_mounted',
                                   return_value=False):
                devices = ceph.unmounted_disks()
                self.assertEqual(devices, ['/dev/sda', '/dev/sdb'])
            with mock.patch.object(ceph,
                                   'is_device_mounted',
                                   return_value=True):
                devices = ceph.unmounted_disks()
                self.assertEqual(devices, [])

    @mock.patch.object(ceph, 'check_output')
    def test_get_partition_list(self, output):
        with open('unit_tests/partx_output', 'r') as partx_out:
            output.return_value = partx_out.read()
        partition_list = ceph.get_partition_list('/dev/xvdb')
        self.assertEqual(len(partition_list), 2)

    @mock.patch.object(ceph, 'check_output')
    def test_get_ceph_pg_stat(self, output):
        """It returns the current PG stat"""
        output.return_value = """{
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
        pg_stat = ceph.get_ceph_pg_stat()
        self.assertEqual(pg_stat['num_pgs'], 320)

    @mock.patch.object(ceph, 'check_output')
    def test_get_ceph_health(self, output):
        """It gives the current Ceph health"""
        output.return_value = """{
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
        health = ceph.get_ceph_health()
        self.assertEqual(health['overall_status'], "HEALTH_OK")

    @mock.patch.object(subprocess, 'check_output')
    def test_reweight_osd(self, mock_reweight):
        """It changes the weight of an OSD"""
        mock_reweight.return_value = "reweighted item id 0 name 'osd.0' to 1"
        reweight_result = ceph.reweight_osd('0', '1')
        self.assertEqual(reweight_result, True)
        mock_reweight.assert_called_once_with(
            ['ceph', 'osd', 'crush', 'reweight', 'osd.0', '1'], stderr=-2)

    @mock.patch.object(ceph, 'is_container')
    def test_determine_packages(self, mock_is_container):
        mock_is_container.return_value = False
        self.assertTrue('ntp' in ceph.determine_packages())
        self.assertEqual(ceph.PACKAGES, ceph.determine_packages())

        mock_is_container.return_value = True
        self.assertFalse('ntp' in ceph.determine_packages())

    @mock.patch.object(ceph, 'chownr')
    @mock.patch.object(ceph, 'cmp_pkgrevno')
    @mock.patch.object(ceph, 'ceph_user')
    @mock.patch.object(ceph, 'os')
    @mock.patch.object(ceph, 'systemd')
    @mock.patch.object(ceph, 'log')
    @mock.patch.object(ceph, 'mkdir')
    @mock.patch.object(ceph, 'subprocess')
    @mock.patch.object(ceph, 'service_restart')
    def _test_bootstrap_monitor_cluster(self,
                                        mock_service_restart,
                                        mock_subprocess,
                                        mock_mkdir,
                                        mock_log,
                                        mock_systemd,
                                        mock_os,
                                        mock_ceph_user,
                                        mock_cmp_pkgrevno,
                                        mock_chownr,
                                        luminos=False):
        test_hostname = ceph.socket.gethostname()
        test_secret = 'mysecret'
        test_keyring = '/var/lib/ceph/tmp/{}.mon.keyring'.format(test_hostname)
        test_path = '/var/lib/ceph/mon/ceph-{}'.format(test_hostname)
        test_done = '{}/done'.format(test_path)
        test_init_marker = '{}/systemd'.format(test_path)

        mock_os.path.exists.return_value = False
        mock_systemd.return_value = True
        mock_cmp_pkgrevno.return_value = 1 if luminos else -1
        mock_ceph_user.return_value = 'ceph'

        test_calls = [
            mock.call(
                ['ceph-authtool', test_keyring,
                 '--create-keyring', '--name=mon.',
                 '--add-key={}'.format(test_secret),
                 '--cap', 'mon', 'allow *']
            ),
            mock.call(
                ['ceph-mon', '--mkfs',
                 '-i', test_hostname,
                 '--keyring', test_keyring]
            ),
            mock.call(['systemctl', 'enable', 'ceph-mon']),
        ]
        if luminos:
            test_calls.append(
                mock.call(['ceph-create-keys', '--id', test_hostname])
            )

        mock_open = mock.mock_open()
        with mock.patch('ceph.open', mock_open, create=True):
            ceph.bootstrap_monitor_cluster(test_secret)

        self.assertEqual(
            mock_subprocess.check_call.mock_calls,
            test_calls
        )
        mock_service_restart.assert_called_with('ceph-mon')
        mock_mkdir.assert_has_calls([
            mock.call('/var/run/ceph', owner='ceph',
                      group='ceph', perms=0o755),
            mock.call(test_path, owner='ceph', group='ceph'),
        ])
        mock_open.assert_has_calls([
            mock.call(test_done, 'w'),
            mock.call(test_init_marker, 'w'),
        ], any_order=True)
        mock_os.unlink.assert_called_with(test_keyring)

    def test_bootstrap_monitor_cluster(self):
        self._test_bootstrap_monitor_cluster(luminos=False)

    def test_bootstrap_monitor_cluster_luminous(self):
        self._test_bootstrap_monitor_cluster(luminos=True)


class CephVersionTestCase(unittest.TestCase):
    @mock.patch.object(ceph, 'get_os_codename_install_source')
    def test_resolve_ceph_version_trusty(self, get_os_codename_install_source):
        get_os_codename_install_source.return_value = 'juno'
        self.assertEqual(ceph.resolve_ceph_version('cloud:trusty-juno'),
                         'firefly')
        get_os_codename_install_source.return_value = 'kilo'
        self.assertEqual(ceph.resolve_ceph_version('cloud:trusty-kilo'),
                         'hammer')
        get_os_codename_install_source.return_value = 'liberty'
        self.assertEqual(ceph.resolve_ceph_version('cloud:trusty-liberty'),
                         'hammer')
        get_os_codename_install_source.return_value = 'mitaka'
        self.assertEqual(ceph.resolve_ceph_version('cloud:trusty-mitaka'),
                         'jewel')
