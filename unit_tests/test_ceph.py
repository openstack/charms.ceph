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
        with_caps = "[client.osd-upgrade]\n"\
            "	key = AQCm7aVYQFXXFhAAj0WIeqcag88DKOvY4UKR/g==\n"\
            "	caps mon = \"allow command \"config-key\";"
        key = ceph.parse_key(with_caps)
        print("key: {}".format(key))
        self.assertEqual(key, expected)

    def test_parse_key_without_caps(self):
        expected = "AQCm7aVYQFXXFhAAj0WIeqcag88DKOvY4UKR/g=="
        without_caps = "[client.osd-upgrade]\n"\
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
