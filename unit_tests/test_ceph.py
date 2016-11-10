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

    def test_get_named_key_with_pool(self):
        with mock.patch.object(ceph, "ceph_user", return_value="ceph"):
            with mock.patch.object(ceph.subprocess, "check_output") \
                    as subprocess:
                with mock.patch.object(ceph.socket, "gethostname",
                                       return_value="osd001"):
                    ceph.get_named_key(name="rgw001",
                                       pool_list=["rbd", "block"])
                    subprocess.assert_called_with(
                        ['sudo', '-u', 'ceph', 'ceph', '--name', 'mon.',
                         '--keyring',
                         '/var/lib/ceph/mon/ceph-osd001/keyring',
                         'auth',
                         'get-or-create', 'client.rgw001', 'mon', 'allow r',
                         'osd',
                         'allow rwx pool=rbd pool=block'])

    def test_get_named_key(self):
        with mock.patch.object(ceph, "ceph_user", return_value="ceph"):
            with mock.patch.object(ceph.subprocess, "check_output") \
                    as subprocess:
                with mock.patch.object(ceph.socket, "gethostname",
                                       return_value="osd001"):
                    ceph.get_named_key(name="rgw001")
                    subprocess.assert_called_with(
                        ['sudo', '-u', 'ceph', 'ceph', '--name', 'mon.',
                         '--keyring',
                         '/var/lib/ceph/mon/ceph-osd001/keyring',
                         'auth',
                         'get-or-create', 'client.rgw001', 'mon', 'allow r',
                         'osd',
                         'allow rwx'])

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
