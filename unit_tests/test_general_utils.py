# Copyright 2019 Canonical Ltd
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

from unittest.mock import patch

import charms_ceph.utils


class GeneralUtilsTestCase(unittest.TestCase):
    def setUp(self):
        super(GeneralUtilsTestCase, self).setUp()

    @patch.object(charms_ceph.utils.subprocess, 'call')
    def test_udevadm_settle(self, _call):
        charms_ceph.utils.udevadm_settle()
        _call.assert_called_once_with(['udevadm', 'settle'])

    @patch.object(charms_ceph.utils, 'udevadm_settle')
    @patch.object(charms_ceph.utils.subprocess, 'call')
    def test_rescan_osd_devices(self, _call, _udevadm_settle):
        charms_ceph.utils.rescan_osd_devices()
        _call.assert_called_once_with([
            'udevadm',
            'trigger',
            '--subsystem-match=block',
            '--action=add'])
        _udevadm_settle.assert_called_once_with()


class AllocateLVTestCase(unittest.TestCase):

    @patch.object(charms_ceph.utils, 'rescan_osd_devices')
    @patch.object(charms_ceph.utils, '_initialize_disk')
    @patch.object(charms_ceph.utils.os.path, 'exists')
    @patch.object(charms_ceph.utils, 'lvm')
    def test_allocate_lv_ok(self,
                            _lvm,
                            _exists,
                            _initialize_disk,
                            _rescan_osd_devices):
        _exists.return_value = True
        _lvm.list_logical_volumes.return_value = []
        _lvm.is_lvm_physical_volume.return_value = False
        _initialize_disk.side_effect = (
            lambda dev, dev_uuid, encrypt, key_manager: dev
        )
        self.assertEqual(
            charms_ceph.utils._allocate_logical_volume(
                dev="/dev/disk/by-dname/foobar",
                lv_type="block",
                osd_fsid="abcdef"
            ),
            "ceph-abcdef/osd-block-abcdef"
        )
        _lvm.create_lvm_physical_volume.assert_called_once_with(
            "/dev/disk/by-dname/foobar"
        )
        _lvm.create_lvm_volume_group.assert_called_once_with(
            "ceph-abcdef",
            "/dev/disk/by-dname/foobar"
        )
        _lvm.create_logical_volume.assert_called_once_with(
            "osd-block-abcdef",
            "ceph-abcdef",
            None,
        )
        _rescan_osd_devices.assert_not_called()

    @patch.object(charms_ceph.utils, 'rescan_osd_devices')
    @patch.object(charms_ceph.utils, '_initialize_disk')
    @patch.object(charms_ceph.utils.os.path, 'exists')
    @patch.object(charms_ceph.utils, 'lvm')
    def test_allocate_lv_bug1878752(self,
                                    _lvm,
                                    _exists,
                                    _initialize_disk,
                                    _rescan_osd_devices):
        _exists.return_value = False
        _lvm.list_logical_volumes.return_value = []
        _lvm.is_lvm_physical_volume.return_value = False
        _initialize_disk.side_effect = (
            lambda dev, dev_uuid, encrypt, key_manager: dev
        )
        self.assertEqual(
            charms_ceph.utils._allocate_logical_volume(
                dev="/dev/disk/by-dname/foobar",
                lv_type="block",
                osd_fsid="abcdef"
            ),
            "ceph-abcdef/osd-block-abcdef"
        )
        _lvm.create_lvm_physical_volume.assert_called_once_with(
            "/dev/disk/by-dname/foobar"
        )
        _lvm.create_lvm_volume_group.assert_called_once_with(
            "ceph-abcdef",
            "/dev/disk/by-dname/foobar"
        )
        _lvm.create_logical_volume.assert_called_once_with(
            "osd-block-abcdef",
            "ceph-abcdef",
            None,
        )
        _rescan_osd_devices.assert_called_once_with()
