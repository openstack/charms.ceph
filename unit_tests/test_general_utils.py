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

from mock import patch

import ceph.utils


class GeneralUtilsTestCase(unittest.TestCase):
    def setUp(self):
        super(GeneralUtilsTestCase, self).setUp()

    @patch.object(ceph.utils.subprocess, 'call')
    def test_udevadm_settle(self, _call):
        ceph.utils.udevadm_settle()
        _call.assert_called_once_with(['udevadm', 'settle'])

    @patch.object(ceph.utils, 'udevadm_settle')
    @patch.object(ceph.utils.subprocess, 'call')
    def test_rescan_osd_devices(self, _call, _udevadm_settle):
        ceph.utils.rescan_osd_devices()
        _call.assert_called_once_with([
            'udevadm',
            'trigger',
            '--subsystem-match=block',
            '--action=add'])
        _udevadm_settle.assert_called_once_with()
