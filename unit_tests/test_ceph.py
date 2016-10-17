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
