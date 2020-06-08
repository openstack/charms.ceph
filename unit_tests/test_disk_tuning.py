# Copyright 2017 Canonical Ltd
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


class DiskTuningTestCase(unittest.TestCase):
    def setUp(self):
        super(DiskTuningTestCase, self).setUp()

    @patch.object(charms_ceph.utils, 'templating')
    def test_persist_settings(self, _templating):
        renderer = _templating.render
        settings = {
            'drive_settings': {
                'some-random-uuid': {
                    'read_ahead_sect': 256
                }
            }
        }
        charms_ceph.utils.persist_settings(settings)
        renderer.assert_called_once_with(source='hdparm.conf',
                                         target=charms_ceph.utils.HDPARM_FILE,
                                         context=settings)

    @patch.object(charms_ceph.utils, 'templating')
    def test_persist_settings_empty_dict(self, _templating):
        renderer = _templating.render
        charms_ceph.utils.persist_settings({})
        assert not renderer.called, 'renderer should not have been called'
