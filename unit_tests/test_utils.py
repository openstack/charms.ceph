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

import collections
import subprocess
import unittest

from unittest.mock import (
    call,
    mock_open,
    MagicMock,
    patch,
)

import charms_ceph.utils as utils

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
        _pkgrevno.side_effect = [1, -1]
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
    @patch.object(utils.os.path, 'exists')
    @patch.object(utils, 'is_device_mounted')
    @patch.object(utils, 'cmp_pkgrevno')
    @patch.object(utils, 'is_block_device')
    def test_osdize_dev_ceph_disk(self, _is_blk, _cmp, _mounted, _exists,
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
        utils.osdize('/dev/sdb', osd_format='xfs', osd_journal=None)
        _ceph_disk.assert_called_with('/dev/sdb', 'xfs', None, False)
        _check_call.assert_called_with(['ceph-disk', 'prepare'])
        db.get.assert_called_with('osd-devices', [])
        db.set.assert_called_with('osd-devices', ['/dev/sdb'])
        db.flush.assert_called_once()

    @patch.object(utils, 'kv')
    @patch.object(utils.subprocess, 'check_call')
    @patch.object(utils, '_ceph_volume')
    @patch.object(utils, 'is_mapped_luks_device')
    @patch.object(utils.os.path, 'exists')
    @patch.object(utils, 'is_device_mounted')
    @patch.object(utils, 'cmp_pkgrevno')
    @patch.object(utils, 'is_block_device')
    def test_osdize_dev_ceph_volume(self, _is_blk, _cmp, _mounted, _exists,
                                    _is_mapped_luks_device,
                                    _ceph_volume,
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
        utils.osdize('/dev/sdb', osd_format='xfs', osd_journal=None)
        _ceph_volume.assert_called_with('/dev/sdb', None, False,
                                        'ceph', None)
        _check_call.assert_called_with(['ceph-volume', 'prepare'])
        db.get.assert_called_with('osd-devices', [])
        db.set.assert_called_with('osd-devices', ['/dev/sdb'])
        db.flush.assert_called_once()

    @patch.object(utils, 'kv')
    @patch.object(utils.subprocess, 'check_call')
    @patch.object(utils, '_ceph_volume')
    @patch.object(utils, 'is_mapped_luks_device')
    @patch.object(utils.os.path, 'exists')
    @patch.object(utils, 'is_device_mounted')
    @patch.object(utils, 'cmp_pkgrevno')
    @patch.object(utils, 'is_block_device')
    def test_osdize_dev_ceph_volume_with_osdid(self, _is_blk, _cmp, _mounted,
                                               _exists, _is_mapped_luks_device,
                                               _ceph_volume, _check_call, _kv):
        """Test that _ceph_volume is called with an OSD id."""
        db = MagicMock()
        _kv.return_value = db
        db.get.return_value = []
        _is_blk.return_value = True
        _mounted.return_value = False
        _exists.return_value = True
        _cmp.return_value = 1
        _ceph_volume.return_value = ['ceph-volume', 'prepare']
        _is_mapped_luks_device.return_value = False
        utils.osdize('/dev/sdb', osd_format='xfs', osd_journal=None,
                     osd_id=123)
        _ceph_volume.assert_called_with('/dev/sdb', None, False,
                                        'ceph', 123)
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
        utils.osdize('/dev/sdb', osd_format='xfs', osd_journal=None)
        db.get.assert_called_with('osd-devices', [])
        db.set.assert_called_with('osd-devices', ['/dev/sdb'])

    @patch.object(utils, 'kv')
    @patch.object(utils.os.path, 'exists')
    @patch.object(utils, 'is_device_mounted')
    @patch.object(utils, 'is_block_device')
    @patch.object(utils, 'is_osd_disk')
    def test_osdize_dev_already_processed_without_kv(self, _is_osd, _is_blk,
                                                     _mounted, _exists, _kv):
        """Ensure that previously processed disks are skipped"""
        db = MagicMock()
        _kv.return_value = db
        db.get.return_value = []
        _exists.return_value = True
        _is_osd.return_value = True
        _mounted.return_value = True
        _is_blk.return_value = True
        utils.osdize('/dev/sdb', osd_format='xfs', osd_journal=None)
        db.get.assert_called_with('osd-devices', [])
        db.set.assert_called_with('osd-devices', ['/dev/sdb'])

    @patch.object(utils, 'kv')
    @patch.object(utils.subprocess, 'check_call')
    @patch.object(utils, '_ceph_volume')
    @patch.object(utils, 'is_mapped_luks_device')
    @patch.object(utils.os.path, 'exists')
    @patch.object(utils, 'is_device_mounted')
    @patch.object(utils, 'cmp_pkgrevno')
    @patch.object(utils, 'is_block_device')
    def test_osdize_already_processed_luks_bluestore(
            self, _is_blk, _cmp, _mounted,
            _exists,
            _is_mapped_luks_device,
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
        _is_mapped_luks_device.return_value = True
        utils.osdize('/dev/sdb', encrypt=True, osd_format=None,
                     osd_journal=None, key_manager='vault')
        db.get.assert_called_with('osd-devices', [])
        db.set.assert_called_with('osd-devices', [])

    @patch.object(utils, 'cmp_pkgrevno')
    @patch.object(utils, 'kv')
    @patch.object(utils.subprocess, 'check_call')
    @patch.object(utils.os.path, 'exists')
    @patch.object(utils, 'is_device_mounted')
    @patch.object(utils, 'cmp_pkgrevno')
    @patch.object(utils, 'mkdir')
    @patch.object(utils, 'chownr')
    @patch.object(utils, 'ceph_user')
    def test_osdize_dir(self, _ceph_user, _chown, _mkdir,
                        _cmp, _mounted, _exists, _call, _kv,
                        _cmp_pkgrevno):
        """Test that the dev osd is initialized correctly"""
        db = MagicMock()
        _cmp_pkgrevno.side_effect = [-1, 1, 1, 1, 1]
        _kv.return_value = db
        db.get.return_value = []
        _ceph_user.return_value = "ceph"
        _mounted.return_value = False
        _exists.return_value = False
        _cmp.return_value = True
        utils.osdize('/srv/osd', osd_format='xfs', osd_journal=None)
        _call.assert_called_with(['sudo', '-u', 'ceph', 'ceph-disk', 'prepare',
                                  '--data-dir', '/srv/osd', '--bluestore'])

        db.get.assert_called_with('osd-devices', [])
        db.set.assert_called_with('osd-devices', ['/srv/osd'])

    @patch.object(utils, 'cmp_pkgrevno')
    @patch.object(utils, 'kv')
    @patch.object(utils.subprocess, 'check_call')
    @patch.object(utils.os.path, 'exists')
    @patch.object(utils, 'is_device_mounted')
    @patch.object(utils, 'cmp_pkgrevno')
    @patch.object(utils, 'mkdir')
    @patch.object(utils, 'chownr')
    @patch.object(utils, 'ceph_user')
    def test_osdize_dir_nautilus(self, _ceph_user, _chown, _mkdir,
                                 _cmp, _mounted, _exists, _call, _kv,
                                 _cmp_pkgrevno):
        """Test that the dev osd is initialized correctly"""
        db = MagicMock()
        _cmp_pkgrevno.side_effect = [1]
        _kv.return_value = db
        db.get.return_value = []
        _ceph_user.return_value = "ceph"
        _mounted.return_value = False
        _exists.return_value = False
        _cmp.return_value = True
        utils.osdize('/srv/osd', osd_format='xfs', osd_journal=None)
        _call.assert_not_called()

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

    def test_flatten_roots(self):
        nodes = [
            {"id": -6, "name": "default", "type": "root", "children": [-5]},
            {"id": -5, "name": "custom", "type": "row", "children": [-3, -4]},
            {"id": -4, "name": "az.0", "type": "rack", "children": [-2]},
            {"id": -2, "name": "test-host.0", "type": "host", "children": [0]},
            {"id": 0, "name": "osd.0", "type": "osd"},
            {"id": -3, "name": "az.1", "type": "rack", "children": [-1]},
            {"id": -1, "name": "test-host.1", "type": "host", "children": [1]},
            {"id": 1, "name": "osd.1", "type": "osd"},
        ]

        host_nodes = utils._flatten_roots(nodes)
        self.assertEqual(len(host_nodes), 2)
        self.assertEqual(host_nodes[0]["identifier"], -1)
        self.assertEqual(host_nodes[0]["rack"], "az.1")
        self.assertEqual(host_nodes[0]["row"], "custom")
        self.assertEqual(host_nodes[0]["root"], "default")
        self.assertEqual(host_nodes[1]["identifier"], -2)
        self.assertEqual(host_nodes[1]["rack"], "az.0")
        self.assertEqual(host_nodes[1]["row"], "custom")
        self.assertEqual(host_nodes[1]["root"], "default")

        rack_nodes = utils._flatten_roots(nodes, "rack")
        self.assertEqual(len(rack_nodes), 2)
        self.assertEqual(rack_nodes[0]["identifier"], -3)
        self.assertEqual(rack_nodes[0]["row"], "custom")
        self.assertEqual(rack_nodes[0]["root"], "default")
        self.assertEqual(rack_nodes[1]["identifier"], -4)
        self.assertEqual(rack_nodes[1]["row"], "custom")
        self.assertEqual(rack_nodes[1]["root"], "default")

    @patch.object(utils.subprocess, 'check_output')
    def test_get_osd_tree_multi_root(self, mock_check_output):
        mock_check_output.return_value = b"""{
    "nodes":[
        {"id":-9,"name":"ssds","type":"root","type_id":10,"children":[-11,-12,-10]},
        {"id":-10,"name":"OS-CS-10","type":"host","type_id":1,"children":[58,56,52]},
        {"id":52,"name":"osd.52","type":"osd","type_id":0,"crush_weight":1.000000,"depth":2,"exists":1,"status":"up","reweight":1.000000,"primary_affinity":1.000000},
        {"id":56,"name":"osd.56","type":"osd","type_id":0,"crush_weight":1.000000,"depth":2,"exists":1,"status":"up","reweight":1.000000,"primary_affinity":1.000000},
        {"id":58,"name":"osd.58","type":"osd","type_id":0,"crush_weight":1.000000,"depth":2,"exists":1,"status":"up","reweight":1.000000,"primary_affinity":1.000000},
        {"id":-12,"name":"OS-CS-09","type":"host","type_id":1,"children":[59,55,53]},
        {"id":53,"name":"osd.53","type":"osd","type_id":0,"crush_weight":1.000000,"depth":2,"exists":1,"status":"up","reweight":1.000000,"primary_affinity":1.000000},
        {"id":55,"name":"osd.55","type":"osd","type_id":0,"crush_weight":1.000000,"depth":2,"exists":1,"status":"up","reweight":1.000000,"primary_affinity":1.000000},
        {"id":59,"name":"osd.59","type":"osd","type_id":0,"crush_weight":1.000000,"depth":2,"exists":1,"status":"up","reweight":1.000000,"primary_affinity":1.000000},
        {"id":-11,"name":"OS-CS-08","type":"host","type_id":1,"children":[57,54,51]},
        {"id":51,"name":"osd.51","type":"osd","type_id":0,"crush_weight":1.000000,"depth":2,"exists":1,"status":"up","reweight":1.000000,"primary_affinity":1.000000},
        {"id":54,"name":"osd.54","type":"osd","type_id":0,"crush_weight":1.000000,"depth":2,"exists":1,"status":"up","reweight":1.000000,"primary_affinity":1.000000},
        {"id":57,"name":"osd.57","type":"osd","type_id":0,"crush_weight":1.000000,"depth":2,"exists":1,"status":"up","reweight":1.000000,"primary_affinity":1.000000},
        {"id":-1,"name":"default","type":"root","type_id":10,"children":[-8,-7,-6,-5,-4,-3,-2]},
        {"id":-2,"name":"OS-CS-05","type":"host","type_id":1,"children":[33,16,11,7,4,2,0]},
        {"id":0,"name":"osd.0","type":"osd","type_id":0,"crush_weight":0.543991,"depth":2,"exists":1,"status":"up","reweight":1.000000,"primary_affinity":1.000000},
        {"id":2,"name":"osd.2","type":"osd","type_id":0,"crush_weight":0.543991,"depth":2,"exists":1,"status":"up","reweight":1.000000,"primary_affinity":1.000000},
        {"id":4,"name":"osd.4","type":"osd","type_id":0,"crush_weight":0.543991,"depth":2,"exists":1,"status":"up","reweight":1.000000,"primary_affinity":1.000000},
        {"id":7,"name":"osd.7","type":"osd","type_id":0,"crush_weight":0.543991,"depth":2,"exists":1,"status":"up","reweight":1.000000,"primary_affinity":1.000000},
        {"id":11,"name":"osd.11","type":"osd","type_id":0,"crush_weight":0.543991,"depth":2,"exists":1,"status":"up","reweight":1.000000,"primary_affinity":1.000000},
        {"id":16,"name":"osd.16","type":"osd","type_id":0,"crush_weight":0.543991,"depth":2,"exists":1,"status":"up","reweight":1.000000,"primary_affinity":1.000000},
        {"id":33,"name":"osd.33","type":"osd","type_id":0,"crush_weight":1.089996,"depth":2,"exists":1,"status":"up","reweight":1.000000,"primary_affinity":1.000000},
        {"id":-3,"name":"OS-CS-02","type":"host","type_id":1,"children":[35,20,15,10,6,3,1]},
        {"id":1,"name":"osd.1","type":"osd","type_id":0,"crush_weight":0.543991,"depth":2,"exists":1,"status":"up","reweight":1.000000,"primary_affinity":1.000000},
        {"id":3,"name":"osd.3","type":"osd","type_id":0,"crush_weight":0.543991,"depth":2,"exists":1,"status":"up","reweight":1.000000,"primary_affinity":1.000000},
        {"id":6,"name":"osd.6","type":"osd","type_id":0,"crush_weight":0.543991,"depth":2,"exists":1,"status":"up","reweight":1.000000,"primary_affinity":1.000000},
        {"id":10,"name":"osd.10","type":"osd","type_id":0,"crush_weight":0.543991,"depth":2,"exists":1,"status":"up","reweight":1.000000,"primary_affinity":1.000000},
        {"id":15,"name":"osd.15","type":"osd","type_id":0,"crush_weight":0.543991,"depth":2,"exists":1,"status":"up","reweight":1.000000,"primary_affinity":1.000000},
        {"id":20,"name":"osd.20","type":"osd","type_id":0,"crush_weight":0.543991,"depth":2,"exists":1,"status":"up","reweight":1.000000,"primary_affinity":1.000000},
        {"id":35,"name":"osd.35","type":"osd","type_id":0,"crush_weight":1.089996,"depth":2,"exists":1,"status":"up","reweight":1.000000,"primary_affinity":1.000000},
        {"id":-4,"name":"OS-CS-03","type":"host","type_id":1,"children":[34,25,22,18,13,9,5]},
        {"id":5,"name":"osd.5","type":"osd","type_id":0,"crush_weight":0.543991,"depth":2,"exists":1,"status":"up","reweight":1.000000,"primary_affinity":1.000000},
        {"id":9,"name":"osd.9","type":"osd","type_id":0,"crush_weight":0.543991,"depth":2,"exists":1,"status":"up","reweight":1.000000,"primary_affinity":1.000000},
        {"id":13,"name":"osd.13","type":"osd","type_id":0,"crush_weight":0.543991,"depth":2,"exists":1,"status":"up","reweight":1.000000,"primary_affinity":1.000000},
        {"id":18,"name":"osd.18","type":"osd","type_id":0,"crush_weight":0.543991,"depth":2,"exists":1,"status":"up","reweight":1.000000,"primary_affinity":1.000000},
        {"id":22,"name":"osd.22","type":"osd","type_id":0,"crush_weight":0.543991,"depth":2,"exists":1,"status":"up","reweight":1.000000,"primary_affinity":1.000000},
        {"id":25,"name":"osd.25","type":"osd","type_id":0,"crush_weight":0.543991,"depth":2,"exists":1,"status":"up","reweight":1.000000,"primary_affinity":1.000000},
        {"id":34,"name":"osd.34","type":"osd","type_id":0,"crush_weight":1.089996,"depth":2,"exists":1,"status":"up","reweight":1.000000,"primary_affinity":1.000000},
        {"id":-5,"name":"OS-CS-01","type":"host","type_id":1,"children":[31,27,24,21,17,12,8]},
        {"id":8,"name":"osd.8","type":"osd","type_id":0,"crush_weight":0.543991,"depth":2,"exists":1,"status":"up","reweight":1.000000,"primary_affinity":1.000000},
        {"id":12,"name":"osd.12","type":"osd","type_id":0,"crush_weight":0.543991,"depth":2,"exists":1,"status":"up","reweight":1.000000,"primary_affinity":1.000000},
        {"id":17,"name":"osd.17","type":"osd","type_id":0,"crush_weight":0.543991,"depth":2,"exists":1,"status":"up","reweight":1.000000,"primary_affinity":1.000000},
        {"id":21,"name":"osd.21","type":"osd","type_id":0,"crush_weight":0.543991,"depth":2,"exists":1,"status":"up","reweight":1.000000,"primary_affinity":1.000000},
        {"id":24,"name":"osd.24","type":"osd","type_id":0,"crush_weight":0.543991,"depth":2,"exists":1,"status":"up","reweight":1.000000,"primary_affinity":1.000000},
        {"id":27,"name":"osd.27","type":"osd","type_id":0,"crush_weight":0.543991,"depth":2,"exists":1,"status":"up","reweight":1.000000,"primary_affinity":1.000000},
        {"id":31,"name":"osd.31","type":"osd","type_id":0,"crush_weight":1.089996,"depth":2,"exists":1,"status":"up","reweight":1.000000,"primary_affinity":1.000000},
        {"id":-6,"name":"OS-CS-04","type":"host","type_id":1,"children":[32,29,28,26,23,19,14]},
        {"id":14,"name":"osd.14","type":"osd","type_id":0,"crush_weight":0.543991,"depth":2,"exists":1,"status":"up","reweight":1.000000,"primary_affinity":1.000000},
        {"id":19,"name":"osd.19","type":"osd","type_id":0,"crush_weight":0.543991,"depth":2,"exists":1,"status":"up","reweight":1.000000,"primary_affinity":1.000000},
        {"id":23,"name":"osd.23","type":"osd","type_id":0,"crush_weight":0.543991,"depth":2,"exists":1,"status":"up","reweight":1.000000,"primary_affinity":1.000000},
        {"id":26,"name":"osd.26","type":"osd","type_id":0,"crush_weight":0.543991,"depth":2,"exists":1,"status":"up","reweight":1.000000,"primary_affinity":1.000000},
        {"id":28,"name":"osd.28","type":"osd","type_id":0,"crush_weight":0.543991,"depth":2,"exists":1,"status":"up","reweight":1.000000,"primary_affinity":1.000000},
        {"id":29,"name":"osd.29","type":"osd","type_id":0,"crush_weight":0.543991,"depth":2,"exists":1,"status":"up","reweight":1.000000,"primary_affinity":1.000000},
        {"id":32,"name":"osd.32","type":"osd","type_id":0,"crush_weight":1.089996,"depth":2,"exists":1,"status":"up","reweight":1.000000,"primary_affinity":1.000000},
        {"id":-7,"name":"OS-CS-07","type":"host","type_id":1,"children":[42,41,40,39,38,37,36,30]},
        {"id":30,"name":"osd.30","type":"osd","type_id":0,"crush_weight":1.089996,"depth":2,"exists":1,"status":"up","reweight":1.000000,"primary_affinity":1.000000},
        {"id":36,"name":"osd.36","type":"osd","type_id":0,"crush_weight":1.089996,"depth":2,"exists":1,"status":"up","reweight":1.000000,"primary_affinity":1.000000},
        {"id":37,"name":"osd.37","type":"osd","type_id":0,"crush_weight":1.089996,"depth":2,"exists":1,"status":"up","reweight":1.000000,"primary_affinity":1.000000},
        {"id":38,"name":"osd.38","type":"osd","type_id":0,"crush_weight":1.089996,"depth":2,"exists":1,"status":"up","reweight":1.000000,"primary_affinity":1.000000},
        {"id":39,"name":"osd.39","type":"osd","type_id":0,"crush_weight":1.089996,"depth":2,"exists":1,"status":"up","reweight":1.000000,"primary_affinity":1.000000},
        {"id":40,"name":"osd.40","type":"osd","type_id":0,"crush_weight":1.089996,"depth":2,"exists":1,"status":"up","reweight":1.000000,"primary_affinity":1.000000},
        {"id":41,"name":"osd.41","type":"osd","type_id":0,"crush_weight":1.089996,"depth":2,"exists":1,"status":"up","reweight":1.000000,"primary_affinity":1.000000},
        {"id":42,"name":"osd.42","type":"osd","type_id":0,"crush_weight":1.089996,"depth":2,"exists":1,"status":"up","reweight":1.000000,"primary_affinity":1.000000},
        {"id":-8,"name":"OS-CS-06","type":"host","type_id":1,"children":[50,49,48,47,46,45,44,43]},
        {"id":43,"name":"osd.43","type":"osd","type_id":0,"crush_weight":1.089996,"depth":2,"exists":1,"status":"up","reweight":1.000000,"primary_affinity":1.000000},
        {"id":44,"name":"osd.44","type":"osd","type_id":0,"crush_weight":1.089996,"depth":2,"exists":1,"status":"up","reweight":1.000000,"primary_affinity":1.000000},
        {"id":45,"name":"osd.45","type":"osd","type_id":0,"crush_weight":1.089996,"depth":2,"exists":1,"status":"up","reweight":1.000000,"primary_affinity":1.000000},
        {"id":46,"name":"osd.46","type":"osd","type_id":0,"crush_weight":1.089996,"depth":2,"exists":1,"status":"up","reweight":1.000000,"primary_affinity":1.000000},
        {"id":47,"name":"osd.47","type":"osd","type_id":0,"crush_weight":1.089996,"depth":2,"exists":1,"status":"up","reweight":1.000000,"primary_affinity":1.000000},
        {"id":48,"name":"osd.48","type":"osd","type_id":0,"crush_weight":1.089996,"depth":2,"exists":1,"status":"up","reweight":1.000000,"primary_affinity":1.000000},
        {"id":49,"name":"osd.49","type":"osd","type_id":0,"crush_weight":1.089996,"depth":2,"exists":1,"status":"up","reweight":1.000000,"primary_affinity":1.000000},
        {"id":50,"name":"osd.50","type":"osd","type_id":0,"crush_weight":1.089996,"depth":2,"exists":1,"status":"up","reweight":1.000000,"primary_affinity":1.000000}
    ],"stray":[]}
"""
        osd_tree = utils.get_osd_tree('test')
        self.assertEqual(len(osd_tree), 10)
        self.assertEqual(osd_tree[0].identifier, -11)
        self.assertEqual(osd_tree[0].name, "OS-CS-08")
        self.assertEqual(osd_tree[-1].identifier, -2)
        self.assertEqual(osd_tree[-1].name, "OS-CS-05")
        self.assertEqual(osd_tree[0].root, "ssds")
        self.assertEqual(osd_tree[-1].root, "default")

    @patch.object(utils.subprocess, 'check_output')
    def test_get_osd_tree_multi_root_with_hierarchy(self, mock_check_output):
        mock_check_output.return_value = b'''{
    "nodes":[
        {"id":-1,"name":"default","type":"root","type_id":10,"children":[-3,-2]},
        {"id":-2,"name":"sata","type":"rack","type_id":3,"pool_weights":{},"children":[-5,-4]},
        {"id":-4,"name":"uat-l-stor-11","type":"host","type_id":1,"pool_weights":{},"children":[5,4,3,2,1,0]},
        {"id":0,"device_class":"hdd","name":"osd.0","type":"osd","type_id":0,"crush_weight":5.455994,"depth":3,"pool_weights":{},"exists":1,"status":"up","reweight":1.000000,"primary_affinity":1.000000},
        {"id":1,"device_class":"hdd","name":"osd.1","type":"osd","type_id":0,"crush_weight":5.455994,"depth":3,"pool_weights":{},"exists":1,"status":"up","reweight":1.000000,"primary_affinity":1.000000},
        {"id":2,"device_class":"hdd","name":"osd.2","type":"osd","type_id":0,"crush_weight":5.455994,"depth":3,"pool_weights":{},"exists":1,"status":"up","reweight":1.000000,"primary_affinity":1.000000},
        {"id":3,"device_class":"hdd","name":"osd.3","type":"osd","type_id":0,"crush_weight":5.455994,"depth":3,"pool_weights":{},"exists":1,"status":"up","reweight":1.000000,"primary_affinity":1.000000},
        {"id":4,"device_class":"hdd","name":"osd.4","type":"osd","type_id":0,"crush_weight":5.455994,"depth":3,"pool_weights":{},"exists":1,"status":"up","reweight":1.000000,"primary_affinity":1.000000},
        {"id":5,"device_class":"hdd","name":"osd.5","type":"osd","type_id":0,"crush_weight":5.455994,"depth":3,"pool_weights":{},"exists":1,"status":"up","reweight":1.000000,"primary_affinity":1.000000},
        {"id":-5,"name":"uat-l-stor-12","type":"host","type_id":1,"pool_weights":{},"children":[11,10,9,8,7,6]},
        {"id":6,"device_class":"hdd","name":"osd.6","type":"osd","type_id":0,"crush_weight":5.455994,"depth":3,"pool_weights":{},"exists":1,"status":"up","reweight":1.000000,"primary_affinity":1.000000},
        {"id":7,"device_class":"hdd","name":"osd.7","type":"osd","type_id":0,"crush_weight":5.455994,"depth":3,"pool_weights":{},"exists":1,"status":"up","reweight":1.000000,"primary_affinity":1.000000},
        {"id":8,"device_class":"hdd","name":"osd.8","type":"osd","type_id":0,"crush_weight":5.455994,"depth":3,"pool_weights":{},"exists":1,"status":"up","reweight":1.000000,"primary_affinity":1.000000},
        {"id":9,"device_class":"hdd","name":"osd.9","type":"osd","type_id":0,"crush_weight":5.455994,"depth":3,"pool_weights":{},"exists":1,"status":"up","reweight":1.000000,"primary_affinity":1.000000},
        {"id":10,"device_class":"hdd","name":"osd.10","type":"osd","type_id":0,"crush_weight":5.455994,"depth":3,"pool_weights":{},"exists":1,"status":"up","reweight":1.000000,"primary_affinity":1.000000},
        {"id":11,"device_class":"hdd","name":"osd.11","type":"osd","type_id":0,"crush_weight":5.455994,"depth":3,"pool_weights":{},"exists":1,"status":"up","reweight":1.000000,"primary_affinity":1.000000},
        {"id":-3,"name":"ssd","type":"rack","type_id":3,"pool_weights":{},"children":[-7,-6]},
        {"id":-6,"name":"uat-l-stor-13","type":"host","type_id":1,"pool_weights":{},"children":[17,16,15,14,13,12]},
        {"id":12,"name":"osd.12","type":"osd","type_id":0,"crush_weight":1.429993,"depth":3,"pool_weights":{},"exists":1,"status":"up","reweight":1.000000,"primary_affinity":1.000000},
        {"id":13,"name":"osd.13","type":"osd","type_id":0,"crush_weight":1.429993,"depth":3,"pool_weights":{},"exists":1,"status":"up","reweight":1.000000,"primary_affinity":1.000000},
        {"id":14,"name":"osd.14","type":"osd","type_id":0,"crush_weight":1.429993,"depth":3,"pool_weights":{},"exists":1,"status":"up","reweight":1.000000,"primary_affinity":1.000000},
        {"id":15,"name":"osd.15","type":"osd","type_id":0,"crush_weight":1.429993,"depth":3,"pool_weights":{},"exists":1,"status":"up","reweight":1.000000,"primary_affinity":1.000000},
        {"id":16,"name":"osd.16","type":"osd","type_id":0,"crush_weight":1.429993,"depth":3,"pool_weights":{},"exists":1,"status":"up","reweight":1.000000,"primary_affinity":1.000000},
        {"id":17,"name":"osd.17","type":"osd","type_id":0,"crush_weight":1.429993,"depth":3,"pool_weights":{},"exists":1,"status":"up","reweight":1.000000,"primary_affinity":1.000000},
        {"id":-7,"name":"uat-l-stor-14","type":"host","type_id":1,"pool_weights":{},"children":[23,22,21,20,19,18]},
        {"id":18,"name":"osd.18","type":"osd","type_id":0,"crush_weight":1.429993,"depth":3,"pool_weights":{},"exists":1,"status":"up","reweight":1.000000,"primary_affinity":1.000000},
        {"id":19,"name":"osd.19","type":"osd","type_id":0,"crush_weight":1.429993,"depth":3,"pool_weights":{},"exists":1,"status":"up","reweight":1.000000,"primary_affinity":1.000000},
        {"id":20,"name":"osd.20","type":"osd","type_id":0,"crush_weight":1.429993,"depth":3,"pool_weights":{},"exists":1,"status":"up","reweight":1.000000,"primary_affinity":1.000000},
        {"id":21,"name":"osd.21","type":"osd","type_id":0,"crush_weight":1.429993,"depth":3,"pool_weights":{},"exists":1,"status":"up","reweight":1.000000,"primary_affinity":1.000000},
        {"id":22,"name":"osd.22","type":"osd","type_id":0,"crush_weight":1.429993,"depth":3,"pool_weights":{},"exists":1,"status":"up","reweight":1.000000,"primary_affinity":1.000000},
        {"id":23,"name":"osd.23","type":"osd","type_id":0,"crush_weight":1.429993,"depth":3,"pool_weights":{},"exists":1,"status":"up","reweight":1.000000,"primary_affinity":1.000000}],
    "stray":[]}'''
        osd_tree = utils.get_osd_tree('test')
        self.assertEqual(len(osd_tree), 4)
        self.assertEqual(osd_tree[0].identifier, -7)
        self.assertEqual(osd_tree[0].name, "uat-l-stor-14")
        self.assertEqual(osd_tree[-1].identifier, -4)
        self.assertEqual(osd_tree[-1].name, "uat-l-stor-11")
        self.assertEqual(osd_tree[0].root, "default")
        self.assertEqual(osd_tree[-1].root, "default")
        self.assertEqual(osd_tree[0].rack, "ssd")
        self.assertEqual(osd_tree[-1].rack, "sata")

    @patch.object(utils.subprocess, 'check_output')
    def test_get_osd_tree_single_root(self, mock_check_output):
        mock_check_output.return_value = b"""{
    "nodes":[
        {"id":-1,"name":"default","type":"root","type_id":10,"children":[-4,-3,-2]},
        {"id":-2,"name":"juju-9d5cf0-icey-4","type":"host","type_id":1,"children":[0]},
        {"id":0,"name":"osd.0","type":"osd","type_id":0,"crush_weight":0.000092,"depth":2,"exists":1,"status":"up","reweight":1.000000,"primary_affinity":1.000000},
        {"id":-3,"name":"juju-9d5cf0-icey-6","type":"host","type_id":1,"children":[1]},
        {"id":1,"name":"osd.1","type":"osd","type_id":0,"crush_weight":0.000092,"depth":2,"exists":1,"status":"up","reweight":1.000000,"primary_affinity":1.000000},
        {"id":-4,"name":"juju-9d5cf0-icey-5","type":"host","type_id":1,"children":[2]},
        {"id":2,"name":"osd.2","type":"osd","type_id":0,"crush_weight":0.000092,"depth":2,"exists":1,"status":"up","reweight":1.000000,"primary_affinity":1.000000}],
    "stray":[]}"""
        osd_tree = utils.get_osd_tree('test')
        self.assertEqual(osd_tree[0].name, "juju-9d5cf0-icey-5")
        self.assertEqual(osd_tree[-1].name, "juju-9d5cf0-icey-4")
        self.assertEqual(osd_tree[0].root, "default")
        self.assertEqual(osd_tree[-1].root, "default")

    @patch.object(utils.subprocess, 'check_output')
    @patch.object(utils, "ceph_user", lambda: "ceph")
    @patch.object(utils.socket, "gethostname", lambda: "osd001")
    def test_get_named_key_with_pool(self, mock_check_output):
        mock_check_output.side_effect = [CalledProcessError(0, 0, 0), b""]
        utils.ceph_auth_get.cache_clear()
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
                  'mon', ('allow r; allow command "osd blacklist"'
                          '; allow command "osd blocklist"'),
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
                  'mon', ('allow r; allow command "osd blacklist"'
                          '; allow command "osd blocklist"'),
                  'osd', 'allow rwx'])])
        mock_check_output.reset_mock()
        mock_check_output.side_effect = [b'key=test']
        utils.get_named_key(name="rgw001")
        mock_check_output.assert_called_once_with([
            'sudo', '-u', 'ceph', 'ceph', '--name',
            'mon.', '--keyring',
            '/var/lib/ceph/mon/ceph-osd001/keyring',
            'auth', 'get', 'client.rgw001'])
        mock_check_output.reset_mock()
        utils.get_named_key(name="rgw001")
        mock_check_output.assert_not_called()

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

    def test_unmounted_devices_with_correct_input(self):
        dev1 = MagicMock(spec=TestDevice)
        dev1.__getitem__.return_value = "block"
        dev1.device_node = '/dev/sda'
        dev2 = MagicMock(spec=TestDevice)
        dev2.__getitem__.return_value = "block"
        dev2.device_node = '/dev/sdb'
        dev3 = MagicMock(spec=TestDevice)
        dev3.__getitem__.return_value = "block"
        dev3.device_node = '/dev/loop1'
        dev4 = MagicMock(spec=TestDevice)
        dev4.__getitem__.return_value = "block"
        dev4.device_node = '/dev/sdm'
        dev5 = MagicMock(spec=TestDevice)
        dev5.__getitem__.return_value = "block"
        dev5.device_node = '/dev/dm-1'
        input_data = [dev1, dev2, dev3, dev4, dev5]
        with patch(
                'pyudev.Context.list_devices',
                return_value=input_data):
            with patch.object(utils, 'is_device_mounted',
                              return_value=False):
                actual_output = utils.unmounted_disks()
                self.assertEqual(
                    actual_output,
                    ['/dev/sda', '/dev/sdb', '/dev/sdm']
                )
            with patch.object(utils, 'is_device_mounted',
                              return_value=True):
                actual_output = utils.unmounted_disks()
                self.assertEqual(actual_output, [])

    def test_unmounted_devices_doesnt_raise_with_incorrect_input(self):
        dev1 = MagicMock(spec=TestDevice)
        dev1.__getitem__.return_value = "block"
        dev1.device_node = '/dev/sda'
        dev2 = MagicMock(spec=TestDevice)
        dev2.__getitem__.return_value = "block"
        dev2.device_node = None
        input_data = [dev1, dev2]
        with patch(
                'pyudev.Context.list_devices',
                return_value=input_data):
            for is_device_mounted in (False, True):
                with patch.object(utils, 'is_device_mounted',
                                  return_value=is_device_mounted):
                    # No assertion, we just want to check that it doesn't raise
                    utils.unmounted_disks()

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

    @patch.object(utils, 'CompareHostReleases')
    def test_determine_packages(self, _cmp):
        _cmp().__str__.return_value = 'bionic'
        _cmp().__ge__.return_value = False
        self.assertEqual(utils.PACKAGES + ['btrfs-tools'],
                         utils.determine_packages())

    @patch.object(utils, '_create_monitor')
    @patch.object(utils, 'ceph_user')
    @patch.object(utils, 'os')
    @patch.object(utils, 'systemd')
    @patch.object(utils, 'mkdir')
    @patch.object(utils.socket, 'gethostname', lambda: 'TestHost')
    def test_bootstrap_monitor_cluster(self,
                                       mock_mkdir,
                                       mock_systemd,
                                       mock_os,
                                       mock_ceph_user,
                                       mock_create_monitor):
        test_hostname = utils.socket.gethostname()
        test_secret = 'mysecret'
        test_keyring = '/var/lib/ceph/tmp/{}.mon.keyring'.format(test_hostname)
        test_path = '/var/lib/ceph/mon/ceph-{}'.format(test_hostname)
        test_done = '{}/done'.format(test_path)
        test_init_marker = '{}/systemd'.format(test_path)

        mock_os.path.exists.return_value = False
        mock_systemd.return_value = True
        mock_ceph_user.return_value = 'ceph'

        utils.bootstrap_monitor_cluster(test_secret)

        mock_mkdir.assert_has_calls([
            call('/var/run/ceph', owner='ceph',
                 group='ceph', perms=0o755),
            call(test_path, owner='ceph', group='ceph',
                 perms=0o755),
        ])
        mock_create_monitor.assert_called_once_with(
            test_keyring,
            test_secret,
            test_hostname,
            test_path,
            test_done,
            test_init_marker,
        )
        mock_os.unlink.assert_called_with(test_keyring)

    @patch.object(utils, 'systemd')
    @patch.object(utils, 'chownr')
    @patch.object(utils, 'cmp_pkgrevno')
    @patch.object(utils, 'ceph_user')
    @patch.object(utils.subprocess, 'check_call')
    @patch.object(utils, 'service_restart')
    @patch.object(utils.socket, 'gethostname', lambda: 'TestHost')
    def _test_create_monitor(self,
                             mock_service_restart,
                             mock_check_call,
                             mock_ceph_user,
                             mock_cmp_pkgrevno,
                             mock_chownr,
                             mock_systemd,
                             nautilus=False):
        test_hostname = utils.socket.gethostname()
        test_secret = 'mysecret'
        test_keyring = '/var/lib/ceph/tmp/{}.mon.keyring'.format(test_hostname)
        test_path = '/var/lib/ceph/mon/ceph-{}'.format(test_hostname)
        test_done = '{}/done'.format(test_path)
        test_init_marker = '{}/systemd'.format(test_path)

        mock_systemd.return_value = True
        mock_cmp_pkgrevno.return_value = 1 if nautilus else -1
        mock_ceph_user.return_value = 'ceph'

        test_systemd_unit = (
            'ceph-mon@{}'.format(test_hostname) if nautilus else 'ceph-mon'
        )

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
            call(['systemctl', 'enable', test_systemd_unit])
        ]

        fake_open = mock_open()
        with patch('charms_ceph.utils.open', fake_open, create=True):
            utils._create_monitor(
                test_keyring,
                test_secret,
                test_hostname,
                test_path,
                test_done,
                test_init_marker
            )

        mock_check_call.assert_has_calls(test_calls)
        mock_service_restart.assert_called_with(test_systemd_unit)

    def test_create_monitor(self):
        self._test_create_monitor(nautilus=False)

    def test_create_monitor_nautilus(self):
        self._test_create_monitor(nautilus=True)

    @patch.object(utils, 'write_file')
    @patch.object(utils, 'cmp_pkgrevno')
    @patch.object(utils, 'ceph_user')
    @patch.object(utils, 'os')
    @patch.object(utils.subprocess, 'check_output')
    @patch.object(utils.subprocess, 'check_call')
    @patch.object(utils.socket, 'gethostname', lambda: 'TestHost')
    def _test_create_keyrings(self,
                              mock_check_call,
                              mock_check_output,
                              mock_os,
                              mock_ceph_user,
                              mock_cmp_pkgrevno,
                              mock_write_file,
                              ceph_version='10.0.0'):
        def _cmp_pkgrevno(_, version):
            # NOTE: this is fairly brittle as it just
            #       does direct string comparison for
            #       version checking
            if ceph_version == version:
                return 1
            else:
                return -1
        test_hostname = utils.socket.gethostname()

        mock_os.path.exists.return_value = False
        mock_cmp_pkgrevno.side_effect = _cmp_pkgrevno
        mock_ceph_user.return_value = 'ceph'
        mock_check_output.return_value = b'testkey'

        test_calls = []
        if ceph_version == '12.0.0':
            test_calls.append(
                call(['ceph-create-keys', '--id', test_hostname,
                      '--timeout', '1800'])
            )
        elif ceph_version == '10.0.0':
            test_calls.append(
                call(['ceph-create-keys', '--id', test_hostname])
            )

        utils.create_keyrings()

        mock_check_call.assert_has_calls(test_calls)

        if ceph_version == '14.0.0':
            mock_check_output.assert_called_once_with([
                'sudo',
                '-u', 'ceph',
                'ceph',
                '--name', 'mon.',
                '--keyring',
                '/var/lib/ceph/mon/ceph-{}/keyring'.format(
                    test_hostname
                ),
                'auth', 'get', 'client.admin',
            ])
            mock_write_file.assert_called_with(
                '/etc/ceph/ceph.client.admin.keyring',
                'testkey\n', group='ceph', owner='ceph',
                perms=0o400
            )
        else:
            mock_check_output.assert_not_called()
            mock_write_file.assert_not_called()

    def test_create_keyrings(self):
        self._test_create_keyrings()

    def test_create_keyrings_luminous(self):
        self._test_create_keyrings(ceph_version='12.0.0')

    def test_create_keyrings_nautilus(self):
        self._test_create_keyrings(ceph_version='14.0.0')

    @patch.object(utils, 'filter_missing_packages')
    @patch.object(utils, 'is_container')
    def test_determine_packages_to_remove_chrony(
            self,
            mock_is_container,
            mock_filter_missing_packages):

        packages_present = ["some", "random", "packages", utils.CHRONY_PACKAGE]
        packages_missing = ["some", "random", "packages"]
        chrony_is_present = True

        def _filter_missing_packages(query_installed_pkgs):
            pkgs = packages_present if chrony_is_present else packages_missing
            return [p for p in query_installed_pkgs if p in pkgs]

        mock_filter_missing_packages.side_effect = _filter_missing_packages

        # Scenarios to check:
        # 1. Not in a container.
        # 2. In a container and chrony is installed.
        # 3. In a container and chrony is not installed

        # verify that an array is passed: bug: #1929054
        # 1. first not in a container
        mock_is_container.return_value = False
        packages = utils.determine_packages_to_remove()
        self.assertNotIn(utils.CHRONY_PACKAGE, packages)
        mock_is_container.assert_called_once()
        mock_filter_missing_packages.assert_not_called()

        # 2. now in a container and chrony is installed
        mock_is_container.return_value = True
        packages = utils.determine_packages_to_remove()
        self.assertIn(utils.CHRONY_PACKAGE, packages)
        mock_filter_missing_packages.assert_called_once_with(
            [utils.CHRONY_PACKAGE])

        # 3. in a container and chrony is not installed
        chrony_is_present = False
        packages = utils.determine_packages_to_remove()
        self.assertNotIn(utils.CHRONY_PACKAGE, packages)

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
        with patch('charms_ceph.open', fake_open, create=True):
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
            'luminous -> mimic',
            'mimic -> nautilus',
            'nautilus -> octopus',
            'octopus -> pacific',
            'pacific -> quincy',
            'quincy -> reef',
            'reef -> squid',
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
            '--no-mon-config',
        ])

    def test_partition_name(self):
        self.assertEqual(utils._partition_name('/dev/sdb'),
                         '/dev/sdb1')
        self.assertEqual(utils._partition_name('/dev/mmcblk0'),
                         '/dev/mmcblk0p1')

    @patch.object(utils, 'get_named_key')
    def test_get_rbd_mirror_key(self, _get_named_key):
        utils.get_rbd_mirror_key('someid')
        _get_named_key.assert_called_once_with(
            name='someid',
            caps=collections.OrderedDict([
                ('mon', ['allow profile rbd-mirror-peer',
                         'allow command "service dump"',
                         'allow command "service status"']),
                ('osd', ['profile rbd']),
                ('mgr', ['allow r']),
            ])
        )

    @patch.object(utils.subprocess, 'check_output')
    def test_list_pools(self, _check_output):
        _check_output.return_value = 'poola\npoolb\n'
        self.assertEqual(utils.list_pools('someuser'), ['poola', 'poolb'])
        _check_output.assert_called_with(['rados', '--id', 'someuser',
                                          'lspools'], universal_newlines=True,
                                         stderr=subprocess.STDOUT)
        self.assertEqual(utils.list_pools(client='someotheruser'),
                         ['poola', 'poolb'])
        _check_output.assert_called_with(['rados', '--id', 'someotheruser',
                                          'lspools'], universal_newlines=True,
                                         stderr=subprocess.STDOUT)
        self.assertEqual(utils.list_pools(),
                         ['poola', 'poolb'])
        _check_output.assert_called_with(['rados', '--id', 'admin',
                                          'lspools'], universal_newlines=True,
                                         stderr=subprocess.STDOUT)

    @patch.object(utils.subprocess, 'check_output')
    def test_get_pool_param(self, _check_output):
        _check_output.return_value = 'size: 3\n'
        self.assertEqual(utils.get_pool_param('rbd', 'size'), '3')
        _check_output.assert_called_with(['ceph', '--id', 'admin', 'osd',
                                          'pool', 'get', 'rbd', 'size'],
                                         universal_newlines=True,
                                         stderr=subprocess.STDOUT)

    @patch.object(utils, 'get_pool_param')
    def test_get_pool_erasure_profile(self, _get_pool_param):
        _get_pool_param.side_effect = subprocess.CalledProcessError(
            13, [], 'EACCES: pool')
        self.assertEqual(utils.get_pool_erasure_profile('cinder-ceph'), None)
        _get_pool_param.side_effect = subprocess.CalledProcessError(
            22, [], 'EINVAL: invalid')
        with self.assertRaises(subprocess.CalledProcessError):
            utils.get_pool_erasure_profile('cinder-ceph')
        _get_pool_param.side_effect = None
        _get_pool_param.return_value = 'my-ec-profile'
        self.assertEqual(utils.get_pool_erasure_profile('cinder-ceph'),
                         'my-ec-profile')

    @patch.object(utils.subprocess, 'check_output')
    def test_get_pool_quota(self, _check_output):
        _check_output.return_value = (
            "quotas for pool 'rbd':\n"
            "  max objects: N/A\n"
            "  max bytes  : N/A\n")
        self.assertEqual(utils.get_pool_quota('rbd'),
                         {})
        _check_output.assert_called_with(['ceph', '--id', 'admin', 'osd',
                                          'pool', 'get-quota', 'rbd'],
                                         universal_newlines=True,
                                         stderr=subprocess.STDOUT)
        _check_output.return_value = (
            "quotas for pool 'rbd':\n"
            "  max objects: 10\n"
            "  max bytes  : N/A\n")
        self.assertEqual(utils.get_pool_quota('rbd'), {'max_objects': '10'})
        _check_output.return_value = (
            "quotas for pool 'rbd':\n"
            "  max objects: N/A\n"
            "  max bytes  : 1000B\n")
        self.assertEqual(utils.get_pool_quota('rbd'), {'max_bytes': '1000'})
        _check_output.return_value = (
            "quotas for pool 'rbd':\n"
            "  max objects: 10\n"
            "  max bytes  : 1000B\n")
        self.assertEqual(utils.get_pool_quota('rbd'),
                         {'max_objects': '10', 'max_bytes': '1000'})

    @patch.object(utils.subprocess, 'check_output')
    def test_get_pool_applications(self, _check_output):
        _check_output.return_value = (
            '{\n'
            '    "pool": {\n'
            '        "application": {}\n'
            '    }\n'
            '}\n')
        self.assertEqual(utils.get_pool_applications(),
                         {'pool': {'application': {}}})
        _check_output.assert_called_with(['ceph', '--id', 'admin', 'osd',
                                          'pool', 'application', 'get'],
                                         universal_newlines=True,
                                         stderr=subprocess.STDOUT)
        utils.get_pool_applications('42')
        _check_output.assert_called_with(['ceph', '--id', 'admin', 'osd',
                                          'pool', 'application', 'get', '42'],
                                         universal_newlines=True,
                                         stderr=subprocess.STDOUT)

    @patch.object(utils, 'get_pool_erasure_profile')
    @patch.object(utils, 'get_pool_param')
    @patch.object(utils, 'get_pool_quota')
    @patch.object(utils, 'list_pools')
    @patch.object(utils, 'get_pool_applications')
    def test_list_pools_detail(self, _get_pool_applications, _list_pools,
                               _get_pool_quota, _get_pool_param,
                               _get_pool_erasure_profile):
        self.assertEqual(utils.list_pools_detail(), {})
        _get_pool_applications.return_value = {'pool': {'application': {}}}
        _list_pools.return_value = ['pool', 'pool2']
        _get_pool_quota.return_value = {'max_objects': '10',
                                        'max_bytes': '1000'}
        _get_pool_param.return_value = '42'
        _get_pool_erasure_profile.return_value = 'my-ec-profile'
        self.assertEqual(
            utils.list_pools_detail(),
            {'pool': {'applications': {'application': {}},
                      'parameters': {'pg_num': '42',
                                     'size': '42',
                                     'erasure_code_profile': 'my-ec-profile'},
                      'quota': {'max_bytes': '1000',
                                'max_objects': '10'},
                      },
             'pool2': {'applications': {},
                       'parameters': {'pg_num': '42',
                                      'size': '42',
                                      'erasure_code_profile': 'my-ec-profile'},
                       'quota': {'max_bytes': '1000',
                                 'max_objects': '10'},
                       },
             })


class CephApplyOSDSettingsTestCase(unittest.TestCase):

    def setUp(self):
        super(CephApplyOSDSettingsTestCase, self).setUp()
        self.base_cmd = 'ceph daemon osd.{osd_id} config --format=json'
        self.get_cmd = self.base_cmd + ' get {key}'
        self.set_cmd = self.base_cmd + ' set {key} {value}'
        self.grace = 'osd_heartbeat_grace'
        self.interval = 'osd_heartbeat_interval'

    @patch.object(utils.subprocess, 'check_output')
    @patch.object(utils.os.path, 'exists')
    @patch.object(utils.os, 'listdir')
    @patch.object(utils, 'filesystem_mounted')
    def test_osd_ids_with_crimson(self, fs_mounted, listdir,
                                  path_exists, check_output):
        check_output.return_value = b'38271 /usr/bin/crimson-osd -i 5\n'
        listdir.return_value = ['ceph-3', 'ceph-5']
        self.assertEqual(['3'], utils.get_local_osd_ids())

    @patch.object(utils, 'get_local_osd_ids')
    @patch.object(utils.subprocess, 'check_output')
    def test_apply_osd_settings(self, _check_output, _get_local_osd_ids):
        _get_local_osd_ids.return_value = ['0']
        output = {
            self.get_cmd.format(osd_id=0, key=self.grace):
                b'{"osd_heartbeat_grace":"19"}',
            self.set_cmd.format(osd_id=0, key=self.grace, value='21'):
                b"""{"success":"osd_heartbeat_grace = '21'"}"""}
        _check_output.side_effect = lambda x: output[' '.join(x)]
        self.assertTrue(
            utils.apply_osd_settings({'osd heartbeat grace': '21'}))
        check_output_calls = [
            call(['ceph', 'daemon', 'osd.0', 'config', '--format=json', 'get',
                  'osd_heartbeat_grace']),
            call(['ceph', 'daemon', 'osd.0', 'config', '--format=json', 'set',
                  'osd_heartbeat_grace', '21'])]
        _check_output.assert_has_calls(check_output_calls)
        self.assertTrue(_check_output.call_count == len(check_output_calls))

    @patch.object(utils, 'get_local_osd_ids')
    @patch.object(utils.subprocess, 'check_output')
    def test_apply_osd_settings_noop_on_one_osd(self, _check_output,
                                                _get_local_osd_ids):
        _get_local_osd_ids.return_value = ['0', '1']
        output = {
            self.get_cmd.format(osd_id=0, key=self.grace):
                b'{"osd_heartbeat_grace":"21"}',
            self.get_cmd.format(osd_id=1, key=self.grace):
                b'{"osd_heartbeat_grace":"20"}',
            self.set_cmd.format(osd_id=1, key=self.grace, value='21'):
                b"""{"success":"osd_heartbeat_interval = '2'"}"""}
        _check_output.side_effect = lambda x: output[' '.join(x)]
        self.assertTrue(
            utils.apply_osd_settings({'osd heartbeat grace': '21'}))
        check_output_calls = [
            call(['ceph', 'daemon', 'osd.0', 'config', '--format=json', 'get',
                  'osd_heartbeat_grace']),
            call(['ceph', 'daemon', 'osd.1', 'config', '--format=json', 'get',
                  'osd_heartbeat_grace']),
            call(['ceph', 'daemon', 'osd.1', 'config', '--format=json', 'set',
                  'osd_heartbeat_grace', '21'])]
        _check_output.assert_has_calls(check_output_calls)
        self.assertTrue(_check_output.call_count == len(check_output_calls))

    @patch.object(utils, 'get_local_osd_ids')
    @patch.object(utils.subprocess, 'check_output')
    def _test_apply_osd_settings_error(self, _check_output,
                                       _get_local_osd_ids):
        _get_local_osd_ids.return_value = ['0']
        output = {
            self.get_cmd.format(osd_id=0, key=self.grace):
                b"""{"error":"error setting 'osd_heartbeat_grace'"}"""}
        _check_output.side_effect = lambda x: output[' '.join(x)]
        self.assertFalse(
            utils.apply_osd_settings({'osd heartbeat grace': '21'}))
        check_output_calls = [
            call(['ceph', 'daemon', 'osd.0', 'config', '--format=json', 'get',
                  'osd_heartbeat_grace'])]
        _check_output.assert_has_calls(check_output_calls)
        self.assertTrue(_check_output.call_count == len(check_output_calls))

    @patch.object(utils, 'get_local_osd_ids')
    @patch.object(utils.subprocess, 'check_output')
    def test_apply_osd_settings_error(self, _check_output,
                                      _get_local_osd_ids):
        _get_local_osd_ids.return_value = ['0', '1']
        output = {
            self.get_cmd.format(osd_id=0, key=self.grace):
                b'{"osd_heartbeat_grace":"19"}',
            self.get_cmd.format(osd_id=0, key=self.interval):
                b'{"osd_heartbeat_interval":"3"}',
            self.set_cmd.format(osd_id=0, key=self.interval, value='2'):
                b"""{"success":"osd_heartbeat_interval = '2'"}""",
            self.set_cmd.format(osd_id=0, key=self.grace, value='21'):
                b"""{"error":"error setting 'osd_heartbeat_grace'"}"""}
        _check_output.side_effect = lambda x: output[' '.join(x)]
        check_output_calls = [
            call(['ceph', 'daemon', 'osd.0', 'config', '--format=json', 'get',
                  'osd_heartbeat_grace']),
            call(['ceph', 'daemon', 'osd.0', 'config', '--format=json', 'get',
                  'osd_heartbeat_interval']),
            call(['ceph', 'daemon', 'osd.0', 'config', '--format=json', 'set',
                  'osd_heartbeat_grace', '21'])]
        with self.assertRaises(utils.OSDConfigSetError):
            utils.apply_osd_settings({
                'osd heartbeat grace': '21',
                'osd heartbeat interval': '2'})
            _check_output.assert_has_calls(check_output_calls)
            self.assertTrue(
                _check_output.call_count == len(check_output_calls))


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
        _check_call.assert_has_calls([
            call(['vaultlocker', 'encrypt',
                  '--uuid', 'test-UUID',
                  '/dev/sdb']),
            call(['dd', 'if=/dev/zero',
                  'of=/dev/mapper/crypt-test-UUID',
                  'bs=512', 'count=1']),
        ])

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
                             encrypt=False),
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
                             encrypt=False),
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
                               encrypt=False),
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
                               encrypt=False),
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
        with patch('charms_ceph.utils.open', fake_open):
            result = utils.is_pristine_disk('/dev/vdz')
        fake_open.assert_called_with('/dev/vdz', 'rb')
        self.assertFalse(_log.called)
        self.assertEqual(result, True)

    @patch.object(utils, 'log')
    def test_is_pristine_disk_oserror(self, _log):
        fake_open = mock_open()
        oserror_exception = OSError('error')
        fake_open.side_effect = oserror_exception
        with patch('charms_ceph.utils.open', fake_open):
            result = utils.is_pristine_disk('/dev/sr0')
        fake_open.assert_called_with('/dev/sr0', 'rb')
        _log.assert_called_with(oserror_exception)
        self.assertEqual(result, False)

    @patch.object(utils, 'WARNING')
    @patch.object(utils, 'log')
    def test_is_pristine_disk_short_read(self, _log, _level_WRN):
        data = b'\0' * 2047
        fake_open = mock_open(read_data=data)
        with patch('charms_ceph.utils.open', fake_open):
            result = utils.is_pristine_disk('/dev/vdz')
        fake_open.assert_called_with('/dev/vdz', 'rb')
        _log.assert_called_with(
            '/dev/vdz: short read, got 2047 bytes expected 2048.',
            level=_level_WRN)
        self.assertEqual(result, False)

    def test_is_pristine_disk_dirty_disk(self):
        data = b'\0' * 2047
        data = data + b'\42'
        fake_open = mock_open(read_data=data)
        with patch('charms_ceph.utils.open', fake_open):
            result = utils.is_pristine_disk('/dev/vdz')
        fake_open.assert_called_with('/dev/vdz', 'rb')
        self.assertEqual(result, False)


class CephManagerAndConfig(unittest.TestCase):

    MODULE_OUT = b"""
{
    "enabled_modules": [
        "dashboard",
        "iostat",
        "restful"
    ]}"""

    @patch.object(utils, 'cmp_pkgrevno')
    @patch.object(utils.subprocess, 'check_output')
    def test_enabled_manager_modules_pre_quincy(
            self, _check_output, _cmp_pkgrevno):
        _check_output.return_value = self.MODULE_OUT
        _cmp_pkgrevno.return_value = -1
        utils.enabled_manager_modules()
        _check_output.assert_called_once_with(['ceph', 'mgr', 'module', 'ls'])

    @patch.object(utils, 'cmp_pkgrevno')
    @patch.object(utils.subprocess, 'check_output')
    def test_enabled_manager_modules_quincy(
            self, _check_output, _cmp_pkgrevno):
        _check_output.return_value = self.MODULE_OUT
        _cmp_pkgrevno.return_value = 0
        utils.enabled_manager_modules()
        _check_output.assert_called_once_with(
            ['ceph', 'mgr', 'module', 'ls', '--format=json'])

    @patch.object(utils, 'cmp_pkgrevno')
    @patch.object(utils.subprocess, 'check_output')
    def test_enabled_manager_modules(self, _check_output, _cmp_pkgrevno):
        _cmp_pkgrevno.return_value = -1
        _check_output.return_value = self.MODULE_OUT
        self.assertEqual(
            utils.enabled_manager_modules(),
            ['dashboard', 'iostat', 'restful'])

    @patch.object(utils, 'enabled_manager_modules')
    def test_is_mgr_module_enabled(self, _enabled_manager_modules):
        _enabled_manager_modules.return_value = ['dashboard', 'restful']
        self.assertTrue(
            utils.is_mgr_module_enabled('dashboard'))
        self.assertFalse(
            utils.is_mgr_module_enabled('ostrich'))

    @patch.object(utils, 'is_mgr_module_enabled')
    @patch.object(utils.subprocess, 'check_call')
    def test_mgr_enable_module(self, _check_call, _is_mgr_module_enabled):
        _is_mgr_module_enabled.return_value = True
        utils.mgr_enable_module('dashboard')
        self.assertFalse(_check_call.called)
        _is_mgr_module_enabled.return_value = False
        utils.mgr_enable_module('dashboard')
        _check_call.assert_called_once_with(
            ['ceph', 'mgr', 'module', 'enable', 'dashboard'])

    @patch.object(utils, 'is_mgr_module_enabled')
    @patch.object(utils.subprocess, 'check_call')
    def test_mgr_disable_module(self, _check_call, _is_mgr_module_enabled):
        _is_mgr_module_enabled.return_value = False
        utils.mgr_disable_module('dashboard')
        self.assertFalse(_check_call.called)
        _is_mgr_module_enabled.return_value = True
        utils.mgr_disable_module('dashboard')
        _check_call.assert_called_once_with(
            ['ceph', 'mgr', 'module', 'disable', 'dashboard'])

    @patch.object(utils.subprocess, 'check_call')
    def test_ceph_config_set(self, _check_call):
        utils.ceph_config_set('mgr/dashboard/ssl', 'true', 'mgr')
        _check_call.assert_called_once_with(
            ['ceph', 'config', 'set', 'mgr', 'mgr/dashboard/ssl', 'true'])

    @patch.object(utils.subprocess, 'check_output')
    def test_ceph_config_get(self, _check_output):
        utils.ceph_config_get('mgr/dashboard/ssl', 'mgr')
        _check_output.assert_called_once_with(
            ['ceph', 'config', 'get', 'mgr', 'mgr/dashboard/ssl'])
