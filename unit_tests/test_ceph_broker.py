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

import json
import mock
import unittest

import ceph_broker

from mock import call


class CephBrokerTestCase(unittest.TestCase):
    def setUp(self):
        super(CephBrokerTestCase, self).setUp()

    @mock.patch('ceph_broker.log')
    def test_process_requests_noop(self, mock_log):
        req = json.dumps({'api-version': 1, 'ops': []})
        rc = ceph_broker.process_requests(req)
        self.assertEqual(json.loads(rc), {'exit-code': 0})

    @mock.patch('ceph_broker.log')
    def test_process_requests_missing_api_version(self, mock_log):
        req = json.dumps({'ops': []})
        rc = ceph_broker.process_requests(req)
        self.assertEqual(json.loads(rc), {
            'exit-code': 1,
            'stderr': 'Missing or invalid api version (None)'})

    @mock.patch('ceph_broker.log')
    def test_process_requests_invalid_api_version(self, mock_log):
        req = json.dumps({'api-version': 2, 'ops': []})
        rc = ceph_broker.process_requests(req)
        self.assertEqual(json.loads(rc),
                         {'exit-code': 1,
                          'stderr': 'Missing or invalid api version (2)'})

    @mock.patch('ceph_broker.log')
    def test_process_requests_invalid(self, mock_log):
        reqs = json.dumps({'api-version': 1, 'ops': [{'op': 'invalid_op'}]})
        rc = ceph_broker.process_requests(reqs)
        self.assertEqual(json.loads(rc),
                         {'exit-code': 1,
                          'stderr': "Unknown operation 'invalid_op'"})

    @mock.patch('ceph_broker.ReplicatedPool')
    @mock.patch('ceph_broker.pool_exists')
    @mock.patch('ceph_broker.log')
    def test_process_requests_create_pool_w_pg_num(self, mock_log,
                                                   mock_pool_exists,
                                                   mock_replicated_pool):
        mock_pool_exists.return_value = False
        reqs = json.dumps({'api-version': 1,
                           'ops': [{
                               'op': 'create-pool',
                               'name': 'foo',
                               'replicas': 3,
                               'pg_num': 100}]})
        rc = ceph_broker.process_requests(reqs)
        mock_pool_exists.assert_called_with(service='admin', name='foo')
        mock_replicated_pool.assert_called_with(service='admin', name='foo',
                                                replicas=3, pg_num=100)
        self.assertEqual(json.loads(rc), {'exit-code': 0})

    @mock.patch('ceph_broker.ReplicatedPool')
    @mock.patch('ceph_broker.pool_exists')
    @mock.patch('ceph_broker.log')
    def test_process_requests_create_pool_exists(self, mock_log,
                                                 mock_pool_exists,
                                                 mock_replicated_pool):
        mock_pool_exists.return_value = True
        reqs = json.dumps({'api-version': 1,
                           'ops': [{'op': 'create-pool',
                                    'name': 'foo',
                                    'replicas': 3}]})
        rc = ceph_broker.process_requests(reqs)
        mock_pool_exists.assert_called_with(service='admin',
                                            name='foo')
        self.assertFalse(mock_replicated_pool.create.called)
        self.assertEqual(json.loads(rc), {'exit-code': 0})

    @mock.patch('ceph_broker.ReplicatedPool')
    @mock.patch('ceph_broker.pool_exists')
    @mock.patch('ceph_broker.log')
    def test_process_requests_create_pool_rid(self, mock_log,
                                              mock_pool_exists,
                                              mock_replicated_pool):
        mock_pool_exists.return_value = False
        reqs = json.dumps({'api-version': 1,
                           'request-id': '1ef5aede',
                           'ops': [{
                               'op': 'create-pool',
                               'name': 'foo',
                               'replicas': 3}]})
        rc = ceph_broker.process_requests(reqs)
        mock_pool_exists.assert_called_with(service='admin', name='foo')
        mock_replicated_pool.assert_called_with(service='admin',
                                                name='foo',
                                                replicas=3)
        self.assertEqual(json.loads(rc)['exit-code'], 0)
        self.assertEqual(json.loads(rc)['request-id'], '1ef5aede')

    @mock.patch('ceph_broker.get_cephfs')
    @mock.patch('ceph_broker.check_output')
    @mock.patch('ceph_broker.pool_exists')
    @mock.patch('ceph_broker.log')
    def test_process_requests_create_cephfs(self,
                                            mock_log,
                                            mock_pool_exists,
                                            check_output,
                                            get_cephfs):
        get_cephfs.return_value = []
        mock_pool_exists.return_value = True
        reqs = json.dumps({'api-version': 1,
                           'request-id': '1ef5aede',
                           'ops': [{
                               'op': 'create-cephfs',
                               'mds_name': 'foo',
                               'data_pool': 'data',
                               'metadata_pool': 'metadata',
                           }]})
        rc = ceph_broker.process_requests(reqs)
        mock_pool_exists.assert_has_calls(
            [
                call(service='admin', name='data'),
                call(service='admin', name='metadata'),
            ])
        check_output.assert_called_with(["ceph",
                                         '--id', 'admin',
                                         "fs", "new", 'foo',
                                         'metadata',
                                         'data'])

        self.assertEqual(json.loads(rc)['exit-code'], 0)
        self.assertEqual(json.loads(rc)['request-id'], '1ef5aede')

    @mock.patch('ceph_broker.check_output')
    @mock.patch('ceph_helpers.Crushmap.load_crushmap')
    @mock.patch('ceph_helpers.Crushmap.ensure_bucket_is_present')
    @mock.patch('ceph_broker.get_osd_weight')
    @mock.patch('ceph_broker.log')
    def test_process_requests_move_osd(self,
                                       mock_log,
                                       get_osd_weight,
                                       ensure_bucket_is_present,
                                       load_crushmap,
                                       check_output):
        load_crushmap.return_value = ""
        ensure_bucket_is_present.return_value = None
        get_osd_weight.return_value = 1
        reqs = json.dumps({'api-version': 1,
                           'request-id': '1ef5aede',
                           'ops': [{
                               'op': 'move-osd-to-bucket',
                               'osd': 'osd.0',
                               'bucket': 'test'
                           }]})
        rc = ceph_broker.process_requests(reqs)
        check_output.assert_called_with(["ceph",
                                         '--id', 'admin',
                                         "osd", "crush", "set",
                                         u"osd.0", "1", "root=test"])

        self.assertEqual(json.loads(rc)['exit-code'], 0)
        self.assertEqual(json.loads(rc)['request-id'], '1ef5aede')

    @mock.patch('ceph_broker.log')
    def test_process_requests_invalid_api_rid(self, mock_log):
        reqs = json.dumps({'api-version': 0, 'request-id': '1ef5aede',
                           'ops': [{'op': 'create-pool'}]})
        rc = ceph_broker.process_requests(reqs)
        self.assertEqual(json.loads(rc)['exit-code'], 1)
        self.assertEqual(json.loads(rc)['stderr'],
                         "Missing or invalid api version (0)")
        self.assertEqual(json.loads(rc)['request-id'], '1ef5aede')
