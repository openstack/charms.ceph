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
import unittest

from unittest.mock import patch, ANY

import charms_ceph.broker

from unittest.mock import call


class CephBrokerTestCase(unittest.TestCase):
    def setUp(self):
        super(CephBrokerTestCase, self).setUp()

    @patch.object(charms_ceph.broker, 'check_call')
    def test_update_service_permission(self, _check_call):
        service_obj = {
            'group_names': {'rwx': ['images']},
            'groups': {'images': {'pools': ['cinder'], 'services': ['nova']}}
        }
        charms_ceph.broker.update_service_permissions(service='nova',
                                                      service_obj=service_obj)
        _check_call.assert_called_with(
            ['ceph', 'auth', 'caps',
             'client.nova',
             'mon', 'allow r, allow command "osd blacklist"',
             'osd', 'allow rwx pool=cinder'])

    @patch.object(charms_ceph.broker, 'check_call')
    @patch.object(charms_ceph.broker, 'get_service_groups')
    @patch.object(charms_ceph.broker, 'monitor_key_set')
    @patch.object(charms_ceph.broker, 'monitor_key_get')
    def test_add_pool_to_existing_group_with_services(self,
                                                      _monitor_key_get,
                                                      _monitor_key_set,
                                                      _get_service_groups,
                                                      _check_call):
        _monitor_key_get.return_value = '{"pools": ["glance"],'\
            ' "services": ["nova"]}'
        service = {
            'group_names': {'rwx': ['images']},
            'groups': {'images': {'pools': [
                'glance', 'cinder'
            ], 'services': ['nova']}}
        }
        _get_service_groups.return_value = service
        charms_ceph.broker.add_pool_to_group(
            pool="cinder",
            group="images"
        )
        _monitor_key_set.assert_called_with(
            key='cephx.groups.images',
            service='admin',
            value=json.dumps({"pools": ["glance", "cinder"],
                              "services": ["nova"]}, sort_keys=True))
        _check_call.assert_called_with([
            'ceph', 'auth', 'caps',
            'client.nova',
            'mon', 'allow r, allow command "osd blacklist"',
            'osd', 'allow rwx pool=glance, allow rwx pool=cinder'])

    @patch.object(charms_ceph.broker, 'monitor_key_set')
    @patch.object(charms_ceph.broker, 'monitor_key_get')
    def test_add_pool_to_existing_group(self,
                                        _monitor_key_get,
                                        _monitor_key_set):
        _monitor_key_get.return_value = '{"pools": ["glance"], "services": []}'
        charms_ceph.broker.add_pool_to_group(
            pool="cinder",
            group="images"
        )
        _monitor_key_set.assert_called_with(
            key='cephx.groups.images',
            service='admin',
            value=json.dumps({"pools": ["glance", "cinder"], "services": []},
                             sort_keys=True))

    @patch.object(charms_ceph.broker, 'monitor_key_set')
    @patch.object(charms_ceph.broker, 'monitor_key_get')
    def test_add_pool_to_new_group(self,
                                   _monitor_key_get,
                                   _monitor_key_set):
        _monitor_key_get.return_value = '{"pools": [], "services": []}'
        charms_ceph.broker.add_pool_to_group(
            pool="glance",
            group="images"
        )
        _monitor_key_set.assert_called_with(
            key='cephx.groups.images',
            service='admin',
            value=json.dumps({"pools": ["glance"], "services": []},
                             sort_keys=True))

    @patch.object(charms_ceph.broker, 'handle_set_key_permissions')
    @patch.object(charms_ceph.broker, 'log')
    def test_process_requests_set_perms(self, mock_log,
                                        handle_set_key_permissions):
        request = {
            "api-version": 1,
            "request-id": "0155c14b",
            "ops": [
                {
                    "client": "manila-ganesha",
                    "op": "set-key-permissions",
                    "permissions": [
                        "mds 'allow *'",
                        "osd 'allow rw'",
                    ]
                }
            ]
        }
        reqs = json.dumps(request)
        rc = charms_ceph.broker.process_requests(reqs)
        handle_set_key_permissions.assert_called_once_with(
            request={
                u'client': u'manila-ganesha',
                u'op': u'set-key-permissions',
                u'permissions': [
                    u"mds 'allow *'",
                    u"osd 'allow rw'",
                ]},
            service='admin')
        self.assertEqual(
            json.loads(rc),
            {'exit-code': 0, u'request-id': u'0155c14b'})

    @patch.object(charms_ceph.broker, 'check_call')
    def test_handle_set_key_permissions(self, _check_call):
        charms_ceph.broker.handle_set_key_permissions(
            request={
                u'client': u'manila-ganesha',
                u'op': u'set-key-permissions',
                u'permissions': [
                    u"mds 'allow *'",
                    u"osd 'allow rw'",
                ]},
            service='admin')
        expected = ['ceph', '--id', 'admin', 'auth', 'caps',
                    'client.manila-ganesha', "mds 'allow *'", "osd 'allow rw'"]
        _check_call.assert_called_once_with(expected)

    @patch.object(charms_ceph.broker, 'check_call')
    def test_set_key_permission(self, _check_call):
        request = {
            u'client': u'manila-ganesha',
            u'op': u'set-key-permissions',
            u'permissions': [
                u"mds 'allow *'",
                u"osd 'allow rw'",
            ]}
        service = 'admin'
        charms_ceph.broker.handle_set_key_permissions(request=request,
                                                      service=service)
        _check_call.assert_called_once_with([
            'ceph',
            '--id', 'admin',
            'auth', 'caps',
            'client.manila-ganesha', "mds 'allow *'", "osd 'allow rw'"])

    def test_pool_permission_list_for_service(self):
        service = {
            'group_names': {'rwx': ['images']},
            'groups': {'images': {'pools': ['glance'], 'services': ['nova']}}
        }
        result = charms_ceph.broker.pool_permission_list_for_service(service)
        self.assertEqual(result, ['mon',
                                  'allow r, allow command "osd blacklist"',
                                  'osd',
                                  'allow rwx pool=glance'])

    def test_pool_permission_list_for_service_multi(self):
        service = {
            'group_names': {'rwx': ['images', 'group1'], 'r': ['group2']},
            'groups': {
                'images': {
                    'pools': ['glance'],
                    'services': ['nova']},
                'group1': {
                    'pools': ['p1'],
                    'services': ['svc1']},
                'group2': {
                    'pools': ['p2'],
                    'services': ['svc2']}}
        }
        result = charms_ceph.broker.pool_permission_list_for_service(service)
        self.assertEqual(
            result,
            [
                'mon',
                'allow r, allow command "osd blacklist"',
                'osd',
                'allow r pool=p2, allow rwx pool=glance, allow rwx pool=p1'])

    @patch.object(charms_ceph.broker, 'monitor_key_set')
    def test_save_service(self, _monitor_key_set):
        service = {
            'group_names': {'rwx': 'images'},
            'groups': {'images': {'pools': ['glance'], 'services': ['nova']}}
        }
        charms_ceph.broker.save_service(service=service, service_name='nova')
        _monitor_key_set.assert_called_with(
            value=json.dumps(service, sort_keys=True),
            key='cephx.services.nova',
            service='admin')

    @patch.object(charms_ceph.broker, 'monitor_key_get')
    def test_get_service_groups_empty(self, _monitor_key_get):
        _monitor_key_get.return_value = None
        service = charms_ceph.broker.get_service_groups('nova')
        _monitor_key_get.assert_called_with(
            key='cephx.services.nova',
            service='admin'
        )
        self.assertEqual(service, {'group_names': {}, 'groups': {}})

    @patch.object(charms_ceph.broker, 'monitor_key_get')
    def test_get_service_groups_empty_str(self, _monitor_key_get):
        _monitor_key_get.return_value = ''
        service = charms_ceph.broker.get_service_groups('nova')
        _monitor_key_get.assert_called_with(
            key='cephx.services.nova',
            service='admin'
        )
        self.assertEqual(service, {'group_names': {}, 'groups': {}})

    @patch.object(charms_ceph.broker, 'get_group')
    @patch.object(charms_ceph.broker, 'monitor_key_get')
    def test_get_service_groups(self, _monitor_key_get, _get_group):
        _monitor_key_get.return_value = '{"group_names": {"rwx": ["images"]}' \
            ',"groups": {}}'
        _get_group.return_value = {
            'pools': ["glance"],
            'services': ['nova']
        }
        service = charms_ceph.broker.get_service_groups('nova')
        _monitor_key_get.assert_called_with(
            key='cephx.services.nova',
            service='admin'
        )
        self.assertEqual(service, {
            'group_names': {'rwx': ['images']},
            'groups': {'images': {'pools': ['glance'], 'services': ['nova']}}
        })

    @patch.object(charms_ceph.broker, 'monitor_key_set')
    def test_save_group(self, _monitor_key_set):
        group = {
            'pools': ["glance"],
            'services': []
        }
        charms_ceph.broker.save_group(group=group, group_name='images')
        _monitor_key_set.assert_called_with(
            key='cephx.groups.images',
            service='admin',
            value=json.dumps(group, sort_keys=True))

    @patch.object(charms_ceph.broker, 'monitor_key_get')
    def test_get_group_empty_str(self, _monitor_key_get):
        _monitor_key_get.return_value = ''
        group = charms_ceph.broker.get_group('images')
        self.assertEqual(group, {
            'pools': [],
            'services': []
        })

    @patch.object(charms_ceph.broker, 'monitor_key_get')
    def test_get_group_empty(self, _monitor_key_get):
        _monitor_key_get.return_value = None
        group = charms_ceph.broker.get_group('images')
        self.assertEqual(group, {
            'pools': [],
            'services': []
        })

    @patch.object(charms_ceph.broker, 'monitor_key_get')
    def test_get_group(self, _monitor_key_get):
        _monitor_key_get.return_value = '{"pools": ["glance"], "services": []}'
        group = charms_ceph.broker.get_group('images')
        self.assertEqual(group, {
            'pools': ["glance"],
            'services': []
        })

    @patch.object(charms_ceph.broker, 'log')
    def test_process_requests_noop(self, mock_log):
        req = json.dumps({'api-version': 1, 'ops': []})
        rc = charms_ceph.broker.process_requests(req)
        self.assertEqual(json.loads(rc), {'exit-code': 0})

    @patch.object(charms_ceph.broker, 'log')
    def test_process_requests_missing_api_version(self, mock_log):
        req = json.dumps({'ops': []})
        rc = charms_ceph.broker.process_requests(req)
        self.assertEqual(json.loads(rc), {
            'exit-code': 1,
            'stderr': 'Missing or invalid api version (None)'})

    @patch.object(charms_ceph.broker, 'log')
    def test_process_requests_invalid_api_version(self, mock_log):
        req = json.dumps({'api-version': 2, 'ops': []})
        rc = charms_ceph.broker.process_requests(req)
        self.assertEqual(json.loads(rc),
                         {'exit-code': 1,
                          'stderr': 'Missing or invalid api version (2)'})

    @patch.object(charms_ceph.broker, 'log')
    def test_process_requests_invalid(self, mock_log):
        reqs = json.dumps({'api-version': 1, 'ops': [{'op': 'invalid_op'}]})
        rc = charms_ceph.broker.process_requests(reqs)
        self.assertEqual(json.loads(rc),
                         {'exit-code': 1,
                          'stderr': "Unknown operation 'invalid_op'"})

    @patch.object(charms_ceph.broker, 'get_osds')
    @patch.object(charms_ceph.broker, 'ReplicatedPool')
    @patch.object(charms_ceph.broker, 'pool_exists')
    @patch.object(charms_ceph.broker, 'log')
    def test_process_requests_create_pool_w_pg_num(self, mock_log,
                                                   mock_pool_exists,
                                                   mock_replicated_pool,
                                                   mock_get_osds):
        mock_pool_exists.return_value = False
        mock_get_osds.return_value = [0, 1, 2]
        op = {
            'op': 'create-pool',
            'name': 'foo',
            'replicas': 3,
            'pg_num': 100,
        }
        reqs = json.dumps({'api-version': 1,
                           'ops': [op]})
        rc = charms_ceph.broker.process_requests(reqs)
        mock_replicated_pool.assert_called_with(service='admin', op=op)
        mock_pool_exists.assert_called_with(service='admin', name='foo')
        self.assertEqual(json.loads(rc), {'exit-code': 0})

    @patch.object(charms_ceph.broker, 'ReplicatedPool')
    @patch.object(charms_ceph.broker, 'pool_exists')
    @patch.object(charms_ceph.broker, 'log')
    @patch.object(charms_ceph.broker, 'add_pool_to_group')
    def test_process_requests_create_pool_w_group(self, add_pool_to_group,
                                                  mock_log, mock_pool_exists,
                                                  mock_replicated_pool):
        mock_pool_exists.return_value = False
        op = {
            'op': 'create-pool',
            'name': 'foo',
            'replicas': 3,
            'group': 'image',
        }
        reqs = json.dumps({'api-version': 1,
                           'ops': [op]})
        rc = charms_ceph.broker.process_requests(reqs)
        add_pool_to_group.assert_called_with(group='image',
                                             pool='foo',
                                             namespace=None)
        mock_pool_exists.assert_called_with(service='admin', name='foo')
        mock_replicated_pool.assert_called_with(service='admin', op=op)
        self.assertEqual(json.loads(rc), {'exit-code': 0})

    @patch.object(charms_ceph.broker, 'ReplicatedPool')
    @patch.object(charms_ceph.broker, 'pool_exists')
    @patch.object(charms_ceph.broker, 'log')
    def test_process_requests_create_pool_exists(self, mock_log,
                                                 mock_pool_exists,
                                                 mock_replicated_pool):
        mock_pool_exists.return_value = True

        op = {
            'op': 'create-pool',
            'name': 'foo',
            'replicas': 3,
        }
        reqs = json.dumps({'api-version': 1,
                           'ops': [op]})
        rc = charms_ceph.broker.process_requests(reqs)
        mock_pool_exists.assert_called_with(service='admin',
                                            name='foo')
        self.assertFalse(mock_replicated_pool.create.called)
        self.assertEqual(json.loads(rc), {'exit-code': 0})

    @patch.object(charms_ceph.broker, 'ReplicatedPool')
    @patch.object(charms_ceph.broker, 'pool_exists')
    @patch.object(charms_ceph.broker, 'log')
    def test_process_requests_create_pool_rid(self, mock_log,
                                              mock_pool_exists,
                                              mock_replicated_pool):
        mock_pool_exists.return_value = False
        op = {
            'op': 'create-pool',
            'name': 'foo',
            'replicas': 3,
        }
        reqs = json.dumps({'api-version': 1,
                           'request-id': '1ef5aede',
                           'ops': [op]})
        rc = charms_ceph.broker.process_requests(reqs)
        mock_replicated_pool.assert_called_with(service='admin', op=op)
        mock_pool_exists.assert_called_with(service='admin', name='foo')
        self.assertEqual(json.loads(rc)['exit-code'], 0)
        self.assertEqual(json.loads(rc)['request-id'], '1ef5aede')

    @patch.object(charms_ceph.broker, 'erasure_profile_exists')
    @patch.object(charms_ceph.broker, 'ErasurePool')
    @patch.object(charms_ceph.broker, 'pool_exists')
    @patch.object(charms_ceph.broker, 'log')
    def test_process_requests_create_erasure_pool(self, mock_log,
                                                  mock_pool_exists,
                                                  mock_erasure_pool,
                                                  mock_profile_exists):
        mock_pool_exists.return_value = False
        op = {
            'op': 'create-pool',
            'pool-type': 'erasure',
            'name': 'foo',
            'erasure-profile': 'default'
        }
        reqs = json.dumps({'api-version': 1,
                           'ops': [op]})
        rc = charms_ceph.broker.process_requests(reqs)
        mock_profile_exists.assert_called_with(service='admin', name='default')
        mock_erasure_pool.assert_called_with(service='admin', op=op)
        mock_pool_exists.assert_called_with(service='admin', name='foo')
        self.assertEqual(json.loads(rc), {'exit-code': 0})

    @patch.object(charms_ceph.broker, 'pool_exists')
    @patch.object(charms_ceph.broker, 'BasePool')
    @patch.object(charms_ceph.broker, 'log', lambda *args, **kwargs: None)
    def test_process_requests_create_cache_tier(self, mock_pool,
                                                mock_pool_exists):
        mock_pool_exists.return_value = True
        op = {
            'op': 'create-cache-tier',
            'cold-pool': 'foo',
            'hot-pool': 'foo-ssd',
            'mode': 'writeback',
            'erasure-profile': 'default'
        }
        reqs = json.dumps({'api-version': 1,
                           'ops': [op]})
        rc = charms_ceph.broker.process_requests(reqs)
        mock_pool_exists.assert_any_call(service='admin', name='foo')
        mock_pool_exists.assert_any_call(service='admin', name='foo-ssd')

        mock_pool().add_cache_tier.assert_called_with(
            cache_pool='foo-ssd', mode='writeback')
        self.assertEqual(json.loads(rc), {'exit-code': 0})

    @patch.object(charms_ceph.broker, 'get_cephfs')
    @patch.object(charms_ceph.broker, 'check_output')
    @patch.object(charms_ceph.broker, 'pool_exists')
    @patch.object(charms_ceph.broker, 'log')
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
        rc = charms_ceph.broker.process_requests(reqs)
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

    @patch.object(charms_ceph.broker, 'get_cephfs')
    @patch.object(charms_ceph.broker, 'check_output')
    @patch.object(charms_ceph.broker, 'pool_exists')
    @patch.object(charms_ceph.broker, 'log')
    def test_process_requests_create_cephfs_ec(self,
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
                               'extra_pools': ['ec_pool'],
                               'data_pool': 'data',
                               'metadata_pool': 'metadata',
                           }]})
        rc = charms_ceph.broker.process_requests(reqs)
        mock_pool_exists.assert_has_calls(
            [
                call(service='admin', name='data'),
                call(service='admin', name='ec_pool'),
                call(service='admin', name='metadata'),
            ],
            any_order=True)
        check_output.assert_has_calls(
            [
                call(['ceph', '--id', 'admin', 'fs', 'new', 'foo', 'metadata',
                      'data']),
                call(['ceph', '--id', 'admin', 'fs', 'add_data_pool', 'foo',
                      'ec_pool'])])
        self.assertEqual(json.loads(rc)['exit-code'], 0)
        self.assertEqual(json.loads(rc)['request-id'], '1ef5aede')

    @patch.object(charms_ceph.broker, 'check_output')
    @patch.object(charms_ceph.broker, 'get_osd_weight')
    @patch.object(charms_ceph.broker, 'log')
    @patch('charms_ceph.crush_utils.Crushmap.load_crushmap')
    @patch('charms_ceph.crush_utils.Crushmap.ensure_bucket_is_present')
    def test_process_requests_move_osd(self,
                                       mock_ensure_bucket_is_present,
                                       mock_load_crushmap,
                                       mock_log,
                                       mock_get_osd_weight,
                                       mock_check_output):
        mock_load_crushmap.return_value = ""
        mock_ensure_bucket_is_present.return_value = None
        mock_get_osd_weight.return_value = 1
        reqs = json.dumps({'api-version': 1,
                           'request-id': '1ef5aede',
                           'ops': [{
                               'op': 'move-osd-to-bucket',
                               'osd': 'osd.0',
                               'bucket': 'test'
                           }]})
        rc = charms_ceph.broker.process_requests(reqs)
        self.assertEqual(json.loads(rc)['exit-code'], 0)
        self.assertEqual(json.loads(rc)['request-id'], '1ef5aede')
        mock_check_output.assert_called_with(["ceph",
                                              '--id', 'admin',
                                              "osd", "crush", "set",
                                              "osd.0", "1", "root=test"])

    @patch.object(charms_ceph.broker, 'log')
    def test_process_requests_invalid_api_rid(self, mock_log):
        reqs = json.dumps({'api-version': 0, 'request-id': '1ef5aede',
                           'ops': [{'op': 'create-pool'}]})
        rc = charms_ceph.broker.process_requests(reqs)
        self.assertEqual(json.loads(rc)['exit-code'], 1)
        self.assertEqual(json.loads(rc)['stderr'],
                         "Missing or invalid api version (0)")
        self.assertEqual(json.loads(rc)['request-id'], '1ef5aede')

    @patch.object(charms_ceph.broker, 'handle_add_permissions_to_key')
    @patch.object(charms_ceph.broker, 'log')
    def test_process_requests_add_perms(self, mock_log,
                                        mock_handle_add_permissions_to_key):
        request = {
            "api-version": 1,
            "request-id": "0155c14b",
            "ops": [
                {
                    "namespace": None,
                    "group-permission": "rwx",
                    "group": "images",
                    "name": "glance",
                    "op": "add-permissions-to-key"
                }
            ]
        }
        reqs = json.dumps(request)
        rc = charms_ceph.broker.process_requests(reqs)
        mock_handle_add_permissions_to_key.assert_called_once_with(
            request={
                u'namespace': None,
                u'group-permission': u'rwx',
                u'group': u'images',
                u'name': u'glance',
                u'op': u'add-permissions-to-key'},
            service='admin')
        self.assertEqual(
            json.loads(rc),
            {'exit-code': 0, u'request-id': u'0155c14b'})

    @patch.object(charms_ceph.broker, 'handle_add_permissions_to_key')
    @patch.object(charms_ceph.broker, 'log')
    def test_process_requests_add_multi_perms(self, mock_log,
                                              mock_handle_add_perms_to_key):
        request = {
            "api-version": 1,
            "request-id": "0155c14b",
            "ops": [
                {
                    "namespace": None,
                    "group-permission": "rwx",
                    "group": "images",
                    "name": "glance",
                    "op": "add-permissions-to-key"
                },
                {
                    "namespace": None,
                    "group-permission": "r",
                    "group": "volumes",
                    "name": "cinder",
                    "op": "add-permissions-to-key"
                }
            ]
        }
        reqs = json.dumps(request)
        rc = charms_ceph.broker.process_requests(reqs)
        call1 = call(
            request={
                u'namespace': None,
                u'group-permission': u'rwx',
                u'group': u'images',
                u'name': u'glance',
                u'op': u'add-permissions-to-key'},
            service='admin')
        call2 = call(
            request={
                u'namespace': None,
                u'group-permission': u'r',
                u'group': u'volumes',
                u'name': u'cinder',
                u'op': u'add-permissions-to-key'},
            service='admin')
        mock_handle_add_perms_to_key.assert_has_calls([call1, call2])
        self.assertEqual(
            json.loads(rc),
            {'exit-code': 0, u'request-id': u'0155c14b'})

    @patch.object(charms_ceph.broker, 'save_service')
    @patch.object(charms_ceph.broker, 'save_group')
    @patch.object(charms_ceph.broker, 'monitor_key_get')
    @patch.object(charms_ceph.broker, 'update_service_permissions')
    def test_handle_add_permissions_to_key(self,
                                           mock_update_service_permissions,
                                           mock_monitor_key_get,
                                           mock_save_group,
                                           mock_save_service):
        mkey = {
            'cephx.services.glance': ('{"groups": {}, '
                                      '"group_names": {"rwx": ["images"]}}'),
            'cephx.groups.images': ('{"services": ["glance", "cinder-ceph", '
                                    '"nova-compute"], "pools": ["glance"]}')}
        mock_monitor_key_get.side_effect = lambda service, key: mkey[key]
        expect_service_name = u'glance'
        expected_group = {
            u'services': [
                u'glance',
                u'cinder-ceph',
                u'nova-compute'],
            u'pools': [u'glance']}
        expect_service_obj = {
            u'groups': {
                u'images': expected_group},
            u'group_names': {
                u'rwx': [u'images']}}
        expect_group_namespace = None
        charms_ceph.broker.handle_add_permissions_to_key(
            request={
                u'namespace': None,
                u'group-permission': u'rwx',
                u'group': u'images',
                u'name': u'glance',
                u'op': u'add-permissions-to-key'},
            service='admin')
        mock_save_group.assert_called_once_with(
            group=expected_group,
            group_name='images')
        mock_save_service.assert_called_once_with(
            service=expect_service_obj,
            service_name=expect_service_name)
        mock_update_service_permissions.assert_called_once_with(
            expect_service_name,
            expect_service_obj,
            expect_group_namespace)

    @patch.object(charms_ceph.broker, 'save_service')
    @patch.object(charms_ceph.broker, 'save_group')
    @patch.object(charms_ceph.broker, 'monitor_key_get')
    @patch.object(charms_ceph.broker, 'update_service_permissions')
    def test_handle_add_permissions_to_key_obj_prefs(self,
                                                     mock_update_serv_perms,
                                                     mock_monitor_key_get,
                                                     mock_save_group,
                                                     mock_save_service):
        mkey = {
            'cephx.services.glance': ('{"groups": {}, "group_names": '
                                      '{"rwx": ["images"]}}'),
            'cephx.groups.images': ('{"services": ["glance", "cinder-ceph", '
                                    '"nova-compute"], "pools": ["glance"]}')}
        mock_monitor_key_get.side_effect = lambda service, key: mkey[key]
        expect_service_name = u'glance'
        expected_group = {
            u'services': [
                u'glance',
                u'cinder-ceph',
                u'nova-compute'],
            u'pools': [u'glance']}
        expect_service_obj = {
            u'groups': {
                u'images': expected_group},
            u'group_names': {
                u'rwx': [u'images']},
            u'object_prefix_perms': {
                u'rwx': [u'rbd_children'], u'r': ['another']}}
        expect_group_namespace = None
        charms_ceph.broker.handle_add_permissions_to_key(
            request={
                u'namespace': None,
                u'group-permission': u'rwx',
                u'group': u'images',
                u'name': u'glance',
                u'object-prefix-permissions': {
                    u'rwx': [u'rbd_children'], u'r': ['another']},
                u'op': u'add-permissions-to-key'},
            service='admin')
        mock_save_group.assert_called_once_with(
            group=expected_group,
            group_name='images')
        mock_save_service.assert_called_once_with(
            service=expect_service_obj,
            service_name=expect_service_name)
        mock_update_serv_perms.assert_called_once_with(
            expect_service_name,
            expect_service_obj,
            expect_group_namespace)

    def test_pool_permission_list_for_service_obj_pref(self):
        expected_group = {
            u'services': [
                u'glance',
                u'cinder-ceph',
                u'nova-compute'],
            u'pools': [u'glance']}
        expect_service_obj = {
            u'groups': {
                u'images': expected_group},
            u'group_names': {
                u'rwx': [u'images']},
            u'object_prefix_perms': {
                u'rwx': [u'rbd_children'], u'r': ['another']}}
        self.assertEqual(charms_ceph.broker.pool_permission_list_for_service(
            expect_service_obj),
            [
                'mon',
                'allow r, allow command "osd blacklist"',
                'osd',
                ('allow rwx pool=glance, '
                 'allow r object_prefix another, '
                 'allow rwx object_prefix rbd_children')])

    def test_pool_permission_list_for_glance(self):
        expected_group = {
            u'services': [
                u'glance',
                u'cinder-ceph',
                u'nova-compute'],
            u'pools': [u'glance']}
        expect_service_obj = {
            u'groups': {
                u'images': expected_group},
            u'group_names': {
                u'rwx': [u'images']},
            u'object_prefix_perms': {
                u'class-read': [u'rbd_children']}}
        self.assertEqual(charms_ceph.broker.pool_permission_list_for_service(
            expect_service_obj),
            [
                'mon',
                'allow r, allow command "osd blacklist"',
                'osd',
                ('allow rwx pool=glance, '
                 'allow class-read object_prefix rbd_children')])

    @patch.object(charms_ceph.broker, 'create_erasure_profile')
    def test_handle_create_erasure_profile(self,
                                           mock_create_erasure_profile):
        request = {
            'erasure-type': 'jerasure',
            'erasure-technique': 'reed_sol',
            'name': 'newprofile',
            'failure-domain': 'rack',
            'k': 4,
            'm': 2,
            'l': 3,
            'c': 5,
            'd': 9,
            'scalar-mds': 'isa',
            'crush-locality': 'host',
            'device-class': 'ssd',
        }
        resp = charms_ceph.broker.handle_create_erasure_profile(
            request,
            'testservice'
        )
        self.assertEqual(resp, {'exit-code': 0})

        mock_create_erasure_profile.assert_called_once_with(
            service='testservice',
            erasure_plugin_name='jerasure',
            profile_name='newprofile',
            failure_domain='rack',
            data_chunks=4,
            coding_chunks=2,
            locality=3,
            durability_estimator=9,
            helper_chunks=5,
            scalar_mds='isa',
            crush_locality='host',
            device_class='ssd',
            erasure_plugin_technique='reed_sol'
        )

    @patch.object(charms_ceph.broker, 'create_erasure_profile')
    def test_handle_create_erasure_profile_bad(self,
                                               mock_create_erasure_profile):
        request = {
            'erasure-type': 'jerasure',
            'erasure-technique': 'reed_sol',
            'name': 'newprofile',
            'failure-domain': 'foobar',
            'k': 4,
            'm': 2,
            'l': None,
        }
        resp = charms_ceph.broker.handle_create_erasure_profile(
            request,
            'testservice'
        )
        self.assertEqual(
            resp,
            {
                'exit-code': 1,
                'stderr': ANY,
            }
        )
        mock_create_erasure_profile.assert_not_called()
