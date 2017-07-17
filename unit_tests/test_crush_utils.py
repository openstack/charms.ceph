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

import unittest

import ceph.crush_utils

from mock import patch


CRUSHMAP1 = """# begin crush map
tunable choose_local_tries 0
tunable choose_local_fallback_tries 0
tunable choose_total_tries 50
tunable chooseleaf_descend_once 1
tunable chooseleaf_vary_r 1
tunable straw_calc_version 1

# devices
device 0 osd.0
device 1 osd.1
device 2 osd.2

# types
type 0 osd
type 1 host
type 2 chassis
type 3 rack
type 4 row
type 5 pdu
type 6 pod
type 7 room
type 8 datacenter
type 9 region
type 10 root

# buckets
host ip-172-31-33-152 {
    id -2        # do not change unnecessarily
    # weight 0.003
    alg straw
    hash 0    # rjenkins1
    item osd.0 weight 0.003
}
host ip-172-31-54-117 {
    id -3        # do not change unnecessarily
    # weight 0.003
    alg straw
    hash 0    # rjenkins1
    item osd.1 weight 0.003
}
host ip-172-31-30-0 {
    id -4        # do not change unnecessarily
    # weight 0.003
    alg straw
    hash 0    # rjenkins1
    item osd.2 weight 0.003
}
root default {
    id -1        # do not change unnecessarily
    # weight 0.009
    alg straw
    hash 0    # rjenkins1
    item ip-172-31-33-152 weight 0.003
    item ip-172-31-54-117 weight 0.003
    item ip-172-31-30-0 weight 0.003
}

# rules
rule replicated_ruleset {
    ruleset 0
    type replicated
    min_size 1
    max_size 10
    step take default
    step chooseleaf firstn 0 type host
    step emit
}

# end crush map"""

CRUSHMAP2 = """# begin crush map
tunable choose_local_tries 0
tunable choose_local_fallback_tries 0
tunable choose_total_tries 50
tunable chooseleaf_descend_once 1
tunable chooseleaf_vary_r 1
tunable straw_calc_version 1

# devices
device 0 osd.0
device 1 osd.1
device 2 osd.2

# types
type 0 osd
type 1 host
type 2 chassis
type 3 rack
type 4 row
type 5 pdu
type 6 pod
type 7 room
type 8 datacenter
type 9 region
type 10 root

# buckets
host ip-172-31-33-152 {
    id -2        # do not change unnecessarily
    # weight 0.003
    alg straw
    hash 0    # rjenkins1
    item osd.0 weight 0.003
}
host ip-172-31-54-117 {
    id -3        # do not change unnecessarily
    # weight 0.003
    alg straw
    hash 0    # rjenkins1
    item osd.1 weight 0.003
}
host ip-172-31-30-0 {
    id -4        # do not change unnecessarily
    # weight 0.003
    alg straw
    hash 0    # rjenkins1
    item osd.2 weight 0.003
}
root default {
    id -1        # do not change unnecessarily
    # weight 0.009
    alg straw
    hash 0    # rjenkins1
    item ip-172-31-33-152 weight 0.003
    item ip-172-31-54-117 weight 0.003
    item ip-172-31-30-0 weight 0.003
}

# rules
rule replicated_ruleset {
    ruleset 0
    type replicated
    min_size 1
    max_size 10
    step take default
    step chooseleaf firstn 0 type host
    step emit
}

# end crush map"""

CRUSHMAP3 = """# begin crush map
tunable choose_local_tries 0
tunable choose_local_fallback_tries 0
tunable choose_total_tries 50
tunable chooseleaf_descend_once 1
tunable chooseleaf_vary_r 1
tunable straw_calc_version 1

# devices
device 0 osd.0
device 1 osd.1
device 2 osd.2

# types
type 0 osd
type 1 host
type 2 chassis
type 3 rack
type 4 row
type 5 pdu
type 6 pod
type 7 room
type 8 datacenter
type 9 region
type 10 root

# buckets
host ip-172-31-33-152 {
    id -2        # do not change unnecessarily
    # weight 0.003
    alg straw
    hash 0    # rjenkins1
    item osd.0 weight 0.003
}
host ip-172-31-54-117 {
    id -3        # do not change unnecessarily
    # weight 0.003
    alg straw
    hash 0    # rjenkins1
    item osd.1 weight 0.003
}
host ip-172-31-30-0 {
    id -4        # do not change unnecessarily
    # weight 0.003
    alg straw
    hash 0    # rjenkins1
    item osd.2 weight 0.003
}
root default {
    id -1        # do not change unnecessarily
    # weight 0.009
    alg straw
    hash 0    # rjenkins1
    item ip-172-31-33-152 weight 0.003
    item ip-172-31-54-117 weight 0.003
    item ip-172-31-30-0 weight 0.003
}

# rules
rule replicated_ruleset {
    ruleset 0
    type replicated
    min_size 1
    max_size 10
    step take default
    step chooseleaf firstn 0 type host
    step emit
}

# end crush map

root test {
    id -5    # do not change unnecessarily
    # weight 0.000
    alg straw
    hash 0  # rjenkins1
}

rule test {
    ruleset 0
    type replicated
    min_size 1
    max_size 10
    step take test
    step chooseleaf firstn 0 type host
    step emit
}"""


CRUSHMAP4 = """root fast {
    id -21    # do not change unnecessarily
    # weight 0.000
    alg straw
    hash 0  # rjenkins1
}

rule fast {
    ruleset 0
    type replicated
    min_size 1
    max_size 10
    step take fast
    step chooseleaf firstn 0 type host
    step emit
}"""


class CephCrushmapTests(unittest.TestCase):
    def setUp(self):
        super(CephCrushmapTests, self).setUp()

    @patch.object(ceph.crush_utils.Crushmap, 'load_crushmap')
    def test_crushmap_buckets(self, load_crushmap):
        load_crushmap.return_value = ""
        crushmap = ceph.crush_utils.Crushmap()
        crushmap.add_bucket("test")
        self.assertEqual(
            crushmap.buckets(), [ceph.crush_utils.CRUSHBucket("test", -1)])

    @patch.object(ceph.crush_utils.Crushmap, 'load_crushmap')
    def test_parsed_crushmap(self, load_crushmap):
        load_crushmap.return_value = CRUSHMAP1
        crushmap = ceph.crush_utils.Crushmap()
        self.assertEqual(
            [ceph.crush_utils.CRUSHBucket("default", -1, True)],
            crushmap.buckets())
        self.assertEqual([-4, -3, -2, -1], crushmap._ids)

    @patch.object(ceph.crush_utils.Crushmap, 'load_crushmap')
    def test_build_crushmap(self, load_crushmap):
        load_crushmap.return_value = CRUSHMAP2
        expected = CRUSHMAP3
        crushmap = ceph.crush_utils.Crushmap()
        crushmap.add_bucket("test")
        self.assertEqual(expected, crushmap.build_crushmap())

    def test_crushmap_string(self):
        result = ceph.crush_utils.Crushmap.bucket_string("fast", -21)
        expected = CRUSHMAP4
        self.assertEqual(expected, result)
