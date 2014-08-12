#
# Copyright 2014 Red Hat, Inc.
#
# Author: Nejc Saje <nsaje@redhat.com>
#
# Licensed under the Apache License, Version 2.0 (the "License"); you may
# not use this file except in compliance with the License. You may obtain
# a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
# WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
# License for the specific language governing permissions and limitations
# under the License.

import mock
import hashlib

import eventlet
from eventlet.green import threading
import tooz.coordination

from ceilometer.openstack.common import test
from ceilometer import partitioning


class FakeToozCoordinator():
    def __init__(self, member_id, shared_storage):
        self._member_id = member_id
        self._groups = shared_storage

    def start(self):
        pass

    def heartbeat(self):
        pass

    def create_group(self, group_id):
        if group_id in self._groups:
            return FakeAsyncError(
                tooz.coordination.GroupAlreadyExist(group_id))
        self._groups[group_id] = {}
        return FakeAsyncResult(None)

    def join_group(self, group_id, capabilities=b''):
        if group_id not in self._groups:
            return FakeAsyncError(
                tooz.coordination.GroupNotCreated(group_id))
        if self._member_id in self._groups[group_id]:
            return FakeAsyncError(
                tooz.coordination.MemberAlreadyExist(group_id,
                                                     self._member_id))
        self._groups[group_id][self._member_id] = {
            "capabilities": capabilities,
        }
        return FakeAsyncResult(None)

    def get_members(self, group_id):
        if group_id not in self._groups:
            return FakeAsyncError(
                tooz.coordination.GroupNotCreated(group_id))
        return FakeAsyncResult(self._groups[group_id])


class FakeAsyncResult(tooz.coordination.CoordAsyncResult):
    def __init__(self, result):
        self.result = result

    def get(self, timeout=0):
        return self.result

    @staticmethod
    def done():
        return True


class FakeAsyncError(tooz.coordination.CoordAsyncResult):
    def __init__(self, error):
        self.error = error

    def get(self, timeout=0):
        raise self.error

    @staticmethod
    def done():
        return True


class WaitGroup:
    def __init__(self):
        self.count = 0
        self.evt = threading.Event()
        self.evt.set()

    def add(self):
        self.count += 1
        if self.evt.is_set():
            self.evt.clear()

    def done(self):
        if self.count > 0:
            self.count -= 1
            if self.count == 0:
                self.evt.set()

    def wait(self):
        self.evt.wait()


class TestPartitioning(test.BaseTestCase):

    def setUp(self):
        super(TestPartitioning, self).setUp()
        self.shared_storage = {}
        self.wait_group_init = WaitGroup()
        self.wait_group_finish = WaitGroup()

    @staticmethod
    def get_new_coordinator(shared_storage, agent_id=None):
        with mock.patch('tooz.coordination.get_coordinator',
                        lambda _, member_id:
                        FakeToozCoordinator(member_id, shared_storage)):
            return partitioning.PartitionCoordinator(mock.MagicMock(),
                                                     agent_id)

    def _usage_simulation(self, *args, **kwargs):
        def worker(agent_id, group_id, all_resources=None,
                   expected_resources=None):
            partition_coordinator = self.get_new_coordinator(
                self.shared_storage, agent_id)
            with mock.patch('ceilometer.partitioning._partition_coordinator',
                            partition_coordinator):
                @partitioning.Partition(group_id=group_id)
                def discover():
                    return all_resources
            if expected_resources is None:
                self.wait_group_finish.done()
                return
            # wait for other 'agents' to initialize
            self.wait_group_init.done()
            self.wait_group_init.wait()
            # re-patch after wait() because the patching is lost when switching
            with mock.patch('ceilometer.partitioning._partition_coordinator',
                            partition_coordinator):
                try:
                    actual_resources = discover()
                    self.assertEqual(expected_resources, actual_resources,
                                     agent_id)
                finally:
                    self.wait_group_finish.done()

        self.wait_group_init.add()
        self.wait_group_finish.add()
        return eventlet.spawn(worker, *args, **kwargs)

    def test_single_group(self):
        self._usage_simulation('agent1', 'group')
        self._usage_simulation('agent2', 'group')
        self.wait_group_finish.wait()

        self.assertEqual(sorted(self.shared_storage.keys()), ['group'])
        self.assertEqual(sorted(self.shared_storage['group'].keys()),
                         ['agent1', 'agent2'])

    def test_multiple_groups(self):
        self._usage_simulation('agent1', 'group1')
        self._usage_simulation('agent2', 'group2')
        self.wait_group_finish.wait()

        self.assertEqual(sorted(self.shared_storage.keys()), ['group1',
                                                              'group2'])

    def test_partitioning(self):
        all_resources = ['resource_%s' % i for i in range(1000)]
        agents = ['agent_%s' % i for i in range(10)]

        expected_resources = [list() for _ in range(len(agents))]
        for r in all_resources:
            key = int(hashlib.md5(str(r)).hexdigest(), 16) % len(agents)
            expected_resources[key].append(r)

        for i, agent in enumerate(agents):
            kwargs = dict(agent_id=agent,
                          group_id='group',
                          all_resources=all_resources,
                          expected_resources=expected_resources[i])
            self._usage_simulation(**kwargs)
        self.wait_group_finish.wait()













