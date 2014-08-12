#
# Copyright 2014 Red Hat, Inc.
#
# Author: Nejc Saje <nsaje@redhat.com>
#
# Licensed under the Apache License, Version 2.0 (the "License"); you may
# not use this file except in compliance with the License. You may obtain
# a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
# WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
# License for the specific language governing permissions and limitations
# under the License.

import functools
import hashlib

from oslo.config import cfg
import tooz.coordination
import uuid

from ceilometer.openstack.common import log

LOG = log.getLogger(__name__)

OPTS = [
    cfg.StrOpt('coordination_backend_url',
               default=None,
               help='The backend URL to use for distributed coordination'),
    cfg.FloatOpt('coordination_heartbeat',
                 default=0.1,
                 help='The time between heartbeats for distributed '
                      'coordination')
]
cfg.CONF.register_opts(OPTS)

_partition_coordinator = None


def set_partition_coordinator(partition_coordinator):
    global _partition_coordinator
    _partition_coordinator = partition_coordinator


class PartitionCoordinator:
    """Workload partitioning coordinator.

    This class wraps the `tooz` library functionality and is used by the
    `Partition` decorator.

    Before the decorator can be used, a new instance of this class *MUST* be
    created and set up using the `set_partition_coordinator` function.

    To ensure that the other agents know we're alive, the `heartbeat` method
    should be called periodically.
    """
    #TODO (nsaje): handle timeouts gracefully

    def __init__(self, conf, our_id=None):
        self._coordinator = None
        self._our_id = our_id or str(uuid.uuid4())
        if conf.coordination_backend_url:
            self._coordinator = tooz.coordination.get_coordinator(
                conf.coordination_backend_url, self._our_id)
            self._coordinator.start()

    @property
    def our_id(self):
        return self._our_id

    def heartbeat(self):
        if self._coordinator:
            self._coordinator.heartbeat()

    def join_group(self, group_id):
        if not self._coordinator:
            return
        while True:
            join_req = self._coordinator.join_group(group_id)
            try:
                join_req.get()
                break
            except tooz.coordination.MemberAlreadyExist:
                return
            except tooz.coordination.GroupNotCreated:
                create_grp_req = self._coordinator.create_group(group_id)
                try:
                    create_grp_req.get()
                except tooz.coordination.GroupAlreadyExist:
                    pass
                continue

    def get_members(self, group_id):
        if self._coordinator:
            get_members_req = self._coordinator.get_members(group_id)
            return get_members_req.get()
        else:
            return [self._our_id]


class NoPartitionCoordinator(Exception):
    pass


class Partition:
    """Workload partitioning decorator.

    Before using the decorator, a new instance of `PartitionCoordinator` must
    be created and set up via the `set_partition_coordinator` function.
    """
    def __init__(self, group_id):
        self.group_id = group_id
        if not _partition_coordinator:
            raise NoPartitionCoordinator('Partition coordinator not set! '
                                         'Before using the decorator, '
                                         'set up a new instance of '
                                         '`PartitionCoordinator`.')
        _partition_coordinator.join_group(group_id)

    def __call__(self, func):
        """Filters an iterable, returning only objects assigned to us.

        We have a list of objects and get a list of active group members from
        `tooz`. We then hash all the objects into buckets and return only
        the ones that hashed into *our* bucket.
        """
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            obj = func(*args, **kwargs)
            if hasattr(obj, '__iter__'):
                members = _partition_coordinator.get_members(self.group_id)
                our_key = sorted(members).index(_partition_coordinator.our_id)
                return [v for v in obj
                        if int(hashlib.md5(str(v)).hexdigest(), 16)
                        % len(members) == our_key]
            else:
                return obj
        return wrapper
