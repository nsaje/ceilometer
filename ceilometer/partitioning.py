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


class PartitionCoordinator:
    """Workload partitioning coordinator.

    This class wraps the `tooz` library functionality and is used by the
    `Partition` decorator.

    Before the decorator can be used, a new instance of this class *MUST* be
    created and set up using the `set_partition_coordinator` function.

    To ensure that the other agents know we're alive, the `heartbeat` method
    should be called periodically.
    """
    # TODO(nsaje): handle timeouts gracefully

    def __init__(self, conf=None, our_id=None):
        self._coordinator = None
        self._groups = set()
        self._our_id = our_id or str(uuid.uuid4())
        conf = conf or cfg.CONF
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
                self._groups.add(group_id)
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

    def _get_members(self, group_id):
        if self._coordinator:
            get_members_req = self._coordinator.get_members(group_id)
            return get_members_req.get()
        else:
            return [self._our_id]

    def get_our_subset(self, group_id, iterable):
        if group_id not in self._groups:
            self.join_group(group_id)
        members = self._get_members(group_id)
        our_key = sorted(members).index(self.our_id)
        return [v for v in iterable
                if int(hashlib.md5(str(v)).hexdigest(), 16)
                % len(members) == our_key]
