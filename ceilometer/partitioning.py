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
import collections

import eventlet
from oslo.config import cfg
import tooz
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


class GroupCoordinator:
    """Coordinates groups. One instance per agent."""
    _coordinator = None
    _our_id = uuid.uuid4()

    def __init__(self, conf):
        if conf.coordination_backend_url:
            self._coordinator = tooz.coordination.getcoordinator(
                conf.coordination_backend_url, self._our_id)

    def heartbeat(self):
        if self._coordinator:
            self._coordinator.heartbeat()

    def _join_group(self, group_id):
        if not self._coordinator:
            return
        while True:
            join_req = self._coordinator.join_group()
            try:
                join_req.get()
                break
            except tooz.MemberAlreadyExist:
                return
            except tooz.GroupNotCreated:
                create_grp_req = self._coordinator.create_group(group_id)
                try:
                    create_grp_req.get()
                except tooz.GroupAlreadyExist:
                    pass
                continue

    def get_partitioner(self, group_id="central_agent_global"):
        self._join_group(group_id)
        return self.Partitioner(self, group_id)

    class Partitioner:
        """ Work partitioner
        """

        def __init__(self, group_coordinator, group_id):
            self.group_coordinator = group_coordinator

        def get_my_partition(self, iterable):
            members = self.group_coordinator.get_members(group_id)
            our_key = sorted(members).index(self.our_id)
            return [v for v in obj
                    if int(hashlib.md5(str(v)).hexdigest(), 16)
                    % len(members) == our_key]
