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

_group_manager = None


def get_group_manager(conf):
    global _group_manager
    if not _group_manager:
        _group_manager = _GroupManager(conf)


class _GroupManager:
    """Group manager, one instance per agent"""
    coordinator = None
    our_id = uuid.uuid4()
    group_members = collections.defaultdict(set)

    def __init__(self, conf):
        if conf.coordination_backend_url:
            self.coordinator = tooz.coordination.getcoordinator(
                conf.coordination_backend_url, self.our_id)

            def heartbeat():
                while True:
                    self.coordinator.heartbeat()
                    self.coordinator.run_watchers()
                    eventlet.sleep(conf.coordination_heartbeat)

            eventlet.spawn_n(heartbeat)

    def group_join_handler(self, event):
        self.group_members[event.group_id] += event.member_id

    def group_leave_handler(self, event):
        self.group_members[event.group_id] -= event.member_id

    def join_group(self, group_id):
        joined = False
        while not joined:
            join_req = self.coordinator.join_group()
            try:
                join_req.get()
            except tooz.MemberAlreadyExist:
                return
            except tooz.GroupNotCreated:
                create_grp_req = self.coordinator.create_group(group_id)
                try:
                    create_grp_req.get()
                except tooz.GroupAlreadyExist:
                    pass
                continue
            joined = True

        get_members_req = self.coordinator.get_members(group_id)
        self.group_members[group_id] += get_members_req.get()

        self.coordinator.watch_join_group(group_id, self.group_join_handler)
        self.coordinator.watch_leave_group(group_id, self.group_leave_handler)

    def get_members(self, group_id):
        return self.group_members[group_id]


class Partition:
    """ Work partitioning decorator
    """

    def __init__(self, func, group_id="central_agent_global"):
        self.func = func
        functools.update_wrapper(self, func)
        if not _group_manager:
            #_group_manager = _GroupManager(cfg.CONF)
            raise Exception('no group manager!') # TODO prettier exception
        _group_manager.join_group(group_id)

    def __call__(self, *args, **kwargs):
        obj = self.func(*args, **kwargs)
        if hasattr(obj, '__iter__'):
            members = _get_members()
            our_key = sorted(members).index(self.our_id)
            return [v for v in obj
                    if int(hashlib.md5(str(v)).hexdigest(), 16)
                    % len(members) == our_key]
        else:
            return obj
