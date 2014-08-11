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

import atexit
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

_coordinator = None
_our_id = uuid.uuid4()
_group_members = collections.defaultdict(set)

if cfg.CONF.backend_url:
    _coordinator = tooz.coordination.get_coordinator(
        cfg.CONF.coordination_backend_url, _our_id)
    def heartbeat():
        while True:
            _coordinator.heartbeat()
            _coordinator.run_watchers()
            eventlet.sleep(cfg.CONF.coordination_heartbeat)
    eventlet.spawn_n(heartbeat)

    def stop():
        _coordinator.stop()
    atexit.register(stop)


def _group_join_handler(event):
    _group_members[event.group_id] += event.member_id


def _group_leave_handler(event):
    _group_members[event.group_id] -= event.member_id


def _join_group(group_id):
    joined = False
    while not joined:
        join_req = _coordinator.join_group()
        try:
            join_req.get()
        except tooz.MemberAlreadyExist:
            return
        except tooz.GroupNotCreated:
            create_grp_req = _coordinator.create_group(group_id)
            try:
                create_grp_req.get()
            except tooz.GroupAlreadyExist:
                pass
            continue
        joined = True

    get_members_req = _coordinator.get_members(group_id)
    _group_members[group_id] += get_members_req.get()

    _coordinator.watch_join_group(group_id, _group_join_handler)
    _coordinator.watch_leave_group(group_id, _group_leave_handler)


def _get_members(group_id):
    return _group_members[group_id]


class Partition:
    """ Work partitioning decorator
    """
    def __init__(self, func, group_id="central_agent_global"):
        self.func = func
        functools.update_wrapper(self, func)
        _join_group(group_id)

    def __call__(self, *args, **kwargs):
        obj = self.func(*args, **kwargs)
        if hasattr(obj, '__iter__'):
            members = _get_members()
            our_key = sorted(members).index(_our_id)
            return [v for v in obj
                    if int(hashlib.md5(str(v)).hexdigest(), 16)
                    % len(members) == our_key]
        else:
            return obj
