#
# Copyright 2014 Red Hat
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

import itertools
import collections
import socket

import msgpack
from oslo.config import cfg

from ceilometer import dispatcher
from ceilometer import messaging
from ceilometer.openstack.common.gettextutils import _
from ceilometerclient import client as ceiloclient
from ceilometer.openstack.common import log
from ceilometer.openstack.common import service as os_service
from ceilometer.openstack.common import units

cfg.CONF.import_opt('alarming_topic', 'ceilometer.alarm.dispatcher.alarm',
                    group="alarming_rpc")


LOG = log.getLogger(__name__)


def _three_level_map():
    collections.defaultdict( # meter_name
        lambda: collections.defaultdict( # resource_id
            lambda: collections.defaultdict( # other parameters
                lambda: set()))) # set of entities

class AlarmDistributorService(os_service.Service):
    """Listens to samples and distributes them to relevant alarms."""

    # decision tree
    tree = _three_level_map()

    # TODO(nsaje): move into a mixin, so it can be used by evaluator
    @property
    def _client(self):
        """Construct or reuse an authenticated API client."""
        if not self.api_client:
            auth_config = cfg.CONF.service_credentials
            creds = dict(
                os_auth_url=auth_config.os_auth_url,
                os_region_name=auth_config.os_region_name,
                os_tenant_name=auth_config.os_tenant_name,
                os_password=auth_config.os_password,
                os_username=auth_config.os_username,
                os_cacert=auth_config.os_cacert,
                os_endpoint_type=auth_config.os_endpoint_type,
                insecure=auth_config.insecure,
                )
            self.api_client = ceiloclient.get_client(2, **creds)
        return self.api_client

    def start(self):
        """todo"""
        self.rpc_server = None
        super(AlarmDistributorService, self).start()

        # TODO(nsaje): why optional?
        transport = messaging.get_transport(optional=True)
        if transport:
            self.rpc_server = messaging.get_rpc_server(
                transport, cfg.CONF.alarming_rpc.alarming_topic, self)
            self.rpc_server.start()

            # Add a dummy thread to have wait() working
            self.tg.add_timer(604800, lambda: None)

    def stop(self):
        if self.rpc_server:
            self.rpc_server.stop()
        super(AlarmDistributorService, self).stop()


    @staticmethod
    def _query_to_kwargs(query):
        kwargs = {}
        for q in query:
            kwargs[q['field']] = q['value']  # TODO(nsaje): validation?
        return kwargs

    @classmethod
    def flatten(cls, d, parent_key='', sep='.'):
        items = []
        for k, v in d.items():
            new_key = parent_key + sep + k if parent_key else k
            if isinstance(v, collections.MutableMapping):
                items.extend(cls.flatten(v, new_key).items())
            else:
                items.append((new_key, v))
        return dict(items)

    def _refresh_alarms(self):
        # TODO(nsaje): handle disabled alarms
        # maybe by having a 'entity' field in the db model.
        # If it's empty, clear the entity and the routing path
        alarms = self._client.alarms.list(q=[{'field': 'enabled',
                                              'value': True}])
        for alarm in alarms: # TODO(nsaje): only do threshold alarms?
            query = self._query_to_kwargs(alarm.query)
            project_id = query.pop('project_id', '')
            resource_id = query.pop('resource_id', '')
            # NOTE(nsaje): only allow 'eq' operator in queries
            remaining_query = frozenset(query.items())
            self.tree[project_id][resource_id][remaining_query] = alarm.id

    def distribute_samples(self, context, data):
        """RPC endpoint for samples to distribute to alarms.
        """
        for sample in data:
            entities = set()
            meter_name = sample['meter_name']
            project_id = sample['project_id']
            resource_id = sample['resource_id']
            flattened_sample = self.flatten(sample)
            properties = frozenset(flattened_sample.items())

            for m_name, p_id, r_id in itertools.product(meter_name,
                                                        (project_id, None),
                                                        (resource_id, None)):
                for query in self.tree[m_name][p_id][r_id].keys():
                    if query.issubset(properties):
                        entities += self.tree[query]

            for entity in entities:
                pass # push to Gnocchi
