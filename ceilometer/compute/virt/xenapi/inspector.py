# Copyright 2014 Intel
#
# Author: Ren Qiaowei <qiaowei.ren@intel.com>
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
"""Implementation of Inspector abstraction for XenAPI."""

from eventlet import timeout
from oslo.config import cfg
try:
    import XenAPI as api
except ImportError:
    api = None

from ceilometer.compute.virt import inspector as virt_inspector
from ceilometer.openstack.common.gettextutils import _

opt_group = cfg.OptGroup(name='xenapi',
                         title='Options for XenAPI')

xenapi_opts = [
    cfg.StrOpt('connection_url',
               help='URL for connection to XenServer/Xen Cloud Platform'),
    cfg.StrOpt('connection_username',
               default='root',
               help='Username for connection to XenServer/Xen Cloud Platform'),
    cfg.StrOpt('connection_password',
               help='Password for connection to XenServer/Xen Cloud Platform',
               secret=True),
    cfg.IntOpt('login_timeout',
               default=10,
               help='Timeout in seconds for XenAPI login.'),
]

CONF = cfg.CONF
CONF.register_group(opt_group)
CONF.register_opts(xenapi_opts, group=opt_group)


class XenapiException(virt_inspector.InspectorException):
    pass


def get_api_session():
    if not api:
        raise ImportError(_('XenAPI not installed'))

    url = CONF.xenapi.connection_url
    username = CONF.xenapi.connection_username
    password = CONF.xenapi.connection_password
    if not url or password is None:
        raise XenapiException(_('Must specify connection_url, and '
                                'connection_password to use'))

    exception = api.Failure(_("Unable to log in to XenAPI "
                              "(is the Dom0 disk full?)"))
    try:
        session = api.Session(url)
        with timeout.Timeout(CONF.xenapi.login_timeout, exception):
            session.login_with_password(username, password)
    except api.Failure as e:
        msg = _("Could not connect to XenAPI: %s") % e.details[0]
        raise XenapiException(msg)
    return session


class XenapiInspector(virt_inspector.Inspector):

    def __init__(self):
        super(XenapiInspector, self).__init__()
        self.session = get_api_session()

    def _get_host_ref(self):
        """Return the xenapi host on which nova-compute runs on."""
        return self.session.xenapi.session.get_this_host(self.session.handle)

    def _call_xenapi(self, method, *args):
        return self.session.xenapi_request(method, args)

    def _list_vms(self):
        host_ref = self._get_host_ref()
        vms = self._call_xenapi("VM.get_all_records_where",
                                'field "is_control_domain"="false" and '
                                'field "is_a_template"="false" and '
                                'field "resident_on"="%s"' % host_ref)
        for vm_ref in vms.keys():
            yield vm_ref, vms[vm_ref]

    def inspect_instances(self):
        for vm_ref, vm_rec in self._list_vms():
            name = vm_rec['name_label']
            other_config = vm_rec['other_config']
            uuid = other_config.get('nova_uuid')
            if uuid:
                yield virt_inspector.Instance(name, uuid)
