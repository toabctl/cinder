
# Copyright 2011 Piston Cloud Computing, Inc.
# All Rights Reserved.

#    Licensed under the Apache License, Version 2.0 (the "License"); you may
#    not use this file except in compliance with the License. You may obtain
#    a copy of the License at
#
#         http://www.apache.org/licenses/LICENSE-2.0
#
#    Unless required by applicable law or agreed to in writing, software
#    distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
#    WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
#    License for the specific language governing permissions and limitations
#    under the License.

"""Test of Policy Engine For Cinder."""

import os.path
import urllib2

from oslo.config import cfg
import six

from cinder import context
from cinder import exception
import cinder.openstack.common.policy
from cinder.openstack.common import policy as common_policy
from cinder import policy
from cinder import test
from cinder import utils


CONF = cfg.CONF


class PolicyFileTestCase(test.TestCase):
    def setUp(self):
        super(PolicyFileTestCase, self).setUp()
        # since is_admin is defined by policy, create context before reset
        self.context = context.RequestContext('fake', 'fake')
        policy.reset()
        self.target = {}
        self.addCleanup(policy.reset)

    def test_modified_policy_reloads(self):
        with utils.tempdir() as tmpdir:
            tmpfilename = os.path.join(tmpdir, 'policy')
            self.flags(policy_file=tmpfilename)

            action = "example:test"
            with open(tmpfilename, "w") as policyfile:
                policyfile.write("""{"example:test": []}""")
            policy.enforce(self.context, action, self.target)
            with open(tmpfilename, "w") as policyfile:
                policyfile.write("""{"example:test": ["false:false"]}""")
            # NOTE(vish): reset stored policy cache so we don't have to
            # sleep(1)
            policy._POLICY_CACHE = {}
            self.assertRaises(exception.PolicyNotAuthorized, policy.enforce,
                              self.context, action, self.target)


class PolicyTestCase(test.TestCase):
    def setUp(self):
        super(PolicyTestCase, self).setUp()
        policy.reset()
        # NOTE(vish): preload rules to circumvent reloading from file
        policy.init()
        rules = {
            "true": [],
            "example:allowed": [],
            "example:denied": [["false:false"]],
            "example:get_http": [["http:http://www.example.com"]],
            "example:my_file": [["role:compute_admin"],
                                ["project_id:%(project_id)s"]],
            "example:early_and_fail": [["false:false", "rule:true"]],
            "example:early_or_success": [["rule:true"], ["false:false"]],
            "example:lowercase_admin": [["role:admin"], ["role:sysadmin"]],
            "example:uppercase_admin": [["role:ADMIN"], ["role:sysadmin"]],
        }
        # NOTE(vish): then overload underlying brain
        common_policy.set_brain(common_policy.Brain(rules))
        self.context = context.RequestContext('fake', 'fake', roles=['member'])
        self.target = {}
        self.addCleanup(policy.reset)

    def test_enforce_nonexistent_action_throws(self):
        action = "example:noexist"
        self.assertRaises(exception.PolicyNotAuthorized, policy.enforce,
                          self.context, action, self.target)

    def test_enforce_bad_action_throws(self):
        action = "example:denied"
        self.assertRaises(exception.PolicyNotAuthorized, policy.enforce,
                          self.context, action, self.target)

    def test_enforce_good_action(self):
        action = "example:allowed"
        policy.enforce(self.context, action, self.target)

    def test_enforce_http_true(self):

        def fakeurlopen(url, post_data):
            return six.StringIO("True")
        self.stubs.Set(urllib2, 'urlopen', fakeurlopen)
        action = "example:get_http"
        target = {}
        result = policy.enforce(self.context, action, target)
        self.assertIsNone(result)

    def test_enforce_http_false(self):

        def fakeurlopen(url, post_data):
            return six.StringIO("False")
        self.stubs.Set(urllib2, 'urlopen', fakeurlopen)
        action = "example:get_http"
        target = {}
        self.assertRaises(exception.PolicyNotAuthorized, policy.enforce,
                          self.context, action, target)

    def test_templatized_enforcement(self):
        target_mine = {'project_id': 'fake'}
        target_not_mine = {'project_id': 'another'}
        action = "example:my_file"
        policy.enforce(self.context, action, target_mine)
        self.assertRaises(exception.PolicyNotAuthorized, policy.enforce,
                          self.context, action, target_not_mine)

    def test_early_AND_enforcement(self):
        action = "example:early_and_fail"
        self.assertRaises(exception.PolicyNotAuthorized, policy.enforce,
                          self.context, action, self.target)

    def test_early_OR_enforcement(self):
        action = "example:early_or_success"
        policy.enforce(self.context, action, self.target)

    def test_ignore_case_role_check(self):
        lowercase_action = "example:lowercase_admin"
        uppercase_action = "example:uppercase_admin"
        # NOTE(dprince) we mix case in the Admin role here to ensure
        # case is ignored
        admin_context = context.RequestContext('admin',
                                               'fake',
                                               roles=['AdMiN'])
        policy.enforce(admin_context, lowercase_action, self.target)
        policy.enforce(admin_context, uppercase_action, self.target)


class DefaultPolicyTestCase(test.TestCase):

    def setUp(self):
        super(DefaultPolicyTestCase, self).setUp()
        policy.reset()
        policy.init()

        self.rules = {
            "default": [],
            "example:exist": [["false:false"]]
        }

        self._set_brain('default')

        self.context = context.RequestContext('fake', 'fake')

        self.addCleanup(policy.reset)

    def _set_brain(self, default_rule):
        brain = cinder.openstack.common.policy.Brain(self.rules,
                                                     default_rule)
        cinder.openstack.common.policy.set_brain(brain)

    def test_policy_called(self):
        self.assertRaises(exception.PolicyNotAuthorized, policy.enforce,
                          self.context, "example:exist", {})

    def test_not_found_policy_calls_default(self):
        policy.enforce(self.context, "example:noexist", {})

    def test_default_not_found(self):
        self._set_brain("default_noexist")
        self.assertRaises(exception.PolicyNotAuthorized, policy.enforce,
                          self.context, "example:noexist", {})


class ContextIsAdminPolicyTestCase(test.TestCase):

    def setUp(self):
        super(ContextIsAdminPolicyTestCase, self).setUp()
        policy.reset()
        policy.init()

    def test_default_admin_role_is_admin(self):
        ctx = context.RequestContext('fake', 'fake', roles=['johnny-admin'])
        self.assertFalse(ctx.is_admin)
        ctx = context.RequestContext('fake', 'fake', roles=['admin'])
        self.assertTrue(ctx.is_admin)

    def test_custom_admin_role_is_admin(self):
        # define explicit rules for context_is_admin
        rules = {
            'context_is_admin': [["role:administrator"], ["role:johnny-admin"]]
        }
        brain = common_policy.Brain(rules, CONF.policy_default_rule)
        common_policy.set_brain(brain)
        ctx = context.RequestContext('fake', 'fake', roles=['johnny-admin'])
        self.assertTrue(ctx.is_admin)
        ctx = context.RequestContext('fake', 'fake', roles=['administrator'])
        self.assertTrue(ctx.is_admin)
        # default rule no longer applies
        ctx = context.RequestContext('fake', 'fake', roles=['admin'])
        self.assertFalse(ctx.is_admin)

    def test_context_is_admin_undefined(self):
        rules = {
            "admin_or_owner": [["role:admin"], ["project_id:%(project_id)s"]],
            "default": [["rule:admin_or_owner"]],
        }
        brain = common_policy.Brain(rules, CONF.policy_default_rule)
        common_policy.set_brain(brain)
        ctx = context.RequestContext('fake', 'fake')
        self.assertFalse(ctx.is_admin)
        ctx = context.RequestContext('fake', 'fake', roles=['admin'])
        self.assertTrue(ctx.is_admin)
