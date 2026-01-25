/*
 * Copyright 2024 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */
#pragma once

#include "base/outcome.h"
#include "security/acl.h"
#include "security/group_range.h"
#include "security/jwt.h"
#include "security/oidc_principal_mapping.h"

namespace security::oidc {

result<acl_principal>
principal_mapping_rule_apply(const principal_mapping_rule&, const jwt& jwt);

result<group_range>
group_policy_apply(const group_claim_policy&, ss::lw_shared_ptr<const jwt> jwt);

} // namespace security::oidc
