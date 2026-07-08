/*
 * Copyright 2026 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */

#include "security/scram_algorithm.h"
#include "security/scram_authenticator.h"
#include "security/types.h"
#include "ssx/sformat.h"

#include <seastar/testing/perf_tests.hh>

// Measures security::validate_scram_credential, the password check that
// request_authenticator::do_authenticate runs on every HTTP Basic auth
// request (pandaproxy, schema registry, admin API). The PBKDF2-style
// salted-password derivation (scram_algorithm::hi, min_iterations = 4096
// HMAC rounds) is what pegged pandaproxy CPU in INC-2882. A wrong password
// costs the same as a correct one: the mismatch is only detected after the
// full derivation.
//
// The correct/wrong cases reuse one credential per case, so they benefit
// from any per-credential memoization of the derivation; the
// unique-password case uses a fresh password each call and always pays the
// full derivation.

namespace {

const security::credential_password& password() {
    static const security::credential_password p{"benchpassword"};
    return p;
}

const security::credential_password& wrong_password() {
    static const security::credential_password p{"wrongpassword"};
    return p;
}

const security::scram_credential& sha256_credential() {
    static const auto cred = security::scram_sha256::make_credentials(
      password()(), security::scram_sha256::min_iterations);
    return cred;
}

const security::scram_credential& sha512_credential() {
    static const auto cred = security::scram_sha512::make_credentials(
      password()(), security::scram_sha512::min_iterations);
    return cred;
}

} // namespace

PERF_TEST(validate_scram_credential, sha256_correct_password) {
    auto mechanism = security::validate_scram_credential(
      sha256_credential(), password());
    perf_tests::do_not_optimize(mechanism);
}

PERF_TEST(validate_scram_credential, sha256_wrong_password) {
    auto mechanism = security::validate_scram_credential(
      sha256_credential(), wrong_password());
    perf_tests::do_not_optimize(mechanism);
}

PERF_TEST(validate_scram_credential, sha512_correct_password) {
    auto mechanism = security::validate_scram_credential(
      sha512_credential(), password());
    perf_tests::do_not_optimize(mechanism);
}

// Never-repeating passwords: every call pays the full salted-password
// derivation, with no opportunity for memoization.
PERF_TEST(validate_scram_credential, sha256_unique_passwords) {
    static size_t counter = 0;
    security::credential_password unique{
      ssx::sformat("benchpassword-{}", counter++)};
    auto mechanism = security::validate_scram_credential(
      sha256_credential(), unique);
    perf_tests::do_not_optimize(mechanism);
}
