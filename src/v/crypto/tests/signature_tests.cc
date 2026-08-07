/*
 * Copyright 2024 Redpanda Data, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */

#include "bytes/bytes.h"
#include "crypto/crypto.h"
#include "crypto/types.h"
#include "crypto_test_utils.h"
#include "test_utils/test.h"
#include "test_values.h"

#include <gtest/gtest.h>

using namespace test_values;

TEST(crypto_sig_validation, rsa_2k_sha256_good) {
    auto key = crypto::key::load_rsa_public_key(
      sig_test_rsa_pub_key_n, sig_test_rsa_pub_key_e);

    EXPECT_TRUE(
      crypto::verify_signature(
        crypto::digest_type::SHA256, key, sig_good_msg, sig_good_sig));
}

TEST(crypto_sig_validation, rsa_2k_sha256_bad) {
    auto key = crypto::key::load_rsa_public_key(
      sig_test_rsa_pub_key_n, sig_test_rsa_pub_key_e);

    EXPECT_FALSE(
      crypto::verify_signature(
        crypto::digest_type::SHA256,
        key,
        bytes_view_to_string_view(sig_bad_msg),
        bytes_view_to_string_view(sig_bad_sig)));
}

TEST(crypto_sig_validation, rsa_2k_sha256_good_multi_string) {
    auto key = crypto::key::load_rsa_public_key(
      sig_test_rsa_pub_key_n, sig_test_rsa_pub_key_e);

    crypto::verify_ctx ctx(crypto::digest_type::SHA256, key);
    ctx.update(bytes_view_to_string_view(sig_good_msg));
    EXPECT_TRUE(std::move(ctx).final(bytes_view_to_string_view(sig_good_sig)));
}

TEST(crypto_sig_validation, rsa_2k_sha256_reset) {
    auto key = crypto::key::load_rsa_public_key(
      sig_test_rsa_pub_key_n, sig_test_rsa_pub_key_e);

    crypto::verify_ctx ctx(crypto::digest_type::SHA256, key);
    ctx.update(sig_good_msg);
    EXPECT_TRUE(ctx.reset(sig_good_sig));
    ctx.update(bytes_view_to_string_view(sig_good_msg));
    EXPECT_TRUE(ctx.reset(bytes_view_to_string_view(sig_good_sig)));
}

TEST(crypto_sig_validation, ecdsa_p256_p1363_good) {
    auto key = crypto::key::load_ec_public_key(
      crypto::ec_curve::P256, es256_rfc7515_a3_x, es256_rfc7515_a3_y);
    EXPECT_TRUE(
      crypto::verify_signature(
        crypto::digest_type::SHA256,
        key,
        es256_rfc7515_a3_msg,
        es256_rfc7515_a3_sig_p1363,
        crypto::signature_format::P1363));
}

TEST(crypto_sig_validation, ecdsa_p256_der_good) {
    auto key = crypto::key::load_ec_public_key(
      crypto::ec_curve::P256, es256_rfc7515_a3_x, es256_rfc7515_a3_y);
    EXPECT_TRUE(
      crypto::verify_signature(
        crypto::digest_type::SHA256,
        key,
        es256_rfc7515_a3_msg,
        es256_rfc7515_a3_sig_der));
}

TEST(crypto_sig_validation, ecdsa_p384_p1363_good) {
    auto key = crypto::key::load_ec_public_key(
      crypto::ec_curve::P384, es384_gen_x, es384_gen_y);
    EXPECT_TRUE(
      crypto::verify_signature(
        crypto::digest_type::SHA384,
        key,
        es384_gen_msg,
        es384_gen_sig_p1363,
        crypto::signature_format::P1363));
    EXPECT_TRUE(
      crypto::verify_signature(
        crypto::digest_type::SHA384, key, es384_gen_msg, es384_gen_sig_der));
}

TEST(crypto_sig_validation, ecdsa_p521_p1363_good) {
    auto key = crypto::key::load_ec_public_key(
      crypto::ec_curve::P521, es512_rfc7515_a4_x, es512_rfc7515_a4_y);
    EXPECT_TRUE(
      crypto::verify_signature(
        crypto::digest_type::SHA512,
        key,
        es512_rfc7515_a4_msg,
        es512_rfc7515_a4_sig_p1363,
        crypto::signature_format::P1363));
}

TEST(crypto_sig_validation, ecdsa_p1363_wrong_length_returns_false) {
    auto key = crypto::key::load_ec_public_key(
      crypto::ec_curve::P256, es256_rfc7515_a3_x, es256_rfc7515_a3_y);
    auto truncated = bytes{
      es256_rfc7515_a3_sig_p1363.begin(), es256_rfc7515_a3_sig_p1363.end() - 1};
    EXPECT_FALSE(
      crypto::verify_signature(
        crypto::digest_type::SHA256,
        key,
        es256_rfc7515_a3_msg,
        truncated,
        crypto::signature_format::P1363));
}

TEST(crypto_sig_validation, ecdsa_p1363_garbage_returns_false_not_throw) {
    auto key = crypto::key::load_ec_public_key(
      crypto::ec_curve::P256, es256_rfc7515_a3_x, es256_rfc7515_a3_y);
    bytes garbage(bytes::initialized_later{}, 64);
    std::fill(garbage.begin(), garbage.end(), 0xff);
    EXPECT_NO_THROW(EXPECT_FALSE(
      crypto::verify_signature(
        crypto::digest_type::SHA256,
        key,
        es256_rfc7515_a3_msg,
        garbage,
        crypto::signature_format::P1363)));
}

TEST(crypto_sig_validation, ecdsa_p256_p1363_bitflip_returns_false) {
    // a correctly sized but wrong signature must exercise OpenSSL's point
    // arithmetic; the 0xff garbage vector short-circuits at the range check
    auto key = crypto::key::load_ec_public_key(
      crypto::ec_curve::P256, es256_rfc7515_a3_x, es256_rfc7515_a3_y);
    auto tampered = es256_rfc7515_a3_sig_p1363;
    tampered[40] ^= 0x01;
    EXPECT_NO_THROW(EXPECT_FALSE(
      crypto::verify_signature(
        crypto::digest_type::SHA256,
        key,
        es256_rfc7515_a3_msg,
        tampered,
        crypto::signature_format::P1363)));
}

TEST(crypto_sig_validation, ecdsa_p1363_via_reset) {
    // the OIDC verifier path calls reset(), not final()
    auto key = crypto::key::load_ec_public_key(
      crypto::ec_curve::P256, es256_rfc7515_a3_x, es256_rfc7515_a3_y);
    crypto::verify_ctx ctx(
      crypto::digest_type::SHA256, key, crypto::signature_format::P1363);
    ctx.update(es256_rfc7515_a3_msg);
    EXPECT_TRUE(ctx.reset(es256_rfc7515_a3_sig_p1363));
    // an early length-reject must leave the reused context clean
    auto truncated = bytes{
      es256_rfc7515_a3_sig_p1363.begin(), es256_rfc7515_a3_sig_p1363.end() - 1};
    ctx.update(es256_rfc7515_a3_msg);
    EXPECT_FALSE(ctx.reset(truncated));
    ctx.update(es256_rfc7515_a3_msg);
    EXPECT_TRUE(ctx.reset(es256_rfc7515_a3_sig_p1363));
}

TEST(crypto_sig_validation, p1363_requires_ec_key) {
    auto key = crypto::key::load_rsa_public_key(
      sig_test_rsa_pub_key_n, sig_test_rsa_pub_key_e);
    EXPECT_THROW(
      crypto::verify_ctx(
        crypto::digest_type::SHA256, key, crypto::signature_format::P1363),
      crypto::exception);
}
