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

#include "crypto/crypto.h"
#include "crypto/exceptions.h"
#include "crypto/types.h"
#include "gtest/gtest.h"
#include "test_utils/test.h"
#include "test_values.h"

#include <gtest/gtest.h>

using namespace test_values;

TEST(crypto_key, load_pem_private_key) {
    auto key = crypto::key::load_key(
      example_pem_rsa_private_key,
      crypto::format_type::PEM,
      crypto::is_private_key_t::yes);

    EXPECT_EQ(key.get_type(), crypto::key_type::RSA);
    EXPECT_TRUE(key.is_private_key());
}

TEST(crypto_key, load_pem_public_key) {
    auto key = crypto::key::load_key(
      example_pem_rsa_public_key,
      crypto::format_type::PEM,
      crypto::is_private_key_t::no);

    EXPECT_EQ(key.get_type(), crypto::key_type::RSA);
    EXPECT_FALSE(key.is_private_key());
}

TEST(crypto_key, load_ec_key) {
    auto ec_key = crypto::key::load_key(
      example_pem_ec_public_key,
      crypto::format_type::PEM,
      crypto::is_private_key_t::no);
    EXPECT_EQ(ec_key.get_type(), crypto::key_type::EC);
}

TEST(crypto_key, load_rsa_pub_key_components) {
    EXPECT_NO_THROW(
      crypto::key::load_rsa_public_key(rsa_pub_key_n, rsa_pub_key_e));
}

TEST(crypto_key, load_ec_pub_key_components) {
    auto key = crypto::key::load_ec_public_key(
      crypto::ec_curve::P256, ec_p256_pub_x, ec_p256_pub_y);
    EXPECT_EQ(key.get_type(), crypto::key_type::EC);
    EXPECT_FALSE(key.is_private_key());
}

TEST(crypto_key, load_ec_pub_key_bad_width) {
    auto short_x = bytes{ec_p256_pub_x.begin(), ec_p256_pub_x.end() - 1};
    EXPECT_THROW(
      crypto::key::load_ec_public_key(
        crypto::ec_curve::P256, short_x, ec_p256_pub_y),
      crypto::exception);

    // P-256 coordinates (32 bytes) against P-384's 48 byte field width
    EXPECT_THROW(
      crypto::key::load_ec_public_key(
        crypto::ec_curve::P384, ec_p256_pub_x, ec_p256_pub_y),
      crypto::exception);
}

TEST(crypto_key, load_ec_pub_key_invalid_point) {
    auto bad_y = ec_p256_pub_y;
    bad_y[bad_y.size() - 1] ^= 0x01;
    EXPECT_THROW(
      crypto::key::load_ec_public_key(
        crypto::ec_curve::P256, ec_p256_pub_x, bad_y),
      crypto::exception);
}
