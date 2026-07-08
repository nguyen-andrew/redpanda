/*
 * Copyright 2026 Redpanda Data, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */
#include "security/scram_credential_cache.h"

#include "bytes/random.h"
#include "hashing/secure.h"

#include <seastar/core/shared_ptr.hh>

#include <bit>

namespace security {

namespace {
constexpr size_t digest_key_size = 32;
// The S3-FIFO probationary queue is conventionally ~10% of the cache.
constexpr size_t small_queue_ratio = 10;
} // namespace

scram_credential_cache::scram_credential_cache(size_t capacity)
  : _digest_key(random_generators::get_crypto_bytes(digest_key_size))
  , _cache(
      {.cache_size = capacity,
       .small_size = std::max<size_t>(1, capacity / small_queue_ratio)}) {}

scram_credential_cache::key_t scram_credential_cache::make_key(
  scram_algorithm_t mech,
  const credential_password& password,
  bytes_view salt,
  int iterations) const {
    hmac_sha256 mac(_digest_key);
    // Unambiguous serialization: fixed-size fields and the password length
    // come first, so no two distinct inputs produce the same byte stream.
    mac.update(std::array<char, 1>{static_cast<char>(mech)});
    mac.update(
      std::bit_cast<std::array<char, sizeof(uint32_t)>>(
        static_cast<uint32_t>(iterations)));
    mac.update(
      std::bit_cast<std::array<char, sizeof(uint32_t)>>(
        static_cast<uint32_t>(password().size())));
    mac.update(std::string_view{password()});
    mac.update(salt);
    return {.digest = mac.reset()};
}

std::optional<bytes> scram_credential_cache::get(
  scram_algorithm_t mech,
  const credential_password& password,
  bytes_view salt,
  int iterations) {
    auto val = _cache.get_value(make_key(mech, password, salt, iterations));
    if (!val) {
        return std::nullopt;
    }
    return **val;
}

void scram_credential_cache::put(
  scram_algorithm_t mech,
  const credential_password& password,
  bytes_view salt,
  int iterations,
  bytes stored_key) {
    _cache.try_insert(
      make_key(mech, password, salt, iterations),
      ss::make_shared<bytes>(std::move(stored_key)));
}

scram_credential_cache::stats_t scram_credential_cache::stats() const {
    auto s = _cache.stat();
    return {
      .hits = s.hit_count,
      .misses = s.access_count - s.hit_count,
      .size = s.index_size};
}

} // namespace security
