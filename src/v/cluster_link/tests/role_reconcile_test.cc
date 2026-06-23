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

#include "cluster_link/role_reconcile.h"
#include "security/role.h"

#include <gtest/gtest.h>

namespace cluster_link {
namespace {

security::role_member user(std::string_view n) {
    return security::role_member{
      security::role_member_type::user, ss::sstring{n}};
}

security::role_with_members rwm(
  std::string_view name, std::initializer_list<security::role_member> members) {
    return security::role_with_members{
      .name = security::role_name{ss::sstring{name}},
      .role = security::role{security::role::container_type{members}}};
}

} // namespace

TEST(role_reconcile, create_only) {
    chunked_vector<security::role_with_members> src{rwm("a", {user("u1")})};
    chunked_vector<security::role_with_members> dst{};
    auto changes = reconcile_roles(std::move(src), std::move(dst));
    ASSERT_EQ(changes.to_create.size(), 1);
    EXPECT_EQ(changes.to_create[0].name, security::role_name{"a"});
    EXPECT_TRUE(changes.to_update.empty());
    EXPECT_TRUE(changes.to_delete.empty());
}

TEST(role_reconcile, membership_update) {
    chunked_vector<security::role_with_members> src{
      rwm("a", {user("u1"), user("u2")})};
    chunked_vector<security::role_with_members> dst{rwm("a", {user("u1")})};
    auto changes = reconcile_roles(std::move(src), std::move(dst));
    EXPECT_TRUE(changes.to_create.empty());
    ASSERT_EQ(changes.to_update.size(), 1);
    EXPECT_EQ(changes.to_update[0].name, security::role_name{"a"});
    EXPECT_EQ(changes.to_update[0].role.members().size(), 2);
    EXPECT_TRUE(changes.to_update[0].role.members().contains(user("u1")));
    EXPECT_TRUE(changes.to_update[0].role.members().contains(user("u2")));
    EXPECT_TRUE(changes.to_delete.empty());
}

TEST(role_reconcile, deletion_propagation) {
    chunked_vector<security::role_with_members> src{};
    chunked_vector<security::role_with_members> dst{rwm("a", {user("u1")})};
    auto changes = reconcile_roles(std::move(src), std::move(dst));
    EXPECT_TRUE(changes.to_create.empty());
    EXPECT_TRUE(changes.to_update.empty());
    ASSERT_EQ(changes.to_delete.size(), 1);
    EXPECT_EQ(changes.to_delete[0], security::role_name{"a"});
}

TEST(role_reconcile, no_op_when_equal) {
    chunked_vector<security::role_with_members> src{rwm("a", {user("u1")})};
    chunked_vector<security::role_with_members> dst{rwm("a", {user("u1")})};
    auto changes = reconcile_roles(std::move(src), std::move(dst));
    EXPECT_TRUE(changes.to_create.empty());
    EXPECT_TRUE(changes.to_update.empty());
    EXPECT_TRUE(changes.to_delete.empty());
}

TEST(role_reconcile, mixed_outcomes) {
    chunked_vector<security::role_with_members> src{
      rwm("a", {user("u1"), user("u2")}), rwm("c", {user("u3")})};
    chunked_vector<security::role_with_members> dst{
      rwm("a", {user("u1")}), rwm("b", {user("u4")})};
    auto changes = reconcile_roles(std::move(src), std::move(dst));

    ASSERT_EQ(changes.to_create.size(), 1);
    EXPECT_EQ(changes.to_create[0].name, security::role_name{"c"});

    ASSERT_EQ(changes.to_update.size(), 1);
    EXPECT_EQ(changes.to_update[0].name, security::role_name{"a"});
    EXPECT_EQ(changes.to_update[0].role.members().size(), 2);
    EXPECT_TRUE(changes.to_update[0].role.members().contains(user("u1")));
    EXPECT_TRUE(changes.to_update[0].role.members().contains(user("u2")));

    ASSERT_EQ(changes.to_delete.size(), 1);
    EXPECT_EQ(changes.to_delete[0], security::role_name{"b"});
}

} // namespace cluster_link
