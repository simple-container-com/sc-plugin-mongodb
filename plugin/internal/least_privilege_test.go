// SPDX-License-Identifier: MIT
// Copyright (c) Simple Container

package mongodb

import "testing"

func TestServiceDatabaseRolesRequireOplogOptIn(t *testing.T) {
	for _, allow := range []bool{false, true} {
		roles := serviceDatabaseRoles("staging", allow)
		if len(roles) != 2+boolInt(allow) {
			t.Fatalf("unexpected roles: %+v", roles)
		}
		for _, role := range roles {
			if role.dbName != "staging" && !(allow && role.dbName == "local" && role.role == "read") {
				t.Fatalf("cross-database grant: %+v", role)
			}
		}
		tpl := serviceDatabaseTemplate("mongodb+srv://example.net", "app", "password", "staging", allow)
		_, present := tpl["oplogUri"]
		if present != allow {
			t.Fatalf("oplog URI present=%t; opt in=%t", present, allow)
		}
	}
}
func boolInt(b bool) int {
	if b {
		return 1
	}
	return 0
}

func TestOplogPrincipalDoesNotPrivilegeLegacyOrOtherEnvironments(t *testing.T) {
	legacy := dbUserInput{projectId: "project", username: "meeting"}
	plain := serviceUserInputs(legacy, "meeting", "staging", false)
	if len(plain) != 1 || plain[0].username != legacy.username {
		t.Fatal("default user identity changed")
	}
	staging := serviceUserInputs(legacy, "meeting", "staging", true)
	production := serviceUserInputs(legacy, "meeting", "production", true)
	if len(staging) != 2 || staging[0].username != legacy.username || len(staging[0].roles) != 2 {
		t.Fatalf("legacy user was privileged or removed: %+v", staging)
	}
	if staging[1].username == legacy.username || staging[1].username == production[1].username {
		t.Fatal("oplog credentials shared across environments")
	}
	if len(staging[1].roles) != 3 || staging[1].roles[2].dbName != "local" {
		t.Fatal("opted-in principal missing oplog role")
	}
	for _, user := range staging {
		for _, role := range user.roles {
			if role.dbName != "meeting" && role.dbName != "local" {
				t.Fatalf("unexpected database role: %+v", role)
			}
		}
	}
}
