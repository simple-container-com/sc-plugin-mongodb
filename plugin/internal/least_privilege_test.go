// SPDX-License-Identifier: MIT
// Copyright (c) Simple Container

package mongodb

import (
	"reflect"
	"testing"
)

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
	plain := serviceUserInputs(legacy, "meeting", "staging", false, true)
	if len(plain) != 1 || plain[0].username != legacy.username {
		t.Fatal("default user identity changed")
	}
	staging := serviceUserInputs(legacy, "meeting", "staging", true, true)
	production := serviceUserInputs(legacy, "meeting", "production", true, true)
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

func TestDefaultServiceUserMatchesOriginMain(t *testing.T) {
	users := serviceUserInputs(dbUserInput{username: "app"}, "app", "prod", false, false)
	if len(users) != 1 || users[0].username != "app" || len(users[0].roles) != 3 || users[0].roles[2] != (dbRole{dbName: "local", role: "read"}) {
		t.Fatalf("default changed legacy user/roles: %+v", users)
	}
}

func TestLegacyTemplateMatchesOriginMain(t *testing.T) {
	want := map[string]string{
		"uri":    "mongodb+srv://app:password@example.net/app",
		"dbName": "app", "password": "password", "user": "app",
		"oplogUri": "mongodb+srv://app:password@example.net/local",
	}
	got := serviceDatabaseTemplate("mongodb+srv://example.net", "app", "password", "app", true)
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("legacy template changed: %+v", got)
	}
}
