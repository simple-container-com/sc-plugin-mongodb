// SPDX-License-Identifier: MIT
// Copyright (c) Simple Container

package peer

import (
	"net/url"
	"strings"
	"testing"
)

const (
	tSalt    = "a-registry-salt-at-least-as-long-as-a-real-one"
	tCluster = "mongodb"
	tProject = "697a574e90d5387e4a889f5f"
	tDB      = "forge-conductor--staging"
	tUser    = "peer-forge-conductor--staging"
)

// TestDerivePassword_StableAcrossDeploys is the whole point of the
// change: two deploys of the same consumer must upsert the SAME
// password, so that a mint reply lost in flight costs nothing and the
// next deploy re-derives the identical string.
func TestDerivePassword_StableAcrossDeploys(t *testing.T) {
	first, err := DerivePassword(tSalt, tCluster, tProject, tDB, tUser)
	if err != nil {
		t.Fatalf("derive: %v", err)
	}
	second, err := DerivePassword(tSalt, tCluster, tProject, tDB, tUser)
	if err != nil {
		t.Fatalf("derive: %v", err)
	}
	if first != second {
		t.Errorf("same inputs must derive the same password, got two values")
	}
	if first == "" {
		t.Error("derived password must not be empty")
	}
}

// TestDerivePassword_DistinctPerIdentityField — stability must not come
// at the cost of isolation. Varying any single identity field has to
// produce a different password, or two consumers on one cluster share a
// credential.
func TestDerivePassword_DistinctPerIdentityField(t *testing.T) {
	base, err := DerivePassword(tSalt, tCluster, tProject, tDB, tUser)
	if err != nil {
		t.Fatalf("derive base: %v", err)
	}

	for _, tc := range []struct {
		name                                 string
		cluster, project, database, username string
	}{
		{"cluster", "smoke-mongo", tProject, tDB, tUser},
		{"project", tCluster, "000000000000000000000000", tDB, tUser},
		{"database", tCluster, tProject, "forge-messenger--staging", tUser},
		{"username", tCluster, tProject, tDB, "peer-forge-messenger--staging"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got, err := DerivePassword(tSalt, tc.cluster, tc.project, tc.database, tc.username)
			if err != nil {
				t.Fatalf("derive: %v", err)
			}
			if got == base {
				t.Errorf("a different %s must derive a different password", tc.name)
			}
		})
	}
}

// TestDerivePassword_SaltRotationChangesEveryPassword — the rotation
// story the random scheme never had: change the salt, redeploy, every
// consumer converges on a new password.
func TestDerivePassword_SaltRotationChangesEveryPassword(t *testing.T) {
	before, err := DerivePassword(tSalt, tCluster, tProject, tDB, tUser)
	if err != nil {
		t.Fatalf("derive: %v", err)
	}
	after, err := DerivePassword(tSalt+"-rotated", tCluster, tProject, tDB, tUser)
	if err != nil {
		t.Fatalf("derive: %v", err)
	}
	if before == after {
		t.Error("a new salt must derive a new password")
	}
}

// TestDerivePassword_ShapeIsURISafe — weaveCredsIntoURI does not escape
// the password, and mongodump is handed the woven URI verbatim. So the
// derived password has to be drawn from a character set that survives a
// URI unescaped. Asserted on the parse, not just the alphabet, because
// the alphabet is the claim and the parse is the consequence.
func TestDerivePassword_ShapeIsURISafe(t *testing.T) {
	const alphabet = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789-_"

	// Several identities, so this is not one lucky hash.
	for _, user := range []string{tUser, "peer-forge-messenger--staging", "peer-a", strings.Repeat("u", 100)} {
		pw, err := DerivePassword(tSalt, tCluster, tProject, tDB, user)
		if err != nil {
			t.Fatalf("derive(%s): %v", user, err)
		}
		if len(pw) != derivedPasswordLength {
			t.Errorf("derive(%s): length %d, want %d", user, len(pw), derivedPasswordLength)
		}
		for i, r := range pw {
			if !strings.ContainsRune(alphabet, r) {
				t.Fatalf("derive(%s): char %q at %d is outside the URI-safe alphabet", user, r, i)
			}
		}

		woven := weaveCredsIntoURI("mongodb+srv://cluster0.example.mongodb.net", user, pw)
		parsed, err := url.Parse(woven)
		if err != nil {
			t.Fatalf("woven URI does not parse: %v", err)
		}
		gotPW, set := parsed.User.Password()
		if !set || gotPW != pw {
			t.Errorf("password did not survive the URI round-trip: got %q set=%v", gotPW, set)
		}
	}
}

// TestDerivePassword_FieldBoundariesAreUnambiguous — the HMAC input is
// length-prefixed, which makes it prefix-free: two different identities
// cannot produce one input string. The two ways to get that wrong are
// bare concatenation (where ("ab","c") and ("a","bc") collide) and
// delimiter-joining (where the delimiter appearing INSIDE a field makes
// ("a|b","c") and ("a","b|c") collide). Both matter here — a collision
// on this input set is two consumers sharing a password.
//
// The delimiter cases are not hypothetical for these fields: a database
// name is a stack name like `forge-conductor--staging`, so `-` is
// everywhere, and a username is `peer-<stack>`.
//
// The first version of this test only covered bare concatenation and so
// passed against a deliberately pipe-joined implementation.
func TestDerivePassword_FieldBoundariesAreUnambiguous(t *testing.T) {
	for _, tc := range []struct {
		name string
		a    [4]string
		b    [4]string
	}{
		// Bare concatenation.
		{
			"cluster/project, no delimiter",
			[4]string{"ab", "c", tDB, tUser},
			[4]string{"a", "bc", tDB, tUser},
		},
		{
			"project/database, no delimiter",
			[4]string{tCluster, "ab", "c", tUser},
			[4]string{tCluster, "a", "bc", tUser},
		},
		{
			"database/username, no delimiter",
			[4]string{tCluster, tProject, "ab", "c"},
			[4]string{tCluster, tProject, "a", "bc"},
		},
		// Empty field absorbed by a bare concatenation.
		{
			"empty field, no delimiter",
			[4]string{"", "ab", tDB, tUser},
			[4]string{"a", "b", tDB, tUser},
		},
		// A delimiter appearing inside a field.
		{
			"pipe inside a field",
			[4]string{"a|b", "c", tDB, tUser},
			[4]string{"a", "b|c", tDB, tUser},
		},
		{
			"colon inside a field",
			[4]string{"a:b", "c", tDB, tUser},
			[4]string{"a", "b:c", tDB, tUser},
		},
		{
			"slash inside a field",
			[4]string{tCluster, tProject, "a/b", "c"},
			[4]string{tCluster, tProject, "a", "b/c"},
		},
		{
			"dash inside a field — the shape real stack names have",
			[4]string{tCluster, tProject, "forge-conductor", "staging"},
			[4]string{tCluster, tProject, "forge", "conductor-staging"},
		},
		{
			"NUL inside a field",
			[4]string{tCluster, tProject, "a\x00b", "c"},
			[4]string{tCluster, tProject, "a", "b\x00c"},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			left, err := DerivePassword(tSalt, tc.a[0], tc.a[1], tc.a[2], tc.a[3])
			if err != nil {
				t.Fatalf("derive: %v", err)
			}
			right, err := DerivePassword(tSalt, tc.b[0], tc.b[1], tc.b[2], tc.b[3])
			if err != nil {
				t.Fatalf("derive: %v", err)
			}
			if left == right {
				t.Error("field boundary is ambiguous: two distinct identities collided")
			}
		})
	}
}

// TestDerivePassword_EmptySaltRefused — no silent "derive from nothing".
func TestDerivePassword_EmptySaltRefused(t *testing.T) {
	if _, err := DerivePassword("", tCluster, tProject, tDB, tUser); err == nil {
		t.Fatal("an empty salt must be refused")
	}
}
