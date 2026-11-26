// SPDX-License-Identifier: MIT
// Copyright (c) Simple Container

package peer

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/base64"
	"fmt"

	"github.com/pkg/errors"
)

// derivationScheme versions the HMAC input. Every derived password is a
// pure function of (salt, scheme, cluster, projectID, database,
// username) — so changing ANY of those changes every password that
// depends on it. Bumping this string is therefore a fleet-wide rotation
// and must be done deliberately, in the same way as rotating the salt:
// bump, redeploy every consumer, let each one re-PATCH and re-register.
const derivationScheme = "sc-plugin-mongodb/scoped-user/v1"

// derivedPasswordLength is 32 characters drawn from base64url's 64-char
// alphabet — 192 bits of the HMAC output, and the same URL-safe
// character set generatePassword used, so a derived password still
// survives weaveCredsIntoURI without escaping.
const derivedPasswordLength = 32

// DerivePassword returns the scoped user's password as a deterministic
// function of the operator's salt and the user's identity.
//
// # Why derive rather than generate
//
// Atlas never returns a password. A randomly-generated one therefore
// has exactly ONE copy — the mint reply, which the calling plugin
// pushes into its own tenant row — and `CreateDatabaseUser` PATCHes the
// live Atlas user with a fresh random value on every single consumer
// deploy. So any deploy whose push is lost leaves Atlas holding a
// password nobody can read and the consumer holding one that no longer
// authenticates, with no repair path short of another full deploy.
//
// That is not hypothetical. It cost `forge-messenger` and
// `forge-conductor` their backups on 2026-11-25 (nine hours of
// `(AuthenticationFailed)`) when a go-embedb replica acked the register
// write and discarded it, and it cost six tenants their backups earlier
// the same night when a snapshot restore rolled their tenant rows back
// to pre-rotation passwords — a restore is a time machine pointed at
// live credentials.
//
// Deriving removes the whole failure class: every deploy PATCHes the
// SAME value, so a lost push is harmless (the stored password still
// authenticates) and self-repairing (the next deploy re-derives and
// re-pushes the identical string). A snapshot restore of a consumer's
// tenant row is likewise harmless, because the password in the old row
// is the password Atlas still holds.
//
// # What it costs
//
// Per-deploy rotation goes away. It was never a security control —
// nothing drove it, nothing audited it, and its interval was "however
// often you happen to deploy". Deliberate rotation is still available
// and is now actually coherent: change the salt (or the scheme above),
// redeploy, and every consumer converges on a new password by the same
// derivation.
//
// # Isolation
//
// Every field that distinguishes one scoped user from another is in the
// HMAC input, so no two users share a password even on one cluster. The
// fields are length-prefixed rather than delimiter-joined: a plain
// separator would let ("ab", "c") and ("a", "bc") collide, which on
// this input set means two different consumers deriving one password.
func DerivePassword(salt, cluster, projectID, database, username string) (string, error) {
	if salt == "" {
		return "", errors.New("derive password: empty salt")
	}
	mac := hmac.New(sha256.New, []byte(salt))
	for _, field := range []string{derivationScheme, cluster, projectID, database, username} {
		// Length-prefixed, so field boundaries are unambiguous.
		if _, err := fmt.Fprintf(mac, "%d:%s", len(field), field); err != nil {
			return "", errors.Wrap(err, "derive password: write hmac input")
		}
	}
	// 24 bytes → exactly 32 base64url characters, no padding.
	return base64.RawURLEncoding.EncodeToString(mac.Sum(nil)[:derivedPasswordLength*6/8]), nil
}
