// SPDX-License-Identifier: MIT
// Copyright (c) Simple Container

package mongodb

import (
	"crypto/sha256"
	"fmt"
	sdk "github.com/pulumi/pulumi/sdk/v3/go/pulumi"
	pApi "github.com/simple-container-com/api/pkg/clouds/pulumi/api"
)

func serviceDatabaseRoles(database string, readOplog bool) []dbRole {
	roles := []dbRole{{dbName: database, role: "dbAdmin"}, {dbName: database, role: "readWrite"}}
	if readOplog {
		roles = append(roles, dbRole{dbName: "local", role: "read"})
	}
	return roles
}

func serviceDatabaseTemplate(uri, user, password, database string, readOplog bool) map[string]string {
	values := map[string]string{
		"uri":    AppendUserPasswordAndDBToMongoUri(uri, user, password, database),
		"dbName": database, "password": password, "user": user,
	}
	if readOplog {
		values["oplogUri"] = AppendUserPasswordAndDBToMongoUri(uri, user, password, "local")
	}
	return values
}

// Preserve the legacy user, roles and password/resource identities by default.
// Explicit readers use a separate principal so a stack name reused in another
// environment cannot inherit local read through the legacy shared username.
func serviceUserInputs(legacy dbUserInput, database, environment string, readOplog, leastPrivilege bool) []dbUserInput {
	legacy.roles = serviceDatabaseRoles(database, !leastPrivilege)
	users := []dbUserInput{legacy}
	if leastPrivilege && readOplog {
		privileged := legacy
		identity := sha256.Sum256([]byte(legacy.projectId + "\x00" + legacy.username + "\x00" + environment))
		privileged.username = fmt.Sprintf("sc-oplog-%x", identity)
		privileged.roles = serviceDatabaseRoles(database, true)
		users = append(users, privileged)
	}
	return users
}

func createServiceDatabaseUsers(ctx *sdk.Context, legacy dbUserInput, database, environment string, readOplog, leastPrivilege bool, params pApi.ProvisionParams) (runtime, original sdk.Output, username string, err error) {
	for i, user := range serviceUserInputs(legacy, database, environment, readOplog, leastPrivilege) {
		runtime, err = createDatabaseUser(ctx, user, params)
		if err != nil {
			return nil, nil, "", err
		}
		if i == 0 {
			original = runtime
		}
		username = user.username
	}
	return runtime, original, username, nil
}
