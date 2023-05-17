// Package migrations contains all the migration files
package migrations

import (
	migrate "github.com/rubenv/sql-migrate"
)

var Migrations = migrate.MemoryMigrationSource{
	Migrations: []*migrate.Migration{
		Migration001InitDatabase,
		Migration002RemoveIsBestAddReceivedAt,
		Migration003Optimistic,
		Migration004BlockedValidator,
		Migration005RemoveBlockedValidator,
		Migration006CreateTooLateGetPayload,
		Migration007BuilderSubmissionWasSimulated,
		Migration008SimErrRename,
		Migration009BlockBuilderRemoveReference,
	},
}
