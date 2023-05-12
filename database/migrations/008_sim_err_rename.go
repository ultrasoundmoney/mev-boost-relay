package migrations

import (
	"github.com/flashbots/mev-boost-relay/database/vars"
	migrate "github.com/rubenv/sql-migrate"
)

var Migration008SimErrRename = &migrate.Migration{
	Id: "008-sim_err_rename",
	Up: []string{
		`ALTER TABLE ` + vars.TableBuilderDemotions + ` RENAME COLUMN submit_block_sim_error TO sim_error ;`,
	},
	Down: []string{},

	DisableTransactionUp:   true,
	DisableTransactionDown: true,
}
