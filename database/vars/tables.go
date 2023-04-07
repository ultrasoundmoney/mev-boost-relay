// Package vars contains the database variables such as dynamic table names
package vars

import "github.com/flashbots/mev-boost-relay/common"

var (
	tableBase = common.GetEnv("DB_TABLE_PREFIX", "dev")

	TableMigrations             = tableBase + "_migrations"
	TableValidatorRegistration  = tableBase + "_validator_registration"
	TableExecutionPayload       = tableBase + "_execution_payload"
	TableBuilderBlockSubmission = tableBase + "_builder_block_submission"
	TableDeliveredPayload       = tableBase + "_payload_delivered"
	TableBlockBuilder           = tableBase + "_blockbuilder"
<<<<<<< HEAD
	TableBuilderDemotions       = tableBase + "_builder_demotions"
=======
	TableBlockedValidator       = tableBase + "_blocked_validator"
>>>>>>> 498d882304308985ed3dc2dfa39abdfc4c255e03
)
