package temporal

// TaskQueue is the shared task queue for all Temporal workflows and activities.
const TaskQueue = "sentinel-workers"

// Schedule IDs for Temporal schedules.
const (
	PriceFetchScheduleID     = "coingecko-price-fetch"
	DataValidationScheduleID = "data-validation"
)
