package temporal

import (
	"context"
	"fmt"

	"go.temporal.io/sdk/activity"

	"github.com/archon-research/stl/stl-verify/internal/services/data_validator"
)

// DataValidator defines the interface for running data validation.
// This is satisfied by data_validator.Service.
type DataValidator interface {
	Validate(ctx context.Context) (*data_validator.Report, error)
}

// DataValidationActivities holds dependencies for data validation Temporal activities.
type DataValidationActivities struct {
	validator DataValidator
}

// NewDataValidationActivities creates a new DataValidationActivities instance.
func NewDataValidationActivities(validator DataValidator) (*DataValidationActivities, error) {
	if validator == nil {
		return nil, fmt.Errorf("validator cannot be nil")
	}
	return &DataValidationActivities{validator: validator}, nil
}

// ValidateDataOutput is the output of the ValidateData activity.
type ValidateDataOutput struct {
	Success    bool   `json:"success"`
	Passed     int    `json:"passed"`
	Failed     int    `json:"failed"`
	Errors     int    `json:"errors"`
	FromBlock  int64  `json:"from_block"`
	ToBlock    int64  `json:"to_block"`
	DurationMs int64  `json:"duration_ms"`
	ReportText string `json:"report_text"`
}

// ValidateData runs the data validation and returns a summary.
func (a *DataValidationActivities) ValidateData(ctx context.Context) (*ValidateDataOutput, error) {
	logger := activity.GetLogger(ctx)
	logger.Info("starting data validation")

	report, err := a.validator.Validate(ctx)
	if err != nil {
		return nil, fmt.Errorf("running data validation: %w", err)
	}

	report.Finalize()

	logger.Info("data validation completed",
		"passed", report.Passed,
		"failed", report.Failed,
		"errors", report.Errors,
		"success", report.Success(),
	)

	return &ValidateDataOutput{
		Success:    report.Success(),
		Passed:     report.Passed,
		Failed:     report.Failed,
		Errors:     report.Errors,
		FromBlock:  report.FromBlock,
		ToBlock:    report.ToBlock,
		DurationMs: report.Duration.Milliseconds(),
		ReportText: report.FormatText(),
	}, nil
}
