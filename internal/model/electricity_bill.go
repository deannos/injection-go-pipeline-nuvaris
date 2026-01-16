// internal/model/electricity_bill.go
package model

import (
	"fmt"
	"time"
)

// ElectricityBill represents the data structure for an electricity bill record.
// The JSON tags define the key names in the incoming JSON payload.
// Consider adding validation tags (e.g., from a library like 'go-playground/validator')
// for more robust input validation if required.
type ElectricityBill struct {
	// BillID is a unique identifier for this specific bill.
	BillID string `json:"bill_id"`
	// CustomerID identifies the customer associated with this bill.
	CustomerID string `json:"customer_id"`
	// AccountNumber is the customer's account number.
	AccountNumber string `json:"account_number"`
	// ServiceAddress is the address where the electricity was supplied.
	ServiceAddress string `json:"service_address"`
	// BillingPeriodStartDate is the start date of the billing period.
	BillingPeriodStartDate time.Time `json:"billing_period_start_date"`
	// BillingPeriodEndDate is the end date of the billing period.
	BillingPeriodEndDate time.Time `json:"billing_period_end_date"`
	// MeterReadingPrevious is the meter reading at the start of the billing period.
	MeterReadingPrevious float64 `json:"meter_reading_previous"`
	// MeterReadingCurrent is the meter reading at the end of the billing period.
	MeterReadingCurrent float64 `json:"meter_reading_current"`
	// ConsumptionKWh is the total energy consumed in kilowatt-hours during the billing period.
	ConsumptionKWh float64 `json:"consumption_kwh"`
	// TariffType indicates the type of tariff applied (e.g., "residential", "commercial").
	TariffType string `json:"tariff_type"`
	// RatePerKWh is the price per kilowatt-hour for the applicable tariff.
	RatePerKWh float64 `json:"rate_per_kwh"`
	// EnergyCharge is the cost based on consumption and rate.
	EnergyCharge float64 `json:"energy_charge"`
	// FixedCharges represent any non-volumetric charges.
	FixedCharges float64 `json:"fixed_charges"`
	// Taxes are any applicable taxes.
	Taxes float64 `json:"taxes"`
	// TotalAmountDue is the sum of all charges and taxes.
	TotalAmountDue float64 `json:"total_amount_due"`
	// DueDate is the date by which the bill should be paid.
	DueDate time.Time `json:"due_date"`
	// BillIssueDate is the date the bill was generated.
	BillIssueDate time.Time `json:"bill_issue_date"`
	// PaymentStatus indicates the current status of the bill (e.g., "unpaid", "paid", "overdue").
	PaymentStatus string `json:"payment_status"` // Consider using an enum type for this
	// Timestamp when the record was ingested by this service (optional, can be set by the service)
	IngestedAt time.Time `json:"ingested_at,omitempty"`
}

// Validate performs basic validation on the ElectricityBill fields.
// This is a simple example; a more robust validation might use a dedicated library.
func (b *ElectricityBill) Validate() error {
	if b.BillID == "" {
		return fmt.Errorf("bill_id cannot be empty")
	}
	if b.CustomerID == "" {
		return fmt.Errorf("customer_id cannot be empty")
	}
	if b.ConsumptionKWh < 0 {
		return fmt.Errorf("consumption_kwh cannot be negative")
	}
	if b.TotalAmountDue < 0 {
		return fmt.Errorf("total_amount_due cannot be negative")
	}
	// Add more validation rules as needed (e.g., date ranges, required fields)
	return nil
}
