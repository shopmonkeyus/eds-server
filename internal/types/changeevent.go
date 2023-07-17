package types

import (
	"bytes"
	"compress/gzip"
	"encoding/json"
	"io"
)

type ChangeEventOperation string

const (
	ChangeEventInsert ChangeEventOperation = "INSERT"
	ChangeEventUpdate ChangeEventOperation = "UPDATE"
	ChangeEventDelete ChangeEventOperation = "DELETE"
)

type ChangeEventPayload interface {
	// GetID returns the event payload id
	GetID() string
	// GetTimestamp returns the timestamp in milliseconds when the event occurred
	GetTimestamp() int64
	// GetMvccTimestamp returns the mvcc timestamp in microseconds when the change occurred
	GetMvccTimestamp() string
	// GetTable returns the table name
	GetTable() string
	// GetKey returns an array of primary keys
	GetKey() []string
	// GetLocationID returns the location id or nil if not provided or relevant for this table
	GetLocationID() *string
	// GetCompanyID returns the company id or nil if not provided or relevant for this table
	GetCompanyID() *string
	// GetUserID returns the user id that made the change or nil if not provided or relevant for this table
	GetUserID() *string
	// GetVersion returns a monotonically increasing version number for the change version to this record
	GetVersion() int64
	// GetModelVersion returns the model schema version hash, used for detecting schema changes
	GetModelVersion() *string
	// GetRegion returns the region where the change was processed
	GetRegion() string
	// GetOperation returns the ChangeEventOperation
	GetOperation() ChangeEventOperation
	// GetBefore returns the record as a json.RawMessage before this change or nil if not provided
	GetBefore() json.RawMessage
	// GetAfter returns the record as a json.RawMessage after this change or nil if not provided
	GetAfter() json.RawMessage
	// GetDiff returns an array of string keys of the properties that changed
	GetDiff() []string
}

type ChangeEvent struct {
	ID            string               `json:"id"`
	Timestamp     int64                `json:"timestamp"`
	MvccTimestamp string               `json:"mvccTimestamp"`
	Table         string               `json:"table"`
	Key           []string             `json:"key"`
	LocationID    *string              `json:"locationId,omitempty"`
	CompanyID     *string              `json:"companyId,omitempty"`
	UserID        *string              `json:"userId,omitempty"`
	SessionID     *string              `json:"sessionId,omitempty"`
	Version       int64                `json:"version"`
	ModelVersion  *string              `json:"modelVersion,omitempty"`
	Region        string               `json:"region"`
	Operation     ChangeEventOperation `json:"operation"`
	Before        json.RawMessage      `json:"before,omitempty"`
	After         json.RawMessage      `json:"after,omitempty"`
	Diff          []string             `json:"diff,omitempty"`
	TableSchema   Table
}

var _ ChangeEventPayload = (*ChangeEvent)(nil)

// String returns a JSON stringified version of the ChangeEvent
func (c ChangeEvent) String() string {
	buf, err := json.Marshal(c)
	if err != nil {
		return err.Error()
	}
	return string(buf)
}

func (c ChangeEvent) GetID() string {
	return c.ID
}

func (c ChangeEvent) GetTimestamp() int64 {
	return c.Timestamp
}

func (c ChangeEvent) GetMvccTimestamp() string {
	return c.MvccTimestamp
}

func (c ChangeEvent) GetTable() string {
	return c.Table
}

func (c ChangeEvent) GetKey() []string {
	return c.Key
}

func (c ChangeEvent) GetLocationID() *string {
	return c.LocationID
}

func (c ChangeEvent) GetCompanyID() *string {
	return c.CompanyID
}

func (c ChangeEvent) GetUserID() *string {
	return c.UserID
}

func (c ChangeEvent) GetVersion() int64 {
	return c.Version
}

func (c ChangeEvent) GetModelVersion() *string {
	return c.ModelVersion
}

func (c ChangeEvent) GetRegion() string {
	return c.Region
}

func (c ChangeEvent) GetOperation() ChangeEventOperation {
	return c.Operation
}

func (c ChangeEvent) GetBefore() json.RawMessage {
	return c.Before
}

func (c ChangeEvent) GetAfter() json.RawMessage {
	return c.After
}

func (c ChangeEvent) GetDiff() []string {
	return c.Diff
}

// Gunzip will unzip data and return buffer inline
func Gunzip(data []byte) (resData []byte, err error) {
	b := bytes.NewBuffer(data)

	var r io.Reader
	r, err = gzip.NewReader(b)
	if err != nil {
		return
	}

	var resB bytes.Buffer
	_, err = resB.ReadFrom(r)
	if err != nil {
		return
	}

	resData = resB.Bytes()
	return
}

func FromChangeEvent(buf []byte, gzip bool) (*ChangeEvent, error) {
	var result ChangeEvent
	var decompressed = buf
	if gzip {
		dec, err := Gunzip(buf)
		if err != nil {
			return nil, err
		}
		decompressed = dec
	}
	err := json.Unmarshal(decompressed, &result)
	if err != nil {
		return nil, err
	}
	return &result, nil
}
