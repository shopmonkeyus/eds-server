package internal

import (
	dm "github.com/shopmonkeyus/eds-server/internal/model"
	"github.com/shopmonkeyus/eds-server/internal/types"
)

type Provider interface {
	// Start the provider and return an error or nil if ok
	Start() error
	// Stop the provider and return an error or nil if ok
	Stop() error
	// Process data received and return an error or nil if processed ok
	Process(data types.ChangeEventPayload, schema dm.Model) error
}
