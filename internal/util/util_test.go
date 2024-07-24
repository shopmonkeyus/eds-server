package util

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestToFileURI(t *testing.T) {
	fileURL := ToFileURI("/var/folders/60/rf284h4d67g343wcswq6jwmr0000gn/T/eds-import2764310919", "*.ndjson.gz")
	assert.Equal(t, "file:///var/folders/60/rf284h4d67g343wcswq6jwmr0000gn/T/eds-import2764310919/*.ndjson.gz", fileURL)
	fileURL = ToFileURI("/var/folders/60/rf284h4d67g343wcswq6jwmr0000gn/T/eds-import2764310919/", "*.ndjson.gz")
	assert.Equal(t, "file:///var/folders/60/rf284h4d67g343wcswq6jwmr0000gn/T/eds-import2764310919/*.ndjson.gz", fileURL)
}
