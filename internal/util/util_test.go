package util

import (
	"errors"
	"math/big"
	"reflect"
	"testing"
)

func TestMaskConnectionString(t *testing.T) {
	tests := []struct {
		name                     string
		originalConnectionString string
		expectedMaskedString     string
	}{
		{
			name:                     "Valid Snowflake Connection String",
			originalConnectionString: "snowflake://jsmith:pasdhowdh-ghx@tflxsoy-lt41015/mydb/PUBLIC?warehouse=COMPUTE_WH&client_session_keep_alive=true",
			expectedMaskedString:     "snowflake://*****:*****@tflxsoy-lt41015/mydb/PUBLIC?warehouse=COMPUTE_WH&client_session_keep_alive=true",
		},
		{
			name:                     "Another Valid Connection String",
			originalConnectionString: "snowflake://user:password@server:port/database?param=value",
			expectedMaskedString:     "snowflake://*****:*****@server:port/database?param=value",
		},
		{
			name:                     "Valid Postgres Connection String",
			originalConnectionString: "postgresql://postgres:$PGPASS@localhost:5432/shopmonkey?sslmode=disable",
			expectedMaskedString:     "postgresql://*****:*****@localhost:5432/shopmonkey?sslmode=disable",
		},
		{
			name:                     "Another Valid Postgres Connection String",
			originalConnectionString: "postgresql://postgresUserName:ReallyLongPasswordHereForTesting123!!!##@localhost:5432/shopmonkey?sslmode=disable",
			expectedMaskedString:     "postgresql://*****:*****@localhost:5432/shopmonkey?sslmode=disable",
		},
		{
			name:                     "Valid Sql Server Connection String",
			originalConnectionString: "sqlserver://sa:$PGPASS@localhost:5432/shopmonkey?sslmode=disable",
			expectedMaskedString:     "sqlserver://*****:*****@localhost:5432/shopmonkey?sslmode=disable",
		},
		{
			name:                     "Another Valid Sql Server Connection String",
			originalConnectionString: "sqlserver://sqlUserName:p@ssw..rd.a.!!!##@localhost:5432/shopmonkey?sslmode=disable",
			expectedMaskedString:     "sqlserver://*****:*****@localhost:5432/shopmonkey?sslmode=disable",
		},
		{
			name:                     "Invalid Connection String",
			originalConnectionString: "invalid-connection-string",
			expectedMaskedString:     "invalid-connection-string",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			maskedString := MaskConnectionString(tt.originalConnectionString)

			if maskedString != tt.expectedMaskedString {
				t.Errorf("Got %s, expected %s", maskedString, tt.expectedMaskedString)
			}
		})
	}
}

func TestTryConvertJson(t *testing.T) {
	floatTestExpectedValue, _ := big.NewFloat(1.70731828833e+12).Float64()
	tests := []struct {
		fieldType string
		val       interface{}
		expected  interface{}
		err       error
	}{
		// Test with a map (should return JSON string)
		{"", map[string]interface{}{"key": "value"}, `{"key":"value"}`, nil},
		// Test with a slice (should return JSON string)
		{"", []interface{}{"value1", "value2"}, `["value1","value2"]`, nil},
		// Test with fieldType as "datetime"
		{"datetime", "2021-06-11T15:04:05Z", `"2021-06-11T15:04:05Z"`, nil},
		// Test with fieldType as other types (should return the value as is)
		{"String", "stringValue", "stringValue", nil},
		{"BigInt", int64(1234567890), int64(1234567890), nil},
		{"Int", int(42), int(42), nil},
		{"Double", float64(3.14), float64(3.14), nil},
		{"Float", 1.70731828833e+12, floatTestExpectedValue, nil},
		{"Boolean", true, true, nil},
		{"Json", `{"key": "value"}`, `{"key": "value"}`, nil},
		{"Bytes", []byte{0x01, 0x02, 0x03}, []byte{0x01, 0x02, 0x03}, nil},
		{"Decimal", "123.45", "123.45", nil},
		// Test with invalid inputs (should return input as is)
		{"", 123, 123, nil},
		{"", nil, nil, nil},
	}

	for _, test := range tests {
		result, err := TryConvertJson(test.fieldType, test.val)
		if !reflect.DeepEqual(result, test.expected) || !errors.Is(err, test.err) {
			t.Errorf("TryConvertJson(%v, %v) = %v, %v; want %v, %v",
				test.fieldType, test.val, result, err, test.expected, test.err)
		}
	}
}
