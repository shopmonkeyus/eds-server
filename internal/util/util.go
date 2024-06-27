package util

import (
	"encoding/json"
	"fmt"

	"regexp"
	"sort"
	"strings"

	"github.com/nats-io/nats-server/v2/server"
)

type Dialect string

const (
	Postgresql Dialect = "postgresql"
	Sqlserver  Dialect = "sqlserver"
	Snowflake  Dialect = "snowflake"
)

func QuoteJoin(vals []string, quote string, sep string) string {
	res := make([]string, 0)
	for _, val := range vals {
		res = append(res, quote+val+quote)
	}
	return strings.Join(res, sep)
}

func SafeCompare(a []string, b []string) bool {
	_a := make([]string, len(a))
	_b := make([]string, len(b))
	copy(_a, a)
	copy(_b, b)
	sort.Strings(_a)
	sort.Strings(_b)
	return strings.Join(_a, "") == strings.Join(_b, "")
}

func Exists(a []string, b string) bool {
	for _, v := range a {
		if v == b {
			return true
		}
	}
	return false
}

func EndsWithSuffix(val string, suffixes ...string) bool {
	for _, suffix := range suffixes {
		if strings.HasSuffix(val, suffix) {
			return true
		}
	}
	return false
}

func Dequote(val string) string {
	return Trim(Trim(strings.ReplaceAll(val, `\"`, `"`), `"`, `"`), "'", "'")
}

func Trim(val string, prefix string, suffix string) string {
	if strings.HasPrefix(val, prefix) && strings.HasSuffix(val, suffix) {
		return val[len(prefix) : len(val)-len(suffix)]
	}
	return val
}

func Tokenize(val string) []string {
	tokens := strings.Split(val, ",")
	results := make([]string, 0)
	for _, token := range tokens {
		results = append(results, strings.TrimSpace(token))
	}
	return results
}

func TokenizeWrap(tokens []string, prefix string, suffix string) []string {
	results := make([]string, 0)
	for _, token := range tokens {
		results = append(results, prefix+strings.TrimSpace(token)+suffix)
	}
	return results
}

func AddIfNotExists(val string, slice *[]string) bool {
	for _, line := range *slice {
		if line == val {
			return false
		}
	}
	*slice = append(*slice, val)
	return true
}

var ctre = regexp.MustCompile(`(_[a-z])`)

func CamelToTitleCase(val string) string {
	val = strings.ToLower(val)
	val = ctre.ReplaceAllStringFunc(val, func(group string) string {
		return strings.ReplaceAll(strings.ToUpper(group), "_", " ")
	})
	return strings.ToUpper(val[0:1]) + val[1:]
}

func MustJSONStringify(val interface{}, pretty bool) string {
	if pretty {
		buf, err := json.MarshalIndent(val, "", " ")
		if err != nil {
			panic(err)
		}
		return string(buf)
	}
	buf, err := json.Marshal(val)
	if err != nil {
		panic(err)
	}
	return string(buf)
}

func Dedupe(vals []string) []string {
	dedupe := make(map[string]bool)
	results := make([]string, 0)
	// maintain the original order, just removing the duplicates
	for _, val := range vals {
		if !dedupe[val] {
			results = append(results, val)
			dedupe[val] = true
		}
	}
	return results
}

func TryConvertJson(fieldType string, val interface{}) (interface{}, error) {
	if v, ok := val.(map[string]interface{}); ok {
		jsonData, err := json.Marshal(v)
		if err != nil {
			return "", err
		}
		return string(jsonData), nil
	}
	if v, ok := val.([]interface{}); ok {
		jsonData, err := json.Marshal(v)
		if err != nil {
			return "", err
		}
		return string(jsonData), nil
	}
	if fieldType == "datetime" {
		jsonData, err := json.Marshal(val)
		if err != nil {
			return "", err
		}
		return string(jsonData), nil

	}
	return val, nil
}

func MaskConnectionString(connectionString string) string {
	re := regexp.MustCompile(`(.*://)(.*?:)(.*)(@.*)`)
	maskedConnectionString := re.ReplaceAllString(connectionString, "$1*****:*****$4")

	return maskedConnectionString
}

func DetermineSnowflakeConversion(newType string) (string, error) {
	switch strings.ToLower(newType) {
	case "date":
		return "TO_DATE", nil
	case "time":
		return "TO_TIME", nil
	case "timestamp":
		return "TO_TIMESTAMP", nil
	case "timestampltz":
		return "TO_TIMESTAMP_LTZ", nil
	case "timestamptz":
		return "TO_TIMESTAMP_TZ", nil
	case "variant", "string":
		return "TO_VARIANT", nil
	case "object":
		return "TO_OBJECT", nil
	case "array":
		return "TO_ARRAY", nil
	case "binary":
		return "TO_BINARY", nil
	case "boolean":
		return "TO_BOOLEAN", nil
	case "number", "integer":
		return "TO_NUMBER", nil
	case "float":
		return "TO_FLOAT", nil
	case "decimal":
		return "TO_DECIMAL", nil
	case "double":
		return "TO_DOUBLE", nil
	default:
		return "", fmt.Errorf("unsupported snowflake conversion for %s", newType)
	}
}

// LoadConfig loads a configuration from a filename
func LoadConfig(configFile string) *server.Options {
	opts, err := server.ProcessConfigFile(configFile)
	if err != nil {
		panic(fmt.Sprintf("Error processing configuration file: %v", err))
	}
	return opts
}

func ExtractCompanyIdFromSubscription(sub string) string {
	parts := strings.Split(sub, ".")
	if len(parts) > 3 {
		return parts[3]
	}
	return ""
}
