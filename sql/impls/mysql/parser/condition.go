package parser

import (
	"fmt"
	"strings"

	"github.com/gofreego/database/sql"
)

var (
	//mysql operators to string map
	operatorToStringMap = map[sql.Operator]string{
		sql.EQ:         "=",
		sql.NEQ:        "<>",
		sql.GT:         ">",
		sql.GTE:        ">=",
		sql.LT:         "<",
		sql.LTE:        "<=",
		sql.IN:         "IN",
		sql.NOTIN:      "NOT IN",
		sql.LIKE:       "LIKE",
		sql.NOTLIKE:    "NOT LIKE",
		sql.ISNULL:     "IS NULL",
		sql.ISNOTNULL:  "IS NOT NULL",
		sql.EXISTS:     "EXISTS",
		sql.NOTEXISTS:  "NOT EXISTS",
		sql.REGEXP:     "REGEXP",
		sql.BETWEEN:    "BETWEEN",
		sql.NOTBETWEEN: "NOT BETWEEN",
		sql.AND:        "AND",
		sql.OR:         "OR",
		sql.NOT:        "NOT",
	}
)

/*
parseCondition parses the condition and returns the query string and the values
returns
string :: condition string
[]any :: values
*/
func parseCondition(condition *sql.Condition) (string, []any, error) {
	if condition == nil {
		return "", nil, nil
	}
	var conditionStrings []string = []string{}
	var conditionValues []any = []any{}
	switch condition.Operator {
	case sql.EQ, sql.NEQ, sql.GT, sql.GTE, sql.LT, sql.LTE:
		if condition.Field == "" {
			return "", nil, fmt.Errorf("failed to parse condition, error: field is empty")
		}
		conditionStrings = append(conditionStrings, fmt.Sprintf("%s %s ?", condition.Field, operatorToStringMap[condition.Operator]))
		conditionValues = append(conditionValues, condition.Value)

	case sql.IN, sql.NOTIN:
		if condition.Field == "" {
			return "", nil, fmt.Errorf("failed to parse condition, error: field is empty")
		}
		if values, ok := condition.Value.([]any); ok && len(values) > 0 {
			conditionStrings = append(conditionStrings, fmt.Sprintf("%s %s (%s)", condition.Field, operatorToStringMap[condition.Operator], getPlaceHolders(len(values))))
			conditionValues = append(conditionValues, values...)
		} else {
			return "", nil, fmt.Errorf("failed to parse condition, error: value for IN/NOTIN must be a non-empty slice")
		}
	case sql.LIKE, sql.NOTLIKE:
		if condition.Field == "" {
			return "", nil, fmt.Errorf("failed to parse condition, error: field is empty")
		}
		if value, ok := condition.Value.(string); !ok {
			return "", nil, fmt.Errorf("failed to parse condition, error: value for LIKE/NOTLIKE must be a string")
		} else {
			conditionStrings = append(conditionStrings, fmt.Sprintf("%s %s ?", condition.Field, operatorToStringMap[condition.Operator]))
			conditionValues = append(conditionValues, value)
		}
	case sql.ISNULL, sql.ISNOTNULL:
		if condition.Field == "" {
			return "", nil, fmt.Errorf("failed to parse condition, error: field is empty")
		}
		conditionStrings = append(conditionStrings, fmt.Sprintf("%s %s", condition.Field, operatorToStringMap[condition.Operator]))
		// ISNULL and ISNOTNULL do not require a value, so we do not append anything to conditionValues
	case sql.EXISTS, sql.NOTEXISTS:
		if condition.Field == "" {
			return "", nil, fmt.Errorf("failed to parse condition, error: field is empty")
		}

	default:
		return "", nil, fmt.Errorf("failed to parse condition, error: invalid operator: %d, for field: %s", condition.Operator, condition.Field)
	}
	return "WHERE " + strings.Join(conditionStrings, " AND "), conditionValues, nil
}
