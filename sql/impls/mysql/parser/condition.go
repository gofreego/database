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
	switch condition.Operator {
	case sql.EQ, sql.NEQ, sql.GT, sql.GTE, sql.LT, sql.LTE:
		if condition.Field == "" {
			return "", nil, fmt.Errorf("failed to parse condition, error: field is empty")
		}
		return fmt.Sprintf("%s %s ?", condition.Field, operatorToStringMap[condition.Operator]), []any{condition.Value}, nil

	case sql.IN, sql.NOTIN:
		if condition.Field == "" {
			return "", nil, fmt.Errorf("failed to parse condition, error: field is empty")
		}
		if values, ok := condition.Value.([]any); ok && len(values) > 0 {
			return fmt.Sprintf("%s %s (%s)", condition.Field, operatorToStringMap[condition.Operator], getPlaceHolders(len(values))), values, nil
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
			return fmt.Sprintf("%s %s ?", condition.Field, operatorToStringMap[condition.Operator]), []any{value}, nil
		}

	case sql.ISNULL, sql.ISNOTNULL:
		if condition.Field == "" {
			return "", nil, fmt.Errorf("failed to parse condition, error: field is empty")
		}
		return fmt.Sprintf("%s %s", condition.Field, operatorToStringMap[condition.Operator]), nil, nil
		// ISNULL and ISNOTNULL do not require a value, so we do not append anything to conditionValues
	case sql.EXISTS, sql.NOTEXISTS:
		// not implemented in this parser, but can be added later
		return "", nil, fmt.Errorf("failed to parse condition, error: EXISTS and NOTEXISTS operators are not implemented in this parser")
	case sql.REGEXP:
		if condition.Field == "" {
			return "", nil, fmt.Errorf("failed to parse condition, error: field is empty")
		}
		if value, ok := condition.Value.(string); !ok {
			return "", nil, fmt.Errorf("failed to parse condition, error: value for REGEXP must be a string")
		} else {
			return fmt.Sprintf("%s %s ?", condition.Field, operatorToStringMap[condition.Operator]), []any{value}, nil
		}
	case sql.AND, sql.OR:
		if condition.Field != "" {
			return "", nil, fmt.Errorf("failed to parse condition, error: field should be empty for logical operators: %s", operatorToStringMap[condition.Operator])
		}
		if len(condition.Conditions) == 0 {
			return "", nil, fmt.Errorf("failed to parse condition, error: conditions should not be empty for logical operators: %s", operatorToStringMap[condition.Operator])
		}
		var conditionStrings []string
		var conditionValues []any
		for _, subCondition := range condition.Conditions {
			subConditionString, subConditionValues, err := parseCondition(&subCondition)
			if err != nil {
				return "", nil, fmt.Errorf("failed to parse sub-condition: %w", err)
			}
			if subConditionString != "" {
				conditionStrings = append(conditionStrings, subConditionString)
				conditionValues = append(conditionValues, subConditionValues...)
			}
		}
		if len(conditionStrings) == 0 {
			return "", nil, fmt.Errorf("failed to parse condition, error: no valid sub-conditions found for logical operator: %s", operatorToStringMap[condition.Operator])
		}
		return fmt.Sprintf("(%s)", strings.Join(conditionStrings, fmt.Sprintf(" %s ", operatorToStringMap[condition.Operator]))), conditionValues, nil
	case sql.NOT:
		if condition.Field != "" {
			return "", nil, fmt.Errorf("failed to parse condition, error: field should be emptyfor NOT operator")
		}
		if len(condition.Conditions) != 1 {
			return "", nil, fmt.Errorf("failed to parse condition, error: NOT operator should have exactly one sub-condition")
		}
		subConditionString, subConditionValues, err := parseCondition(&condition.Conditions[0])
		if err != nil {
			return "", nil, fmt.Errorf("failed to parse sub-condition for NOT operator: %w", err)
		}
		if subConditionString == "" {
			return "", nil, fmt.Errorf("failed to parse condition, error: sub-condition for NOT operator should not be empty")
		}
		return fmt.Sprintf("NOT (%s)", subConditionString), subConditionValues, nil
	case sql.BETWEEN, sql.NOTBETWEEN:
		if condition.Field == "" {
			return "", nil, fmt.Errorf("failed to parse condition, error: field is empty")
		}
		if values, ok := condition.Value.([]any); ok && len(values) == 2 {
			return fmt.Sprintf("%s %s ? AND ?", condition.Field, operatorToStringMap[condition.Operator]), []any{values[0], values[1]}, nil
		} else {
			return "", nil, fmt.Errorf("failed to parse condition, error: value for BETWEEN/NOTBETWEEN must be a slice of two elements")
		}

	default:
		return "", nil, fmt.Errorf("failed to parse condition, error: invalid operator: %d, for field: %s", condition.Operator, condition.Field)
	}
}
