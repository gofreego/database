package parser

import (
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/gofreego/database/sql"
)

var (
	//postgresql operators to string map
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
		sql.REGEXP:     "~",
		sql.BETWEEN:    "BETWEEN",
		sql.NOTBETWEEN: "NOT BETWEEN",
		sql.AND:        "AND",
		sql.OR:         "OR",
		sql.NOT:        "NOT",
	}
)

/*
parseCondition parses the condition and returns the query string and the value indexes
returns
string :: condition string
[]int :: value indexes for PostgreSQL placeholders
*/
func parseCondition(condition *sql.Condition) (string, []int, error) {
	if condition == nil {
		// if condition is nil, return a condition that always returns true
		return "TRUE", nil, nil
	}
	// Validate the condition
	if err := condition.Validate(); err != nil {
		return "", nil, err
	}
	switch condition.Operator {
	case sql.EQ, sql.NEQ, sql.GT, sql.GTE, sql.LT, sql.LTE:
		if condition.Value.IsValue() {
			if condition.Value.IsColumn() {
				if condition.Value.IsStringValue() {
					return fmt.Sprintf("%s %s %s", condition.Field, operatorToStringMap[condition.Operator], condition.Value.Value), nil, nil
				} else {
					return "", nil, sql.NewInvalidQueryError("invalid condition, error: value for %s operator must be a string, field: %s", operatorToStringMap[condition.Operator], condition.Field)
				}
			}
			// If the value is a fixed value, we use it directly
			return fmt.Sprintf("%s %s %s", condition.Field, operatorToStringMap[condition.Operator], getValue(condition.Value.Value)), nil, nil
		}
		return fmt.Sprintf("%s %s $%d", condition.Field, operatorToStringMap[condition.Operator], condition.Value.Index+1), []int{condition.Value.Index}, nil
	case sql.IN, sql.NOTIN:
		if condition.Value.Value != nil {
			// check if value is a slice
			if slice, ok := condition.Value.Value.([]any); ok {
				if len(slice) == 0 {
					return "", nil, sql.NewInvalidQueryError("invalid condition, error: value for IN/NOTIN must be a non-empty slice, field: %s", condition.Field)
				}
				// If the value is fixed we use it directly
				return fmt.Sprintf("%s %s (%s)", condition.Field, operatorToStringMap[condition.Operator], getValueString(slice...)), nil, nil
			} else {
				return "", nil, sql.NewInvalidQueryError("invalid condition, error: value for IN/NOTIN must be a slice, field: %s", condition.Field)
			}
		} else {
			if condition.Value.Count > 0 {
				placeholders := make([]string, condition.Value.Count)
				for i := 0; i < condition.Value.Count; i++ {
					placeholders[i] = "$" + strconv.Itoa(condition.Value.Index+i+1)
				}
				return fmt.Sprintf("%s %s (%s)", condition.Field, operatorToStringMap[condition.Operator], strings.Join(placeholders, ", ")), []int{condition.Value.Index}, nil
			} else {
				return "", nil, sql.NewInvalidQueryError("invalid condition, error: value indexes for IN/NOTIN must be a non-empty slice, field: %s", condition.Field)
			}
		}
	case sql.LIKE, sql.NOTLIKE, sql.REGEXP:
		if condition.Value.Value != nil {
			// check if value is a string
			if str, ok := condition.Value.Value.(string); ok {
				if str == "" {
					return "", nil, sql.NewInvalidQueryError("invalid condition, error: value for LIKE/NOTLIKE must be a non-empty string, field: %s", condition.Field)
				}
				return fmt.Sprintf("%s %s %s", condition.Field, operatorToStringMap[condition.Operator], getValue(str)), nil, nil
			} else {
				return "", nil, sql.NewInvalidQueryError("invalid condition, error: value for LIKE/NOTLIKE must be a string, field: %s", condition.Field)
			}
		} else {
			return fmt.Sprintf("%s %s $%d", condition.Field, operatorToStringMap[condition.Operator], condition.Value.Index+1), []int{condition.Value.Index}, nil
		}
	case sql.ISNULL, sql.ISNOTNULL:
		return fmt.Sprintf("%s %s", condition.Field, operatorToStringMap[condition.Operator]), nil, nil
		// ISNULL and ISNOTNULL do not require a value, so we do not append anything to conditionValues
	case sql.EXISTS, sql.NOTEXISTS:
		// not implemented in this parser, but can be added later
		return "", nil, sql.NewInvalidQueryError("invalid condition, error: EXISTS and NOTEXISTS operators are not implemented in this parser")
	case sql.AND, sql.OR:
		var conditionStrings []string
		var conditionValues []int
		for _, subCondition := range condition.Conditions {
			subConditionString, subConditionValues, err := parseCondition(&subCondition)
			if err != nil {
				return "", nil, sql.NewInvalidQueryError("invalid sub-condition for operator: %s, error: %s", condition.Operator.String(), err.Error())
			}
			if subConditionString != "" {
				conditionStrings = append(conditionStrings, subConditionString)
				conditionValues = append(conditionValues, subConditionValues...)
			}
		}
		if len(conditionStrings) == 0 {
			return "", nil, sql.NewInvalidQueryError("invalid condition, error: no valid sub-conditions found for logical operator: %s", operatorToStringMap[condition.Operator])
		}
		return fmt.Sprintf("(%s)", strings.Join(conditionStrings, fmt.Sprintf(" %s ", operatorToStringMap[condition.Operator]))), conditionValues, nil
	case sql.NOT:
		if len(condition.Conditions) != 1 {
			return "", nil, sql.NewInvalidQueryError("invalid condition, error: NOT operator should have exactly one sub-condition")
		}
		subConditionString, subConditionValues, err := parseCondition(&condition.Conditions[0])
		if err != nil {
			return "", nil, sql.NewInvalidQueryError("invalid sub-condition for NOT operator: %s", err.Error())
		}
		if subConditionString == "" {
			return "", nil, sql.NewInvalidQueryError("invalid condition, error: no valid sub-condition found for NOT operator: %s", condition.Field)
		}
		return fmt.Sprintf("NOT (%s)", subConditionString), subConditionValues, nil
	case sql.BETWEEN, sql.NOTBETWEEN:
		if condition.Value.Value != nil {
			// check if value is a slice of length 2
			if slice, ok := condition.Value.Value.([]any); ok {
				if len(slice) != 2 {
					return "", nil, sql.NewInvalidQueryError("invalid condition, error: value for BETWEEN/NOTBETWEEN must be a slice of length 2, field: %s", condition.Field)
				}
				// If the value is fixed we use it directly
				return fmt.Sprintf("%s %s %s AND %s", condition.Field, operatorToStringMap[condition.Operator], getValueString(slice[0]), getValueString(slice[1])), nil, nil
			} else {
				return "", nil, sql.NewInvalidQueryError("invalid condition, error: value for BETWEEN/NOTBETWEEN must be a slice of length 2, field: %s", condition.Field)
			}
		} else {
			return fmt.Sprintf("(%s %s $%d AND $%d)", condition.Field, operatorToStringMap[condition.Operator], condition.Value.Index+1, condition.Value.Index+2), []int{condition.Value.Index}, nil
		}
	default:
		return "", nil, sql.NewInvalidQueryError("invalid condition, error: invalid operator: %d, for field: %s", condition.Operator, condition.Field)
	}
}

func getValueString(values ...any) string {
	var valueStrings []string
	for _, value := range values {
		valueStrings = append(valueStrings, getValue(value))
	}
	return strings.Join(valueStrings, ", ")
}

func getValue(value any) string {
	switch v := value.(type) {
	case string:
		return fmt.Sprintf("'%s'", v)
	case time.Time:
		return fmt.Sprintf("'%s'", v.Format(time.RFC3339))
	default:
		return fmt.Sprintf("%v", v)
	}
}
