package parser

import (
	"fmt"
	"strings"
	"time"

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
func parseCondition(condition *sql.Condition) (string, []*sql.Value, error) {
	if condition == nil {
		return "", nil, nil
	}
	// Validate the condition
	if err := condition.Validate(); err != nil {
		return "", nil, err
	}
	switch condition.Operator {
	case sql.EQ, sql.NEQ, sql.GT, sql.GTE, sql.LT, sql.LTE:
		if condition.Value.Value != nil {
			// If the value is a fixed value, we use it directly
			return fmt.Sprintf("%s %s %v", condition.Field, operatorToStringMap[condition.Operator], condition.Value.Value), nil, nil
		}
		return fmt.Sprintf("%s %s ?", condition.Field, operatorToStringMap[condition.Operator]), []*sql.Value{condition.Value}, nil
	case sql.IN, sql.NOTIN:
		if condition.Value.Value != nil {
			// check if value is a slice
			if slice, ok := condition.Value.Value.([]any); ok {
				if len(slice) == 0 {
					return "", nil, sql.NewInvalidQueryError("invalid condition, error: value for IN/NOTIN must be a non-empty slice, field: %s", condition.Field)
				}
				// If the value is fixed we use it directly
				return fmt.Sprintf("%s %s (%s)", condition.Field, operatorToStringMap[condition.Operator], getValueString(slice)), nil, nil
			} else {
				return "", nil, sql.NewInvalidQueryError("invalid condition, error: value for IN/NOTIN must be a slice, field: %s", condition.Field)
			}
		} else {
			if condition.Value.Count > 0 {
				return fmt.Sprintf("%s %s (%s)", condition.Field, operatorToStringMap[condition.Operator], getPlaceHolders(condition.Value.Count)), []*sql.Value{condition.Value.WithType(sql.Array)}, nil
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
				return fmt.Sprintf("%s %s '%s'", condition.Field, operatorToStringMap[condition.Operator], str), nil, nil
			} else {
				return "", nil, sql.NewInvalidQueryError("invalid condition, error: value for LIKE/NOTLIKE must be a string, field: %s", condition.Field)
			}
		} else {
			return fmt.Sprintf("%s %s ?", condition.Field, operatorToStringMap[condition.Operator]), []*sql.Value{condition.Value.WithType(sql.String)}, nil
		}
	case sql.ISNULL, sql.ISNOTNULL:
		return fmt.Sprintf("%s %s", condition.Field, operatorToStringMap[condition.Operator]), nil, nil
		// ISNULL and ISNOTNULL do not require a value, so we do not append anything to conditionValues
	case sql.EXISTS, sql.NOTEXISTS:
		// not implemented in this parser, but can be added later
		return "", nil, sql.NewInvalidQueryError("invalid condition, error: EXISTS and NOTEXISTS operators are not implemented in this parser")
	case sql.AND, sql.OR:
		var conditionStrings []string
		var conditionValues []*sql.Value
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
			return fmt.Sprintf("(%s %s ? AND ?)", condition.Field, operatorToStringMap[condition.Operator]), []*sql.Value{condition.Value.WithType(sql.Array).WithCount(2)}, nil
		}
	default:
		return "", nil, sql.NewInvalidQueryError("invalid condition, error: invalid operator: %d, for field: %s", condition.Operator, condition.Field)
	}
}

func getValueString(values ...any) string {
	var valueStrings []string
	for _, value := range values {
		switch v := value.(type) {
		case string:
			valueStrings = append(valueStrings, fmt.Sprintf("'%s'", v))
		case time.Time:
			valueStrings = append(valueStrings, fmt.Sprintf("'%s'", v.Format(time.RFC3339)))
		default:
			valueStrings = append(valueStrings, fmt.Sprintf("%v", v))
		}
	}
	return strings.Join(valueStrings, ", ")
}
