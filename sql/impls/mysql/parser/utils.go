package parser

import "strings"

func getPlaceHolders(count int) string {
	return strings.Repeat("?, ", count-1) + "?"
}
