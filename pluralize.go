package microrm

import "strings"

func pluralize(word string) string {
	lower := strings.ToLower(word)

	if strings.HasSuffix(lower, "s") || strings.HasSuffix(lower, "x") ||
		strings.HasSuffix(lower, "ch") || strings.HasSuffix(lower, "sh") {
		return word + "es"
	}

	if strings.HasSuffix(lower, "y") && len(word) > 1 &&
		!strings.ContainsRune("aeiou", rune(lower[len(lower)-2])) {
		return word[:len(word)-1] + "ies"
	}

	if strings.HasSuffix(lower, "f") {
		return word[:len(word)-1] + "ves"
	}

	if strings.HasSuffix(lower, "fe") {
		return word[:len(word)-2] + "ves"
	}

	return word + "s"
}
