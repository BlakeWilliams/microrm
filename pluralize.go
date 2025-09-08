package dbmap

import "strings"

var defaultPluralizer = basicPluralizer{}

type basicPluralizer struct{}

func (b basicPluralizer) Pluralize(word string) string {
	lower := strings.ToLower(word)

	if plural, ok := irregulars[lower]; ok {
		return plural
	}

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

	if strings.HasSuffix(lower, "o") && len(word) > 1 &&
		!strings.ContainsRune("aeiou", rune(lower[len(lower)-2])) {

		if _, exists := oExceptions[lower]; exists {
			return word + "s"
		}

		return word + "es"
	}

	return word + "s"
}

var irregulars = map[string]string{
	// People
	"person": "people",
	"man":    "men",
	"woman":  "women",
	"child":  "children",
	"human":  "humans",

	// Body parts
	"tooth": "teeth",
	"foot":  "feet",
	"goose": "geese",
	"mouse": "mice",
	"louse": "lice",
	"ox":    "oxen",

	// Animals
	"deer":    "deer",
	"sheep":   "sheep",
	"fish":    "fish",
	"moose":   "moose",
	"swine":   "swine",
	"buffalo": "buffalo",
	"bison":   "bison",
	"salmon":  "salmon",
	"trout":   "trout",
	"species": "species",
	"series":  "series",

	// Data/Tech
	"datum":  "data",
	"medium": "media",
	"forum":  "forums",
	"virus":  "viruses",
	"status": "statuses",
	"campus": "campuses",
	"corpus": "corpora",
	"genus":  "genera",

	// Latin/Greek
	"alumnus":     "alumni",
	"alumna":      "alumnae",
	"analysis":    "analyses",
	"axis":        "axes",
	"basis":       "bases",
	"crisis":      "crises",
	"diagnosis":   "diagnoses",
	"ellipsis":    "ellipses",
	"hypothesis":  "hypotheses",
	"oasis":       "oases",
	"parenthesis": "parentheses",
	"synopsis":    "synopses",
	"thesis":      "theses",
	"phenomenon":  "phenomena",
	"criterion":   "criteria",
	"bacterium":   "bacteria",
	"curriculum":  "curricula",
	"memorandum":  "memoranda",
	"millennium":  "millennia",
	"stadium":     "stadiums",
	"aquarium":    "aquariums",
	"gymnasium":   "gymnasiums",
	"auditorium":  "auditoriums",
	"emporium":    "emporiums",

	// Uncountable (same singular/plural)
	"advice":      "advice",
	"aircraft":    "aircraft",
	"bread":       "bread",
	"butter":      "butter",
	"chess":       "chess",
	"clothing":    "clothing",
	"coal":        "coal",
	"evidence":    "evidence",
	"feedback":    "feedback",
	"furniture":   "furniture",
	"gold":        "gold",
	"homework":    "homework",
	"honey":       "honey",
	"information": "information",
	"jewelry":     "jewelry",
	"luggage":     "luggage",
	"money":       "money",
	"music":       "music",
	"news":        "news",
	"oil":         "oil",
	"oxygen":      "oxygen",
	"paper":       "paper",
	"permission":  "permission",
	"research":    "research",
	"rice":        "rice",
	"sand":        "sand",
	"software":    "software",
	"sugar":       "sugar",
	"traffic":     "traffic",
	"travel":      "travel",
	"trouble":     "trouble",
	"water":       "water",
	"weather":     "weather",
	"wood":        "wood",
	"work":        "work",

	// Common business/ORM terms
	"staff":        "staff",
	"equipment":    "equipment",
	"headquarters": "headquarters",
}

var oExceptions = map[string]bool{
	"audio":  true,
	"auto":   true,
	"cameo":  true,
	"casino": true,
	"combo":  true,
	"disco":  true,
	"embryo": true,
	"euro":   true,
	"folio":  true,
	"halo":   true,
	"kilo":   true,
	"logo":   true,
	"macro":  true,
	"memo":   true,
	"metro":  true,
	"micro":  true,
	"nano":   true,
	"patio":  true,
	"photo":  true,
	"piano":  true,
	"pro":    true,
	"radio":  true,
	"ratio":  true,
	"retro":  true,
	"romeo":  true,
	"solo":   true,
	"stereo": true,
	"studio": true,
	"tempo":  true,
	"video":  true,
	"zero":   true,
}
