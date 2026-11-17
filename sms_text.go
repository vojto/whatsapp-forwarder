package main

// Expected behavior:
//   - singleSegmentSMS turns any text into plain GSM-7 that fits in one SMS segment (160 units),
//     so every forward costs exactly one segment.
//   - Diacritics are stripped (č → c), typographic punctuation becomes ASCII, and anything else
//     outside GSM-7 (emoji, non-Latin scripts) is dropped. Long text is cut and ends with "...".
//
// Gotchas:
//   - A single non-GSM character switches the whole SMS to UCS-2 (70 characters per segment),
//     which is why unknown characters are dropped rather than kept.
//   - GSM-7 extension characters (^ { } \ [ ] ~ |) take two units each.
//   - Alphanumeric sender IDs allow at most 11 letters, digits and spaces, with at least one letter.

import (
	"strings"
	"unicode"

	"golang.org/x/text/unicode/norm"
)

const smsSegmentUnits = 160
const maxSenderIDLength = 11
const smsCutMarker = "..."

var asciiReplacements = strings.NewReplacer(
	"‘", "'", "’", "'", "‚", "'", "`", "'",
	"“", "\"", "”", "\"", "„", "\"",
	"–", "-", "—", "-", "…", "...",
	" ", " ", "\t", " ",
	"ł", "l", "Ł", "L", "đ", "d", "Đ", "D", "ß", "ss", "ø", "o", "Ø", "O", "æ", "ae", "Æ", "AE",
)

// Lifecycle
// ---------------------------------------------------------------------------

func singleSegmentSMS(text string) string {
	plain := toGSMText(text)
	if gsmUnits(plain) <= smsSegmentUnits {
		return plain
	}
	return cutToUnits(plain, smsSegmentUnits-len(smsCutMarker)) + smsCutMarker
}

// senderID turns a contact name into an alphanumeric SMS sender ID ("Klaudia Komárová" → "Klaudia"),
// or "" if the name has no usable letters.
func senderID(name string) string {
	var builder strings.Builder
	for _, character := range toGSMText(name) {
		if unicode.IsLetter(character) || unicode.IsDigit(character) || character == ' ' {
			builder.WriteRune(character)
		}
	}
	words := strings.Fields(builder.String())
	if len(words) == 0 || !strings.ContainsFunc(builder.String(), unicode.IsLetter) {
		return ""
	}
	full := strings.Join(words, " ")
	if len(full) <= maxSenderIDLength {
		return full
	}
	if len(words[0]) <= maxSenderIDLength {
		return words[0]
	}
	return words[0][:maxSenderIDLength]
}

// Conversion
// ---------------------------------------------------------------------------

func toGSMText(text string) string {
	decomposed := norm.NFD.String(asciiReplacements.Replace(text))
	var builder strings.Builder
	for _, character := range decomposed {
		if unicode.Is(unicode.Mn, character) {
			continue
		}
		if character == '\n' || character == '\r' || (character >= ' ' && character <= '~') {
			builder.WriteRune(character)
		}
	}
	return strings.TrimSpace(builder.String())
}

// Length
// ---------------------------------------------------------------------------

func gsmUnits(text string) int {
	units := 0
	for _, character := range text {
		units += characterUnits(character)
	}
	return units
}

func cutToUnits(text string, maxUnits int) string {
	units := 0
	for index, character := range text {
		units += characterUnits(character)
		if units > maxUnits {
			return strings.TrimSpace(text[:index])
		}
	}
	return text
}

func characterUnits(character rune) int {
	if strings.ContainsRune(`^{}\[]~|`, character) {
		return 2
	}
	return 1
}
