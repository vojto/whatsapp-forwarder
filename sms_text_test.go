package main

import (
	"strings"
	"testing"
)

func TestSingleSegmentSMS(t *testing.T) {
	cases := []struct {
		name     string
		input    string
		expected string
	}{
		{"strips diacritics", "Klaudia Komárová: Taký som kúsok smutný, čo?", "Klaudia Komarova: Taky som kusok smutny, co?"},
		{"drops emoji", "Jo idem 💃🏼", "Jo idem"},
		{"converts punctuation", "“Hej” – it’s…", `"Hej" - it's...`},
		{"drops non-latin", "Ahoj Привет", "Ahoj"},
	}
	for _, testCase := range cases {
		if actual := singleSegmentSMS(testCase.input); actual != testCase.expected {
			t.Errorf("%s: got %q, want %q", testCase.name, actual, testCase.expected)
		}
	}
}

func TestSingleSegmentSMSCutsToOneSegment(t *testing.T) {
	long := strings.Repeat("ž", 200)
	actual := singleSegmentSMS(long)
	if gsmUnits(actual) != smsSegmentUnits || !strings.HasSuffix(actual, smsCutMarker) {
		t.Errorf("got %d units: %q", gsmUnits(actual), actual)
	}

	withExtension := strings.Repeat("[", 100)
	if units := gsmUnits(singleSegmentSMS(withExtension)); units > smsSegmentUnits {
		t.Errorf("extension characters exceed one segment: %d units", units)
	}
}

func TestSenderID(t *testing.T) {
	cases := map[string]string{
		"Jakub 🐼":               "Jakub",
		"Klaudia Komárová":      "Klaudia",
		"Ján Novák":             "Jan Novak",
		"Maximilianoslav Bondy": "Maximiliano",
		"🐼🐼":                    "",
		"1234":                  "",
	}
	for name, expected := range cases {
		if actual := senderID(name); actual != expected {
			t.Errorf("senderID(%q) = %q, want %q", name, actual, expected)
		}
	}
}
