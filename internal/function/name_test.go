package function

import (
	"strings"
	"testing"
)

func TestValidName(t *testing.T) {
	valid := []string{
		"user-events",
		"welcome_email",
		"jobs.v2",
		"a",
		"a1",
		"a-b_c.d",
	}
	for _, name := range valid {
		if err := ValidName(name); err != nil {
			t.Errorf("ValidName(%q) = %v, want nil", name, err)
		}
	}
}

func TestValidNameRejects(t *testing.T) {
	invalid := []string{
		"User Events", // space + uppercase
		"hello/world", // path separator
		"-lead",       // starts with '-'
		".lead",       // starts with '.'
		"trail.",      // ends with '.'
		"UPPER",       // uppercase
		"",            // empty
	}
	for _, name := range invalid {
		if err := ValidName(name); err == nil {
			t.Errorf("ValidName(%q) = nil, want error", name)
		}
	}
}

func TestValidNameTooLong(t *testing.T) {
	// maxNameLen characters are acceptable, one more is not.
	ok := strings.Repeat("a", maxNameLen)
	if err := ValidName(ok); err != nil {
		t.Errorf("ValidName(%d 'a') = %v, want nil", maxNameLen, err)
	}
	if err := ValidName(ok + "a"); err == nil {
		t.Errorf("ValidName(%d 'a') = nil, want error", maxNameLen+1)
	}
}
