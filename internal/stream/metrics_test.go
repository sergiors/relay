package stream

import (
	"fmt"
	"testing"
	"time"
)

func TestPendingAgeParsing(t *testing.T) {
	age, ok := pendingAge("1234567-0")
	if !ok {
		t.Fatal("expected 1234567-0 to parse")
	}
	want := time.Since(time.UnixMilli(1234567))
	if age < want-2*time.Second || age > want+2*time.Second {
		t.Errorf("age = %v, want ~%v for %q", age, want, "1234567-0")
	}
}

func TestPendingAgeFreshID(t *testing.T) {
	ms := time.Now().UnixMilli()
	age, ok := pendingAge(fmt.Sprintf("%d-0", ms))
	if !ok {
		t.Fatalf("expected %d-0 to parse", ms)
	}
	if age < 0 || age > 2*time.Second {
		t.Errorf("fresh id age = %v, want ~0", age)
	}
}

func TestPendingAgeInvalidInputs(t *testing.T) {
	cases := []string{
		"",
		"-",
		"abc",
		"no-dash",
		"1234567", // no seq part
		"-0",
	}
	for _, c := range cases {
		if _, ok := pendingAge(c); ok {
			t.Errorf("expected %q to fail to parse", c)
		}
	}
}
