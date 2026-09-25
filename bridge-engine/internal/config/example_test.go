package config

import "testing"

// The shipped example config must stay valid.
func TestExampleConfigLoads(t *testing.T) {
	if _, err := Load("../../config.example.yaml"); err != nil {
		t.Fatal(err)
	}
	if _, err := Load("../../config.demo.yaml"); err != nil {
		t.Fatal(err)
	}
}
