package utils

import (
	"fmt"
	"log"
)

func Check(err error) {
	if err != nil {
		log.Fatalf("%+v", err)
	}
}

func AssertTrue(b bool) {
	if !b {
		log.Fatalf("%+v", fmt.Errorf("assert failed"))
	}
}

// AssertTruef is AssertTrue with extra info.
func AssertTruef(b bool, format string, args ...interface{}) {
	if !b {
		log.Fatalf("%+v", fmt.Errorf(format, args...))
	}
}
