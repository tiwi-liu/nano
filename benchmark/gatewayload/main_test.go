package main

import (
	"testing"
	"time"
)

func TestValidate(t *testing.T) {
	valid := config{
		addr:        "127.0.0.1:34590",
		transport:   "ws",
		connections: 100,
		rate:        10,
		parallel:    8,
		dialTimeout: time.Second,
		hold:        time.Second,
		reportEvery: time.Second,
	}
	if err := validate(valid); err != nil {
		t.Fatal(err)
	}

	invalidTransport := valid
	invalidTransport.transport = "udp"
	if err := validate(invalidTransport); err == nil {
		t.Fatal("invalid transport was accepted")
	}

	invalidConnections := valid
	invalidConnections.connections = 0
	if err := validate(invalidConnections); err == nil {
		t.Fatal("zero connections was accepted")
	}
}
