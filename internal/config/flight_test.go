package config

import "testing"

func TestFlightAddr(t *testing.T) {
	cfg, err := Load(env(map[string]string{"IGLOO_ADMIN_PASSWORD": "password"}))
	if err != nil {
		t.Fatal(err)
	}
	if cfg.FlightAddr != ":31337" {
		t.Errorf("default FlightAddr = %q, want :31337", cfg.FlightAddr)
	}

	cfg, err = Load(env(map[string]string{"IGLOO_ADMIN_PASSWORD": "password", "IGLOO_FLIGHT_ADDR": ""}))
	if err != nil {
		t.Fatal(err)
	}
	if cfg.FlightAddr != "" {
		t.Errorf("FlightAddr = %q, want empty to turn Flight SQL off", cfg.FlightAddr)
	}
}
