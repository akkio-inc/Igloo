package config

import "testing"

func TestConsoleAddr(t *testing.T) {
	cfg, err := Load(env(map[string]string{"IGLOO_ADMIN_PASSWORD": "password"}))
	if err != nil {
		t.Fatal(err)
	}
	if cfg.ConsoleAddr != ":8443" {
		t.Errorf("default ConsoleAddr = %q, want :8443", cfg.ConsoleAddr)
	}

	cfg, err = Load(env(map[string]string{"IGLOO_ADMIN_PASSWORD": "password", "IGLOO_CONSOLE_ADDR": ""}))
	if err != nil {
		t.Fatal(err)
	}
	if cfg.ConsoleAddr != "" {
		t.Errorf("ConsoleAddr = %q, want empty to disable the console", cfg.ConsoleAddr)
	}
}
