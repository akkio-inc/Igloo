package config

import (
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"
)

func TestNotifications(t *testing.T) {
	cfg, err := Load(env(map[string]string{"IGLOO_ADMIN_PASSWORD": "password"}))
	if err != nil {
		t.Fatal(err)
	}
	if len(cfg.Notifications.AllowHosts) != 0 || cfg.Notifications.SMTP.Enabled() {
		t.Errorf("defaults: %+v", cfg.Notifications)
	}

	file := filepath.Join(t.TempDir(), "smtp-password")
	if err := os.WriteFile(file, []byte("from-a-file\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	cfg, err = Load(env(map[string]string{
		"IGLOO_ADMIN_PASSWORD":           "password",
		"IGLOO_NOTIFICATION_ALLOW_HOSTS": "Hooks.Internal, 10.0.0.5, ::1,hooks.internal",
		"IGLOO_SMTP_HOST":                "smtp.example.com",
		"IGLOO_SMTP_USERNAME":            "alerts",
		"IGLOO_SMTP_PASSWORD_FILE":       file,
		"IGLOO_SMTP_FROM":                "alerts@agency.example",
		"IGLOO_SMTP_ALLOWED_DOMAINS":     "Agency.example, @brand.example",
	}))
	if err != nil {
		t.Fatal(err)
	}
	n := cfg.Notifications
	if !slices.Equal(n.AllowHosts, []string{"hooks.internal", "10.0.0.5", "::1"}) {
		t.Errorf("allow hosts: %q", n.AllowHosts)
	}
	if s := n.SMTP; !s.Enabled() || s.Port != 587 || s.Password != "from-a-file" || s.From != "alerts@agency.example" ||
		!slices.Equal(s.AllowedDomains, []string{"agency.example", "brand.example"}) {
		t.Errorf("SMTP: %+v", s)
	}

	for _, bad := range []map[string]string{
		{"IGLOO_NOTIFICATION_ALLOW_HOSTS": "https://hooks.internal/"},
		{"IGLOO_NOTIFICATION_ALLOW_HOSTS": "hooks.internal:8080"},
		{"IGLOO_SMTP_FROM": "alerts@agency.example"},
		{"IGLOO_SMTP_HOST": "smtp.example.com", "IGLOO_SMTP_ALLOWED_DOMAINS": "agency.example"},
		{"IGLOO_SMTP_HOST": "smtp.example.com", "IGLOO_SMTP_FROM": "Alerts <a@agency.example>", "IGLOO_SMTP_ALLOWED_DOMAINS": "agency.example"},
		{"IGLOO_SMTP_HOST": "smtp.example.com", "IGLOO_SMTP_FROM": "a@agency.example"},
		{"IGLOO_SMTP_HOST": "smtp.example.com:25", "IGLOO_SMTP_FROM": "a@agency.example", "IGLOO_SMTP_ALLOWED_DOMAINS": "agency.example"},
		{"IGLOO_SMTP_HOST": "smtp.example.com", "IGLOO_SMTP_FROM": "a@agency.example", "IGLOO_SMTP_ALLOWED_DOMAINS": "agency.example",
			"IGLOO_SMTP_PORT": "smtp"},
		{"IGLOO_SMTP_HOST": "smtp.example.com", "IGLOO_SMTP_FROM": "a@agency.example", "IGLOO_SMTP_ALLOWED_DOMAINS": "agency.example",
			"IGLOO_SMTP_USERNAME": "alerts"},
		{"IGLOO_SMTP_HOST": "smtp.example.com", "IGLOO_SMTP_FROM": "a@agency.example", "IGLOO_SMTP_ALLOWED_DOMAINS": "agency.example",
			"IGLOO_SMTP_USERNAME": "alerts", "IGLOO_SMTP_PASSWORD": "x", "IGLOO_SMTP_PASSWORD_FILE": file},
	} {
		bad["IGLOO_ADMIN_PASSWORD"] = "password"
		if _, err := Load(env(bad)); err == nil {
			t.Errorf("%v: accepted", bad)
		} else if strings.Contains(err.Error(), "from-a-file") {
			t.Errorf("%v: the error shows the password: %v", bad, err)
		}
	}
}
