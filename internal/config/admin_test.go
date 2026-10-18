package config

import (
	"strings"
	"testing"
)

func TestCheckAdminPassword(t *testing.T) {
	for _, tc := range []struct {
		user, password string
		ok             bool
	}{
		// Placeholders, whatever their length.
		{"igloo", "change-me", false},
		{"igloo", "CHANGE-ME", false},
		{"igloo", "changeme", false},
		{"igloo", "Please-Change-Me-0123456789", false},
		{"igloo", "change_me_change_me_change_me", false},
		{"igloo", "x Change Me x 0123456789abcdef", false},
		// Common words and the user's name.
		{"igloo", "password", false},
		{"igloo", "Admin", false},
		{"igloo", "igloo", false},
		{"igloo", " secret ", false},
		{"igloo", "default", false},
		{"warehouse_administrator", "warehouse_administrator", false},
		{"warehouse_administrator", "WAREHOUSE_ADMINISTRATOR", false},
		// One character repeated.
		{"igloo", "aaaaaaaaaaaaaaaaaaaaaaaa", false},
		{"igloo", "                        ", false},
		{"igloo", "éééééééééééééééééééé", false},
		// Length, counted in characters rather than bytes.
		{"igloo", "", false},
		{"igloo", "Kq7vT2mXz9LpR4w", false}, // 15
		{"igloo", "Kq7vT2mXz9LpR4wN", true}, // 16
		{"igloo", "Kq7vT2mXz9LpR4ü", false}, // 15 characters, 16 bytes
		{"igloo", "Kq7vT2mXz9LpR4wü", true}, // 16 characters
		{"igloo", "correct horse battery staple", true},
		{"igloo", "igloo-warehouse-2026-key", true}, // contains the user's name, but is not it
		// What init-env.sh, the Helm chart and the OpenTofu modules generate.
		{"igloo", "q8Zr1JtXo5LcW2nVb7KsE4mYh0PdG6Ua", true},
	} {
		err := CheckAdminPassword(tc.user, tc.password)
		if (err == nil) != tc.ok {
			t.Errorf("CheckAdminPassword(%q, %q) = %v, want ok=%v", tc.user, tc.password, err, tc.ok)
		}
	}
}

func TestCheckAdminPasswordSaysWhatToDo(t *testing.T) {
	short := CheckAdminPassword("igloo", "Kq7vT2mXz9LpR4w")
	if short == nil || !strings.Contains(short.Error(), "fewer than 16 characters") {
		t.Errorf("15 characters: %v", short)
	}
	placeholder := CheckAdminPassword("igloo", "change-me")
	if placeholder == nil || !strings.Contains(placeholder.Error(), "placeholder") {
		t.Errorf("change-me: %v", placeholder)
	}
	for _, err := range []error{short, placeholder} {
		msg := err.Error()
		for _, want := range []string{"IGLOO_ADMIN_PASSWORD", "openssl rand -base64 24", "init-env.sh", "make up"} {
			if !strings.Contains(msg, want) {
				t.Errorf("%q does not mention %s", msg, want)
			}
		}
	}
}

func TestCheckAdminPasswordDoesNotRepeatIt(t *testing.T) {
	for _, password := range []string{"change-me", "Xy-change-me-Qz-0123456789", "Tr0ub4dor&3", "Kq7vT2mXz9LpR4w", "qqqqqqqqqqqqqqqqqqqq"} {
		err := CheckAdminPassword("igloo", password)
		if err == nil {
			t.Fatalf("%q passed", password)
		}
		if strings.Contains(err.Error(), password) {
			t.Errorf("the error repeats %q: %s", password, err)
		}
	}
}

// Load leaves the password's strength to the moment Igloo creates the
// account, so installs whose account exists keep starting.
func TestLoadAcceptsAWeakAdminPassword(t *testing.T) {
	cfg, err := Load(env(map[string]string{"IGLOO_ADMIN_PASSWORD": "change-me"}))
	if err != nil {
		t.Fatal(err)
	}
	if cfg.AdminPassword != "change-me" || CheckAdminPassword(cfg.AdminUser, cfg.AdminPassword) == nil {
		t.Errorf("AdminPassword = %q", cfg.AdminPassword)
	}
	if _, err := Load(env(map[string]string{"IGLOO_ADMIN_PASSWORD": ""})); err == nil || !strings.Contains(err.Error(), "IGLOO_ADMIN_PASSWORD must be set") {
		t.Errorf("empty password: %v", err)
	}
}
