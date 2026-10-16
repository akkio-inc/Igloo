package config

import (
	"net"
	"net/mail"
	"os"
	"slices"
	"strconv"
	"strings"
)

// Notifications configures where alerts and other notifications may go
// (docs/alerts.md).
type Notifications struct {
	// AllowHosts are host names and addresses notifications may reach
	// although they are not public, and without TLS, such as a webhook
	// receiver or mail relay on the private network.
	AllowHosts []string
	SMTP       SMTP
}

// SMTP is the mail server email integrations send through. An empty Host
// means there is none, and email integrations cannot be created.
type SMTP struct {
	Host     string
	Port     int
	Username string
	Password string
	// From is the sender of every email.
	From string
	// AllowedDomains are the domains email integrations may send to.
	AllowedDomains []string
}

// Enabled reports whether a mail server is configured.
func (s SMTP) Enabled() bool { return s.Host != "" }

// loadNotifications reads IGLOO_NOTIFICATION_ALLOW_HOSTS and IGLOO_SMTP_*.
func loadNotifications(cfg *Config, lookup func(string) (string, bool), addErr func(string, ...any)) {
	get := func(key string) string {
		v, _ := lookup(key)
		return strings.TrimSpace(v)
	}
	list := func(key string) []string {
		var out []string
		for _, v := range strings.Split(get(key), ",") {
			if v = strings.ToLower(strings.TrimSpace(v)); v != "" && !slices.Contains(out, v) {
				out = append(out, v)
			}
		}
		return out
	}

	n := Notifications{AllowHosts: list("IGLOO_NOTIFICATION_ALLOW_HOSTS")}
	for _, h := range n.AllowHosts {
		if strings.ContainsAny(h, "/:@ ") && net.ParseIP(h) == nil {
			addErr("IGLOO_NOTIFICATION_ALLOW_HOSTS entry %q must be a host name or an IP address, without a scheme or port", h)
		}
	}

	s := SMTP{
		Host:     get("IGLOO_SMTP_HOST"),
		Port:     587,
		Username: get("IGLOO_SMTP_USERNAME"),
		From:     get("IGLOO_SMTP_FROM"),
	}
	for _, d := range list("IGLOO_SMTP_ALLOWED_DOMAINS") {
		s.AllowedDomains = append(s.AllowedDomains, strings.TrimPrefix(d, "@"))
	}
	if v := get("IGLOO_SMTP_PORT"); v != "" {
		port, err := strconv.Atoi(v)
		if err != nil || port < 1 || port > 65535 {
			addErr("IGLOO_SMTP_PORT must be a port number, got %q", v)
		}
		s.Port = port
	}
	// The password comes from a file, as Kubernetes secrets are mounted,
	// or from the environment.
	password, _ := lookup("IGLOO_SMTP_PASSWORD")
	switch file := get("IGLOO_SMTP_PASSWORD_FILE"); {
	case file != "" && password != "":
		addErr("set IGLOO_SMTP_PASSWORD or IGLOO_SMTP_PASSWORD_FILE, not both")
	case file != "":
		b, err := os.ReadFile(file)
		if err != nil {
			addErr("IGLOO_SMTP_PASSWORD_FILE: %w", err)
		}
		s.Password = strings.TrimSpace(string(b))
	default:
		s.Password = strings.TrimSpace(password)
	}

	if s.Host == "" {
		for _, key := range []string{"IGLOO_SMTP_PORT", "IGLOO_SMTP_USERNAME", "IGLOO_SMTP_PASSWORD", "IGLOO_SMTP_PASSWORD_FILE",
			"IGLOO_SMTP_FROM", "IGLOO_SMTP_ALLOWED_DOMAINS"} {
			if get(key) != "" {
				addErr("%s is set but IGLOO_SMTP_HOST is not", key)
			}
		}
		s = SMTP{}
	} else {
		if strings.ContainsAny(s.Host, "/:@ ") && net.ParseIP(s.Host) == nil {
			addErr("IGLOO_SMTP_HOST must be a host name or an IP address, without a scheme or port, got %q", s.Host)
		}
		if a, err := mail.ParseAddress(s.From); err != nil || a.Name != "" || a.Address != s.From {
			addErr("IGLOO_SMTP_FROM must be an email address such as alerts@example.com, got %q", s.From)
		}
		// Fail closed: without a list, alerts could mail data anywhere.
		if len(s.AllowedDomains) == 0 {
			addErr("IGLOO_SMTP_ALLOWED_DOMAINS must name the email domains alerts may send to, such as example.com")
		}
		if (s.Username == "") != (s.Password == "") {
			addErr("set both IGLOO_SMTP_USERNAME and a password, or neither")
		}
	}
	n.SMTP = s
	cfg.Notifications = n
}
