package config

import (
	"fmt"
	"slices"
	"strings"
	"unicode"
	"unicode/utf8"
)

// MinAdminPasswordLength is the fewest characters IGLOO_ADMIN_PASSWORD may
// have for Igloo to create the admin account with it. NIST SP 800-63B asks
// for at least 15 when a password is the only factor; the passwords
// init-env.sh, the Helm chart and the OpenTofu modules generate have 32.
const MinAdminPasswordLength = 16

// guessableAdminPasswords are refused whatever their length, compared
// without case or the spaces around them, as is the admin's own name.
var guessableAdminPasswords = []string{"password", "admin", "igloo", "secret", "default"}

// CheckAdminPassword returns an error if password is too weak for Igloo to
// create the admin account user with: shorter than MinAdminPasswordLength
// characters, a placeholder such as change-me, a common word or the user's
// name, or one character repeated. It is not a strength meter, only a
// guard against the obvious. The error never contains the password.
//
// Load does not call it: an account that exists already does not use the
// password, so only creating one needs it (see cmd/igloo's ensureAdmin).
func CheckAdminPassword(user, password string) error {
	var problem string
	switch word := strings.ToLower(strings.TrimSpace(password)); {
	case isPlaceholder(word), slices.Contains(guessableAdminPasswords, word), word == strings.ToLower(user),
		oneCharacter(password):
		problem = "is a placeholder or easily guessed"
	case utf8.RuneCountInString(password) < MinAdminPasswordLength:
		problem = fmt.Sprintf("has fewer than %d characters", MinAdminPasswordLength)
	default:
		return nil
	}
	return fmt.Errorf("IGLOO_ADMIN_PASSWORD %s, so Igloo will not create the admin account with it. "+
		"Set it to a long random value, e.g. from `openssl rand -base64 24`; "+
		"deploy/single-vm/init-env.sh (run by `make up`) generates one when it creates .env", problem)
}

// isPlaceholder reports whether a lowercase password contains "change me",
// however its words are joined: change-me, changeme, change_me.
func isPlaceholder(password string) bool {
	letters := strings.Map(func(r rune) rune {
		if unicode.IsLetter(r) || unicode.IsDigit(r) {
			return r
		}
		return -1
	}, password)
	return strings.Contains(letters, "changeme")
}

// oneCharacter reports whether s is a single character, repeated.
func oneCharacter(s string) bool {
	first, _ := utf8.DecodeRuneInString(s)
	return s != "" && strings.Trim(s, string(first)) == ""
}
