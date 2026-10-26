package config

import (
	"slices"
	"testing"
	"time"
)

func TestAI(t *testing.T) {
	cfg, err := Load(env(map[string]string{"IGLOO_ADMIN_PASSWORD": "password"}))
	if err != nil {
		t.Fatal(err)
	}
	a := cfg.AI
	if !a.Cache || a.CacheRetention != 30*24*time.Hour || a.MaxCalls != 1000 || a.MaxCostUSD != 1 || a.RequestTimeout != 2*time.Minute ||
		len(a.AllowHosts) != 0 {
		t.Errorf("defaults: %+v", a)
	}
	cfg, err = Load(env(map[string]string{
		"IGLOO_ADMIN_PASSWORD":     "password",
		"IGLOO_AI_ALLOW_HOSTS":     "vllm.internal, 10.0.0.7,vllm.internal",
		"IGLOO_AI_CACHE":           "off",
		"IGLOO_AI_CACHE_RETENTION": "7d",
		"IGLOO_AI_MAX_CALLS":       "0",
		"IGLOO_AI_MAX_COST_USD":    "25.5",
		"IGLOO_AI_REQUEST_TIMEOUT": "30s",
	}))
	if err != nil {
		t.Fatal(err)
	}
	a = cfg.AI
	if a.Cache || a.CacheRetention != 7*24*time.Hour || a.MaxCalls != 0 || a.MaxCostUSD != 25.5 || a.RequestTimeout != 30*time.Second ||
		!slices.Equal(a.AllowHosts, []string{"vllm.internal", "10.0.0.7"}) {
		t.Errorf("set: %+v", a)
	}
	for _, bad := range []map[string]string{
		{"IGLOO_AI_ALLOW_HOSTS": "http://vllm.internal"},
		{"IGLOO_AI_CACHE": "maybe"},
		{"IGLOO_AI_CACHE_RETENTION": "0"},
		{"IGLOO_AI_MAX_CALLS": "-1"},
		{"IGLOO_AI_MAX_COST_USD": "lots"},
		{"IGLOO_AI_REQUEST_TIMEOUT": "never"},
	} {
		bad["IGLOO_ADMIN_PASSWORD"] = "password"
		if _, err := Load(env(bad)); err == nil {
			t.Errorf("%v: accepted", bad)
		}
	}
}
