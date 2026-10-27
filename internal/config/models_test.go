package config

import "testing"

func TestModels(t *testing.T) {
	cfg, err := Load(env(map[string]string{"IGLOO_ADMIN_PASSWORD": "password"}))
	if err != nil || cfg.Models.Dir != "" || cfg.Models.Mirror != "" {
		t.Fatalf("defaults: %+v %v", cfg.Models, err)
	}
	cfg, err = Load(env(map[string]string{"IGLOO_ADMIN_PASSWORD": "password", "IGLOO_MODEL_DIR": "/var/lib/igloo/models",
		"IGLOO_MODEL_MIRROR": "https://models.internal/igloo"}))
	if err != nil || cfg.Models.Dir != "/var/lib/igloo/models" || cfg.Models.Mirror != "https://models.internal/igloo" {
		t.Errorf("set: %+v %v", cfg.Models, err)
	}
	for _, bad := range []map[string]string{{"IGLOO_MODEL_DIR": "models"}, {"IGLOO_MODEL_MIRROR": "ftp://models.internal"}} {
		bad["IGLOO_ADMIN_PASSWORD"] = "password"
		if _, err := Load(env(bad)); err == nil {
			t.Errorf("%v accepted", bad)
		}
	}
}
