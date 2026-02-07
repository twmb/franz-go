package kfake

import "testing"

func TestConfigDefaults(t *testing.T) {
	t.Parallel()
	exceptions := map[string]struct{}{
		"broker.id":               {},
		"broker.rack":             {},
		"kfake.is_internal":       {},
		"sasl.enabled.mechanisms": {},
		"super.users":             {},
	}
	for k := range validTopicConfigs {
		if _, ok := configDefaults[k]; !ok {
			if _, ok := exceptions[k]; !ok {
				t.Errorf("configDefaults missing %q", k)
			}
		}
	}
}
