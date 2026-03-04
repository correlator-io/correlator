package kafka

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestConfig_Validate(t *testing.T) {
	tests := []struct {
		name    string
		config  Config
		wantErr error
	}{
		{
			name: "valid config with all fields",
			config: Config{
				Enabled: true,
				Brokers: []string{"localhost:9092"},
				Topic:   "openlineage.events",
				GroupID: "correlator",
			},
			wantErr: nil,
		},
		{
			name: "disabled config skips validation",
			config: Config{
				Enabled: false,
				Brokers: nil,
				Topic:   "",
				GroupID: "",
			},
			wantErr: nil,
		},
		{
			name: "enabled but no brokers",
			config: Config{
				Enabled: true,
				Brokers: nil,
				Topic:   "openlineage.events",
				GroupID: "correlator",
			},
			wantErr: ErrBrokersRequired,
		},
		{
			name: "enabled but empty brokers slice",
			config: Config{
				Enabled: true,
				Brokers: []string{},
				Topic:   "openlineage.events",
				GroupID: "correlator",
			},
			wantErr: ErrBrokersRequired,
		},
		{
			name: "enabled but no topic",
			config: Config{
				Enabled: true,
				Brokers: []string{"localhost:9092"},
				Topic:   "",
				GroupID: "correlator",
			},
			wantErr: ErrTopicRequired,
		},
		{
			name: "enabled but no group ID",
			config: Config{
				Enabled: true,
				Brokers: []string{"localhost:9092"},
				Topic:   "openlineage.events",
				GroupID: "",
			},
			wantErr: ErrGroupIDRequired,
		},
		{
			name: "multiple brokers",
			config: Config{
				Enabled: true,
				Brokers: []string{"broker1:9092", "broker2:9092", "broker3:9092"},
				Topic:   "openlineage.events",
				GroupID: "correlator",
			},
			wantErr: nil,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := tt.config.Validate()
			if tt.wantErr != nil {
				require.Error(t, err)
				assert.ErrorIs(t, err, tt.wantErr)
			} else {
				assert.NoError(t, err)
			}
		})
	}
}

func TestLoadConfig(t *testing.T) {
	tests := []struct {
		name     string
		envVars  map[string]string
		expected Config
	}{
		{
			name:    "defaults when no env vars set",
			envVars: map[string]string{},
			expected: Config{
				Enabled: false,
				Brokers: []string{},
				Topic:   defaultTopic,
				GroupID: defaultGroupID,
			},
		},
		{
			name: "enabled with single broker",
			envVars: map[string]string{
				"CORRELATOR_KAFKA_ENABLED": "true",
				"CORRELATOR_KAFKA_BROKERS": "localhost:9092",
				"CORRELATOR_KAFKA_TOPIC":   "my.custom.topic",
				"CORRELATOR_KAFKA_GROUP":   "my-group",
			},
			expected: Config{
				Enabled: true,
				Brokers: []string{"localhost:9092"},
				Topic:   "my.custom.topic",
				GroupID: "my-group",
			},
		},
		{
			name: "multiple brokers comma-separated",
			envVars: map[string]string{
				"CORRELATOR_KAFKA_ENABLED": "true",
				"CORRELATOR_KAFKA_BROKERS": "broker1:9092, broker2:9092, broker3:9092",
			},
			expected: Config{
				Enabled: true,
				Brokers: []string{"broker1:9092", "broker2:9092", "broker3:9092"},
				Topic:   defaultTopic,
				GroupID: defaultGroupID,
			},
		},
		{
			name: "disabled explicitly",
			envVars: map[string]string{
				"CORRELATOR_KAFKA_ENABLED": "false",
			},
			expected: Config{
				Enabled: false,
				Brokers: []string{},
				Topic:   defaultTopic,
				GroupID: defaultGroupID,
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Set env vars
			for k, v := range tt.envVars {
				t.Setenv(k, v)
			}

			config := LoadConfig()

			assert.Equal(t, tt.expected.Enabled, config.Enabled)
			assert.Equal(t, tt.expected.Brokers, config.Brokers)
			assert.Equal(t, tt.expected.Topic, config.Topic)
			assert.Equal(t, tt.expected.GroupID, config.GroupID)
		})
	}
}
