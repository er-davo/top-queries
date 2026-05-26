package config

import (
	"fmt"
	"strings"
	"time"

	"github.com/go-playground/validator/v10"
	"github.com/spf13/viper"
)

// Config holds the consolidated application configuration parameters.
type Config struct {
	Experimental bool      `mapstructure:"experimental"`
	App          App       `mapstructure:"app" validate:"required"`
	Kafka        Kafka     `mapstructure:"kafka" validate:"required"`
	Collector    Collector `mapstructure:"collector" validate:"required"`
	AntiFraud    AntiFraud `mapstructure:"anti_fraud" validate:"required"`
	StopList     StopList  `mapstructure:"stop_list" validate:"required"`
}

// App encapsulates basic application and infrastructure settings.
type App struct {
	Port            int           `mapstructure:"port" validate:"required,gte=1,lte=65535"`
	LogLevel        string        `mapstructure:"log_level" validate:"required,oneof=debug info warn error"`
	IsProd          bool          `mapstructure:"is_prod"`
	ShutdownTimeout time.Duration `mapstructure:"shutdown_timeout" validate:"required,gt=0"`
}

// Kafka contains parameters required to connect to the Apache Kafka cluster.
type Kafka struct {
	Brokers []string `mapstructure:"brokers" validate:"required,gt=0"`
	Topic   string   `mapstructure:"topic" validate:"required"`
	Group   string   `mapstructure:"group" validate:"required"`
}

// Collector defines sliding window and buffering parameters for tracking query frequencies.
type Collector struct {
	BucketDuration time.Duration `mapstructure:"bucket_duration" validate:"required,gt=0"`
	WindowDuration time.Duration `mapstructure:"window_duration" validate:"required,gt=0"`
	TickerDuration time.Duration `mapstructure:"ticker_duration" validate:"required,gt=0"`
	TopLimit       int           `mapstructure:"top_limit" validate:"required,gt=0,lte=10000"`
}

// AntiFraud specifies the limits and TTL settings used by the rate-limiting filter.
type AntiFraud struct {
	CacheSize int           `mapstructure:"cache_size" validate:"required,gt=0"`
	Limit     int64         `mapstructure:"limit" validate:"required,gt=0"`
	TTL       time.Duration `mapstructure:"ttl" validate:"required,gt=0"`
}

// StopList wraps the initial set of blacklisted terms.
type StopList struct {
	Words []string `mapstructure:"words"`
}

// Load reads config from the provided file path, merges environment overrides, and validates the output structure.
func Load(configFilePath string) (*Config, error) {
	v := viper.New()

	setDefaults(v)

	v.AutomaticEnv()
	v.SetEnvKeyReplacer(strings.NewReplacer(".", "_"))

	if configFilePath != "" {
		v.SetConfigFile(configFilePath)
		if err := v.ReadInConfig(); err != nil {
			return nil, fmt.Errorf("failed to read config file: %w", err)
		}
	}

	var cfg Config

	if err := v.Unmarshal(&cfg); err != nil {
		return nil, fmt.Errorf("failed to unmarshal config: %w", err)
	}

	if err := cfg.Validate(); err != nil {
		return nil, fmt.Errorf("config validation error: %w", err)
	}

	return &cfg, nil
}

// Validate executes struct validation tags on the Config fields.
func (c *Config) Validate() error {
	return validator.New().Struct(c)
}

func setDefaults(v *viper.Viper) {
	v.SetDefault("app.port", 8080)
	v.SetDefault("app.log_level", "info")
	v.SetDefault("app.is_prod", false)
	v.SetDefault("app.shutdown_timeout", "10s")

	v.SetDefault("kafka.brokers", []string{"localhost:9092"})
	v.SetDefault("kafka.topic", "search-events")
	v.SetDefault("kafka.group", "top-queries-collector")

	v.SetDefault("collector.bucket_duration", "5s")
	v.SetDefault("collector.window_duration", "5m")
	v.SetDefault("collector.ticker_duration", "1s")
	v.SetDefault("collector.top_limit", 100)
}
