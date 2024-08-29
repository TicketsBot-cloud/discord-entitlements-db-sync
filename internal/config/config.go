package config

import (
	"github.com/caarlos0/env/v11"
	"go.uber.org/zap/zapcore"
	"time"
)

type Config struct {
	Daemon           bool          `env:"DAEMON" envDefault:"true"`
	RunFrequency     time.Duration `env:"RUN_FREQUENCY" envDefault:"1m"`
	ExecutionTimeout time.Duration `env:"EXECUTION_TIMEOUT" envDefault:"5m"`

	SentryDsn string        `env:"SENTRY_DSN"`
	JsonLogs  bool          `env:"JSON_LOGS" envDefault:"false"`
	LogLevel  zapcore.Level `env:"LOG_LEVEL" envDefault:"info"`

	Discord struct {
		ApplicationId uint64 `env:"APPLICATION_ID"`
		Token         string `env:"TOKEN"`
		ProxyHost     string `env:"PROXY_HOST"`
	} `envPrefix:"DISCORD_"`

	DatabaseUri string `env:"DATABASE_URI"`

	MaxRemovalsThreshold int `env:"MAX_REMOVALS_THRESHOLD" envDefault:"100"`
}

func LoadFromEnv() (Config, error) {
	var config Config
	err := env.Parse(&config)
	return config, err
}
