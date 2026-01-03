package main

import (
	"context"
	"fmt"
	"net/http"
	"time"

	"github.com/TicketsBot-cloud/common/observability"
	"github.com/TicketsBot-cloud/database"
	"github.com/TicketsBot-cloud/discord-entitlements-db-sync/internal/config"
	"github.com/TicketsBot-cloud/discord-entitlements-db-sync/internal/daemon"
	"github.com/TicketsBot-cloud/gdl/rest/request"
	"github.com/getsentry/sentry-go"
	"github.com/jackc/pgx/v4/pgxpool"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

func main() {
	config, err := config.LoadFromEnv()
	if err != nil {
		panic(err)
	}

	if len(config.Discord.ProxyHost) > 0 {
		request.RegisterPreRequestHook(func(_ string, req *http.Request) {
			req.URL.Scheme = "http"
			req.URL.Host = config.Discord.ProxyHost
		})
	}

	// Build logger
	if len(config.SentryDsn) > 0 {
		if err := sentry.Init(sentry.ClientOptions{
			Dsn: config.SentryDsn,
		}); err != nil {
			panic(fmt.Errorf("sentry.Init: %w", err))
		}
	}

	var logger *zap.Logger
	if config.JsonLogs {
		loggerConfig := zap.NewProductionConfig()
		loggerConfig.Level.SetLevel(config.LogLevel)

		logger, err = loggerConfig.Build(
			zap.AddCaller(),
			zap.AddStacktrace(zap.ErrorLevel),
			zap.WrapCore(observability.ZapSentryAdapter(observability.EnvironmentProduction)),
		)
	} else {
		loggerConfig := zap.NewDevelopmentConfig()
		loggerConfig.Level.SetLevel(config.LogLevel)
		loggerConfig.EncoderConfig.EncodeLevel = zapcore.CapitalColorLevelEncoder

		logger, err = loggerConfig.Build(zap.AddCaller(), zap.AddStacktrace(zap.ErrorLevel))
	}

	if err != nil {
		panic(fmt.Errorf("failed to initialise zap logger: %w", err))
	}

	logger.Info("Connecting to database...")
	db, err := connectDatabase(config)
	if err != nil {
		logger.Fatal("Failed to connect to database", zap.Error(err))
		return
	}

	logger.Info("Database connected.")

	d := daemon.NewDaemon(config, db, logger)
	if config.Daemon {
		if err := d.Start(); err != nil {
			panic(err)
		}
	} else {
		ctx, cancel := context.WithTimeout(context.Background(), config.ExecutionTimeout)
		defer cancel()

		if err := d.RunOnce(ctx); err != nil {
			panic(err)
		}
	}
}

func connectDatabase(config config.Config) (*database.Database, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	pool, err := pgxpool.Connect(ctx, config.DatabaseUri)
	if err != nil {
		return nil, err
	}

	return database.NewDatabase(pool), nil
}
