package main

import (
	"context"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/redis/go-redis/v9"

	"relay/internal/function"
	"relay/internal/runner"
	"relay/internal/runtime"
	"relay/internal/stream"
)

func envOr(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}

func main() {
	logger := log.New(os.Stdout, "relay: ", log.LstdFlags)

	timeout := 30 * time.Second
	if v := os.Getenv("FUNCTION_TIMEOUT"); v != "" {
		parsed, err := time.ParseDuration(v)
		if err != nil {
			logger.Fatalf("FUNCTION_TIMEOUT: %v", err)
		}
		timeout = parsed
	}

	cfg := struct {
		redisAddr    string
		redisStream  string
		redisGroup   string
		redisCons    string
		functionsDir string
	}{
		redisAddr:    envOr("REDIS_ADDR", "localhost:6379"),
		redisStream:  envOr("REDIS_STREAM", "events"),
		redisGroup:   envOr("REDIS_GROUP", "relay"),
		redisCons:    envOr("REDIS_CONSUMER", "worker-1"),
		functionsDir: envOr("FUNCTIONS_DIR", "./functions"),
	}

	client := redis.NewClient(&redis.Options{Addr: cfg.redisAddr})
	defer client.Close()

	loader := function.NewLoader(cfg.functionsDir, logger)
	functions, err := loader.Load()
	if err != nil {
		logger.Fatalf("load functions: %v", err)
	}
	logger.Printf("loaded %d function(s) from %s", len(functions), cfg.functionsDir)

	// Prepare (build) each function's image. A function whose image cannot be
	// built is marked unavailable so the runner skips it; the rest continue.
	manager, err := runtime.NewManager(logger)
	if err != nil {
		logger.Fatalf("runtime: %v", err)
	}
	defer manager.Close()
	preparedCount := 0
	var prepared []*runner.PreparedFunction
	for _, fn := range functions {
		p, err := manager.Prepare(context.Background(), fn)
		if err != nil {
			logger.Printf("function %q: prepare: %v", fn.Name, err)
			prepared = append(prepared, runner.NewUnavailable(fn))
			continue
		}
		prepared = append(prepared, runner.NewPrepared(fn, p, manager))
		preparedCount++
	}
	logger.Printf("prepared %d function(s)", preparedCount)

	consumer := stream.NewConsumer(stream.ConsumerConfig{
		Client:   client,
		Stream:   cfg.redisStream,
		Group:    cfg.redisGroup,
		Consumer: cfg.redisCons,
		Log:      logger,
	})

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	if err := consumer.EnsureGroup(ctx); err != nil {
		logger.Fatalf("ensure consumer group: %v", err)
	}

	run := runner.New(prepared, timeout, logger)

	logger.Printf("consuming stream %q as group %q consumer %q",
		cfg.redisStream,
		cfg.redisGroup,
		cfg.redisCons,
	)
	if err := consumer.Consume(ctx, run.Handle); err != nil {
		logger.Fatalf("consume: %v", err)
	}
	logger.Printf("shutdown complete")
}
