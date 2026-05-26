package app

import (
	"context"
	"fmt"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"top-queries/internal/api"
	"top-queries/internal/broker"
	"top-queries/internal/broker/consumer"
	"top-queries/internal/collector"
	"top-queries/internal/config"
	"top-queries/internal/filters"
	"top-queries/internal/handler"

	"github.com/go-chi/chi/v5"
	chimiddleware "github.com/go-chi/chi/v5/middleware"
	"github.com/go-chi/cors"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/sarulabs/di/v2"
	"github.com/segmentio/kafka-go"
	"go.uber.org/zap"
	"golang.org/x/sync/errgroup"
)

// KafkaConsumer defines the operational interface required to manage background stream processing lifecycles.
type KafkaConsumer interface {
	Start(ctx context.Context) error
	Close() error
}

// App encapsulates the application context, configuration parameters, and the dependency injection graph container.
type App struct {
	cfg       *config.Config
	logger    *zap.Logger
	container di.Container
}

// New constructs the dependency injection container, resolves component relationships, and initializes the App wrapper.
func New(cfg *config.Config, logger *zap.Logger) (*App, error) {
	builder, err := di.NewBuilder()
	if err != nil {
		return nil, fmt.Errorf("failed to create di builder: %w", err)
	}

	builder.Add(di.Def{
		Name: "kafka-consumer",
		Build: func(ctn di.Container) (interface{}, error) {
			broker.WaitKafkaConsumersGroupReadiness(
				cfg.Kafka.Brokers[0],
				cfg.Kafka.Topic,
			)
			return consumer.NewSearchLogConsumer(
				kafka.NewReader(kafka.ReaderConfig{
					Brokers:          cfg.Kafka.Brokers,
					Topic:            cfg.Kafka.Topic,
					GroupID:          cfg.Kafka.Group,
					CommitInterval:   time.Second,
					ReadBatchTimeout: 10 * time.Second,
					MaxBytes:         10e6,
				}),
				ctn.Get("top-queries-list").(consumer.Collector),
				ctn.Get("filters").(filters.Filter),
				logger,
			), nil
		},
		Close: func(obj interface{}) error {
			return obj.(KafkaConsumer).Close()
		},
	})

	builder.Add(di.Def{
		Name: "stop-list-filter",
		Build: func(ctn di.Container) (interface{}, error) {
			return filters.NewStopListFilter(cfg.StopList.Words)
		},
	})

	builder.Add(di.Def{
		Name: "anti-fraud-filter",
		Build: func(ctn di.Container) (interface{}, error) {
			return filters.NewAntiFraudFilter(
				cfg.AntiFraud.CacheSize,
				cfg.AntiFraud.Limit,
				cfg.AntiFraud.TTL,
			)
		},
	})

	builder.Add(di.Def{
		Name: "filters",
		Build: func(ctn di.Container) (interface{}, error) {
			return filters.NewChain(
				filters.NewMetricsDecorator(ctn.Get("anti-fraud-filter").(filters.Filter), "antifraud"),
				filters.NewMetricsDecorator(ctn.Get("stop-list-filter").(filters.Filter), "stoplist"),
			), nil
		},
	})

	builder.Add(di.Def{
		Name: "top-queries-list",
		Build: func(ctn di.Container) (interface{}, error) {
			collCfg := collector.Config{
				BucketDuration: cfg.Collector.BucketDuration,
				WindowDuration: cfg.Collector.WindowDuration,
			}

			if cfg.Experimental {
				logger.Info("DI: initializing high-load TimeIndexedCollector (Raw JSON)")
				coll := collector.NewTimeIndexedCollector(collCfg)
				coll.StartAggregatorWorker(cfg.Collector.TickerDuration, cfg.Collector.TopLimit)
				return coll, nil
			}

			logger.Info("DI: initializing StructIndexedCollector for benchmarks/development")
			coll := collector.NewStructIndexedCollector(collCfg)
			coll.StartAggregatorWorker(cfg.Collector.TickerDuration, cfg.Collector.TopLimit)
			return coll, nil
		},
	})

	builder.Add(di.Def{
		Name: "http-handler",
		Build: func(ctn di.Container) (interface{}, error) {
			swList := ctn.Get("stop-list-filter").(handler.StopWordList)
			base := handler.NewBaseHandler(swList)

			if cfg.Experimental {
				topList := ctn.Get("top-queries-list").(handler.RawTopQueriesList)
				return handler.NewRawHandler(*base, topList), nil
			}

			topList := ctn.Get("top-queries-list").(handler.StructedTopQueriesList)
			return handler.NewStructHandler(*base, topList), nil
		},
	})

	builder.Add(di.Def{
		Name: "http-server",
		Build: func(ctn di.Container) (interface{}, error) {
			h := ctn.Get("http-handler").(api.StrictServerInterface)

			r := chi.NewRouter()
			r.Use(chimiddleware.Logger)
			r.Use(chimiddleware.RequestID)
			r.Use(chimiddleware.Recoverer)

			r.Use(cors.Handler(cors.Options{
				AllowedOrigins:   []string{"https://*", "http://*"},
				AllowedMethods:   []string{"GET", "POST", "PUT", "DELETE", "OPTIONS"},
				AllowedHeaders:   []string{"Accept", "Authorization", "Content-Type", "X-CSRF-Token"},
				ExposedHeaders:   []string{"Link"},
				AllowCredentials: true,
				MaxAge:           300,
			}))

			r.Use(handler.LoggerMiddleware(logger))

			r.Handle("/metrics", promhttp.Handler())

			strictHandler := api.NewStrictHandler(h, nil)
			api.HandlerWithOptions(strictHandler, api.ChiServerOptions{
				BaseURL:    "",
				BaseRouter: r,
			})

			return &http.Server{
				Addr:         fmt.Sprintf(":%d", cfg.App.Port),
				Handler:      r,
				ReadTimeout:  5 * time.Second,
				WriteTimeout: 10 * time.Second,
			}, nil
		},
	})

	return &App{
		cfg:       cfg,
		logger:    logger,
		container: builder.Build(),
	}, nil
}

// Run executes the application blocking routines via a localized errgroup and hooks OS signals to trigger graceful termination.
func (a *App) Run() error {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	srv := a.container.Get("http-server").(*http.Server)
	kafkaConsumer := a.container.Get("kafka-consumer").(KafkaConsumer)

	g, ctx := errgroup.WithContext(ctx)

	g.Go(func() error {
		a.logger.Info("starting kafka consumer...")
		if err := kafkaConsumer.Start(ctx); err != nil {
			return fmt.Errorf("kafka consumer error: %w", err)
		}
		return nil
	})

	g.Go(func() error {
		a.logger.Info("starting HTTP server...", zap.Int("port", a.cfg.App.Port))
		if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			return fmt.Errorf("http server error: %w", err)
		}
		return nil
	})

	<-ctx.Done()
	a.logger.Info("shutting down application gracefully...")

	return a.shutdown(srv)
}

func (a *App) shutdown(srv *http.Server) error {
	shutdownCtx, cancel := context.WithTimeout(context.Background(), a.cfg.App.ShutdownTimeout)
	defer cancel()

	var shutdownErr error

	a.logger.Info("stopping HTTP server...")
	if err := srv.Shutdown(shutdownCtx); err != nil {
		shutdownErr = fmt.Errorf("failed to shutdown HTTP server: %w", err)
	}

	a.logger.Info("stopping kafka consumer and clearing container...")
	a.container.Delete()

	a.logger.Info("application gracefully stopped")
	return shutdownErr
}
