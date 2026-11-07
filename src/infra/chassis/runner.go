package chassis

import (
	"context"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"event-driven-platform/src/infra/observability"
	"event-driven-platform/src/infra/observability/metrics"
	"event-driven-platform/src/shared/middleware"
)

type Config struct {
	PublicAddr      string
	AdminAddr       string
	ShutdownTimeout time.Duration
}

func DefaultConfig() Config {
	publicPort := os.Getenv("PORT")
	if publicPort == "" {
		publicPort = "8080"
	}
	if publicPort[0] != ':' {
		publicPort = ":" + publicPort
	}

	adminPort := os.Getenv("ADMIN_PORT")
	if adminPort == "" {
		adminPort = "8081"
	}
	adminHost := os.Getenv("ADMIN_HOST")
	if adminHost == "" {
		adminHost = "127.0.0.1"
	}

	return Config{
		PublicAddr:      publicPort,
		AdminAddr:       fmt.Sprintf("%s:%s", adminHost, adminPort),
		ShutdownTimeout: 15 * time.Second,
	}
}

type App struct {
	Config   Config
	Services Services
	Health   *HealthCoordinator
	Spec     ServiceSpec
}

func Run(spec ServiceSpec, args []string, env []string) int {
	bootCtx, exitCode, err := Preflight(args, env, os.Stdout)
	if err != nil {
		log.Printf("Preflight error: %v\n", err)
		return exitCode
	}

	if bootCtx.ValidateOnly {
		fmt.Println("Configuration and desired state valid")
		return ExitSuccess
	}

	cfg := DefaultConfig()
	health := NewHealthCoordinator()
	deps := Dependencies{Context: context.Background()}

	otelEndpoint := os.Getenv("OTEL_EXPORTER_OTLP_ENDPOINT")
	if otelEndpoint == "" {
		otelEndpoint = "127.0.0.1:4317"
	}
	shutdownTracer, err := observability.InitGlobalTracer(context.Background(), spec.Identity.Name, otelEndpoint)
	if err != nil {
		log.Printf("Warning: failed to initialize OpenTelemetry SDK: %v\n", err)
	} else if shutdownTracer != nil {
		defer func() {
			shutCtx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
			defer cancel()
			_ = shutdownTracer(shutCtx)
		}()
	}

	services, err := spec.BuildServices(deps)
	if err != nil {
		log.Printf("Dependency composition failed: %v\n", err)
		return ExitDependency
	}

	app := &App{
		Config:   cfg,
		Services: services,
		Health:   health,
		Spec:     spec,
	}

	lifecycle := NewLifecycle()

	switch bootCtx.Command {
	case "serve":
		wireServe(app, lifecycle)
		if app.Spec.RegisterDaemons != nil {
			app.Spec.RegisterDaemons(lifecycle, app.Services)
		}
		if shutdownTracer != nil {
			lifecycle.AddComponent("telemetry-flusher", func(ctx context.Context) error {
				<-ctx.Done()
				shutCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
				defer cancel()
				log.Println("Flushing telemetry spans before process termination...")
				return shutdownTracer(shutCtx)
			}, func(_ context.Context) error {
				return nil
			})
		}
	case "migrate":
		if spec.Migrations != nil {
			mig := spec.Migrations()
			if mig.Apply != nil {
				if err := mig.Apply(context.Background()); err != nil {
					log.Printf("Migration failed: %v\n", err)
					return ExitDependency
				}
			}
		}
		return ExitSuccess
	default:
		log.Printf("Unsupported command mode: %s\n", bootCtx.Command)
		return ExitUsage
	}

	return runLongLived(app, lifecycle)
}

func wireServe(app *App, lifecycle *Lifecycle) {
	registry, httpMetrics := metrics.Default(os.Getenv("GIT_SHA"))

	adminMux := http.NewServeMux()
	adminMux.Handle("GET /metrics", registry.Handler())
	adminMux.HandleFunc("/health/live", app.Health.LiveHandler())
	adminMux.HandleFunc("/health/ready", app.Health.ReadyHandler())

	adminServer := &http.Server{
		Addr:         app.Config.AdminAddr,
		Handler:      adminMux,
		ReadTimeout:  5 * time.Second,
		WriteTimeout: 5 * time.Second,
	}

	lifecycle.AddComponent("admin-server", func(_ context.Context) error {
		if err := adminServer.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			return fmt.Errorf("admin listener failed: %w", err)
		}
		return nil
	}, func(ctx context.Context) error {
		return adminServer.Shutdown(ctx)
	})

	publicMux := http.NewServeMux()
	if app.Spec.RegisterAPIRoutes != nil {
		app.Spec.RegisterAPIRoutes(publicMux, app.Services)
	}

	// Metrics wraps the mux directly so it observes r.Pattern; recovery sits outside it.
	wrappedHandler := middleware.TraceAndContextMiddleware(
		middleware.ErrorRecoveryMiddleware(
			middleware.BodyLimitMiddleware(middleware.MaxRequestBodyBytesFromEnv())(
				httpMetrics.Middleware(publicMux))))

	publicServer := &http.Server{
		Addr:              app.Config.PublicAddr,
		Handler:           wrappedHandler,
		ReadHeaderTimeout: 10 * time.Second,
		ReadTimeout:       15 * time.Second,
		WriteTimeout:      15 * time.Second,
		IdleTimeout:       75 * time.Second,
	}

	lifecycle.AddComponent("public-server", func(_ context.Context) error {
		if err := publicServer.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			return fmt.Errorf("public listener failed: %w", err)
		}
		return nil
	}, func(ctx context.Context) error {
		return publicServer.Shutdown(ctx)
	})
}

func runLongLived(app *App, lifecycle *Lifecycle) int {
	rootCtx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	fatalChan := make(chan error, 2)
	lifecycle.StartAll(rootCtx, fatalChan)

	app.Health.SetReady(true)
	log.Printf("Service %s running (tier: %s, team: %s)\n", app.Spec.Identity.Name, app.Spec.Identity.Tier, app.Spec.Identity.Team)

	var exitCode = ExitSuccess
	select {
	case <-rootCtx.Done():
		log.Println("Termination signal received, beginning shutdown")
	case err := <-fatalChan:
		log.Printf("Fatal runtime error: %v, shutting down\n", err)
		exitCode = ExitRuntime
	}

	app.Health.SetReady(false)
	stop() // cancel rootCtx so daemons stop on the fatal-error path too

	if err := lifecycle.StopAll(app.Config.ShutdownTimeout); err != nil {
		log.Printf("Shutdown finished with errors: %v\n", err)
	} else {
		log.Println("Shutdown complete, exiting cleanly")
	}

	return exitCode
}
