package api

import (
	"context"
	"fmt"
	"net/http"
	"time"

	"github.com/livepeer/healthy-streams/health"
	"golang.org/x/sync/errgroup"
)

func ListenAndServe(ctx context.Context, addr string, shutdownGracePeriod time.Duration, healthcore *health.Core) error {
	srv := &http.Server{
		Addr:    addr,
		Handler: NewHandler(healthcore),
	}
	eg, ctx := errgroup.WithContext(ctx)
	eg.Go(func() error {
		<-ctx.Done()
		shutCtx, cancel := context.WithTimeout(context.Background(), shutdownGracePeriod)
		defer cancel()
		if err := srv.Shutdown(shutCtx); err != nil {
			if closeErr := srv.Close(); closeErr != nil {
				err = fmt.Errorf("shutdownErr=%w closeErr=%q", err, closeErr)
			}
			return fmt.Errorf("api server shutdown error: %w", err)
		}
		return nil
	})
	eg.Go(func() error {
		if err := srv.ListenAndServe(); err != http.ErrServerClosed {
			return fmt.Errorf("api server listen and serve error: %w", err)
		}
		return nil
	})
	return eg.Wait()
}
