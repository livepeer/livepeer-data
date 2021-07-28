package api

import (
	"context"
	"fmt"
	"net"
	"net/http"
	"time"

	"github.com/golang/glog"
	"github.com/livepeer/livepeer-data/health"
	"golang.org/x/sync/errgroup"
)

func ListenAndServe(ctx context.Context, host string, port uint, shutdownGracePeriod time.Duration, healthcore *health.Core) error {
	srv := &http.Server{
		Addr:    fmt.Sprintf("%s:%d", host, port),
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
		ln, err := net.Listen("tcp", srv.Addr)
		if err != nil {
			return fmt.Errorf("api server listen error: %w", err)
		}
		defer ln.Close()
		glog.Infof("Listening on %s", ln.Addr())

		if err := srv.Serve(ln); err != http.ErrServerClosed {
			return fmt.Errorf("api serve error: %w", err)
		}
		return nil
	})
	return eg.Wait()
}
