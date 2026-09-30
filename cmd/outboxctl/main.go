package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/m1chlcz/pgoutbox"
)

const (
	usage = `usage: outboxctl --dsn DSN stats|list|retry|drop [options]

Commands:
  stats [--kind KIND]...                 count pending, leased, and dead jobs
  list [--state all|pending|leased|dead] list jobs, newest first
       [--kind KIND]... [--limit N]
  retry --id ID                          return a dead job to the queue
  drop --id ID                           delete one job`

	statsUsage = "usage: outboxctl stats --dsn DSN [--kind KIND] (repeatable)"
	retryUsage = "usage: outboxctl retry --dsn DSN --id ID"
	dropUsage  = "usage: outboxctl drop --dsn DSN --id ID"

	defaultListLimit = 50
)

func main() {
	os.Exit(runMain())
}

func runMain() int {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	if err := run(ctx, os.Args[1:], os.Stdout); err != nil {
		_, _ = fmt.Fprintln(os.Stderr, err)
		return 1
	}
	return 0
}

func run(ctx context.Context, args []string, out io.Writer) error {
	if len(args) == 0 {
		return errors.New(usage)
	}
	switch args[0] {
	case "stats":
		return statsCommand(ctx, args[1:], out)
	case "list":
		return listCommand(ctx, args[1:], out)
	case "retry":
		return retryCommand(ctx, args[1:], out)
	case "drop":
		return dropCommand(ctx, args[1:], out)
	default:
		return errors.New(usage)
	}
}

func statsCommand(ctx context.Context, args []string, out io.Writer) error {
	flags := flag.NewFlagSet("stats", flag.ContinueOnError)
	flags.SetOutput(io.Discard)
	dsn := flags.String("dsn", "", "PostgreSQL connection string")
	var kinds []string
	flags.Func("kind", "job kind (repeatable)", func(value string) error {
		kinds = append(kinds, value)
		return nil
	})
	if err := flags.Parse(args); err != nil || flags.NArg() != 0 {
		return errors.New(statsUsage)
	}
	queue, pool, err := openQueue(ctx, *dsn)
	if err != nil {
		return err
	}
	defer pool.Close()
	stats, err := queue.Stats(ctx, kinds)
	if err != nil {
		return err
	}
	_, _ = fmt.Fprintf(out, "pending=%d leased=%d dead=%d\n", stats.Pending, stats.Leased, stats.Dead)
	return nil
}

func listCommand(ctx context.Context, args []string, out io.Writer) error {
	flags := flag.NewFlagSet("list", flag.ContinueOnError)
	flags.SetOutput(io.Discard)
	dsn := flags.String("dsn", "", "PostgreSQL connection string")
	state := flags.String("state", "all", "all, pending, leased, or dead")
	limit := flags.Int("limit", defaultListLimit, "maximum rows")
	var kinds []string
	flags.Func("kind", "job kind (repeatable)", func(value string) error {
		kinds = append(kinds, value)
		return nil
	})
	if err := flags.Parse(args); err != nil || flags.NArg() != 0 {
		return errors.New("usage: outboxctl list --dsn DSN [--state STATE] [--kind KIND]... [--limit N]")
	}
	queue, pool, err := openQueue(ctx, *dsn)
	if err != nil {
		return err
	}
	defer pool.Close()
	views, err := queue.List(ctx, pgoutbox.State(*state), kinds, *limit)
	if err != nil {
		return err
	}
	for _, view := range views {
		lastError := ""
		if view.LastError != nil {
			lastError = *view.LastError
		}
		_, _ = fmt.Fprintf(out, "%d\t%s\t%s\t%d\t%s\t%s\t%s\n",
			view.ID,
			view.Kind,
			stateOf(view),
			view.Attempts,
			view.AvailableAt.UTC().Format(time.RFC3339),
			view.WorkerID,
			lastError,
		)
	}
	return nil
}

func retryCommand(ctx context.Context, args []string, out io.Writer) error {
	return runIDCommand(ctx, args, out, idCommandSpec{
		name:    "retry",
		usage:   retryUsage,
		verb:    "retried",
		missing: "is not dead",
		action:  (*pgoutbox.Queue).Retry,
	})
}

func dropCommand(ctx context.Context, args []string, out io.Writer) error {
	return runIDCommand(ctx, args, out, idCommandSpec{
		name:    "drop",
		usage:   dropUsage,
		verb:    "dropped",
		missing: "was not found",
		action:  (*pgoutbox.Queue).Drop,
	})
}

type idCommandSpec struct {
	name    string
	usage   string
	verb    string
	missing string
	action  func(*pgoutbox.Queue, context.Context, int64) (bool, error)
}

func runIDCommand(ctx context.Context, args []string, out io.Writer, spec idCommandSpec) error {
	flags := flag.NewFlagSet(spec.name, flag.ContinueOnError)
	flags.SetOutput(io.Discard)
	dsn := flags.String("dsn", "", "PostgreSQL connection string")
	id := flags.Int64("id", 0, "job ID")
	if err := flags.Parse(args); err != nil || flags.NArg() != 0 || *id <= 0 {
		return errors.New(spec.usage)
	}
	queue, pool, err := openQueue(ctx, *dsn)
	if err != nil {
		return err
	}
	defer pool.Close()
	changed, err := spec.action(queue, ctx, *id)
	if err != nil {
		return err
	}
	if !changed {
		return fmt.Errorf("job %d %s", *id, spec.missing)
	}
	_, _ = fmt.Fprintf(out, "%s %d\n", spec.verb, *id)
	return nil
}

func openQueue(ctx context.Context, dsn string) (*pgoutbox.Queue, *pgxpool.Pool, error) {
	if dsn == "" {
		dsn = os.Getenv("DATABASE_URL")
	}
	if dsn == "" {
		return nil, nil, errors.New("--dsn or DATABASE_URL is required")
	}
	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		return nil, nil, fmt.Errorf("open database: %w", err)
	}
	if err = pool.Ping(ctx); err != nil {
		pool.Close()
		return nil, nil, fmt.Errorf("connect to database: %w", err)
	}
	if err = pgoutbox.EnsureSchema(ctx, pool); err != nil {
		pool.Close()
		return nil, nil, err
	}
	queue, err := pgoutbox.New(pool)
	if err != nil {
		pool.Close()
		return nil, nil, err
	}
	return queue, pool, nil
}

func stateOf(view pgoutbox.JobView) string {
	switch {
	case view.DeadLetteredAt != nil:
		return "dead"
	case view.ProcessedAt != nil:
		return "processed"
	case view.LeaseUntil != nil && view.LeaseUntil.After(time.Now()):
		return "leased"
	default:
		return "pending"
	}
}
