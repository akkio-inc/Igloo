package audit

import (
	"context"
	"os"
	"slices"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
)

// Usage for chargeback: igloo.usage sums the query history by day, compute
// pool, user, role and query tag, and estimates what each group cost. A
// pool's cost for a day is the time its replicas ran that day (each records
// its own, see Compute) times their machines' hourly price; each group gets
// the share of it that its statements' running time is of all statements
// that ran in the pool that day. Time a statement waited for its role's
// limits does not count, and neither, separately, does idle time: the
// statements that ran carry the whole day's cost.

// computeInterval is how often a replica records how long it has run.
const computeInterval = time.Minute

// Compute is how long one replica process of a compute pool, started at
// Started, has run on one day (UTC). A process writes the same day again
// as it runs, with a larger total; writing a total again changes nothing,
// so a write retried after a failure that in fact committed counts no time
// twice.
type Compute struct {
	Day     time.Time
	Pool    string
	Replica string
	Started time.Time
	Seconds float64
	// PricePerHour is what the replica's machine costs an hour, or zero
	// if unknown.
	PricePerHour float64
}

// Usage sums the statements of one day, compute pool, user, role and
// query tag.
type Usage struct {
	Day                        time.Time
	Pool, User, Role, QueryTag string
	Statements, Failed         int64
	// Run is how long the statements ran, not counting Queued, the time
	// they waited for their role's limits.
	Run, Queued time.Duration
	// CPUTime and BytesRead add up what DuckDB measured.
	CPUTime   time.Duration
	BytesRead int64
	// Share is Run as a fraction of the running time of every statement
	// in the pool that day.
	Share float64
	// ComputeHours is how long the pool's replicas ran that day, or -1 if
	// nothing recorded it.
	ComputeHours float64
	// Cost is the pool's cost that day times Share, or nil when a
	// replica's price is unknown.
	Cost *float64
}

// computeClock tracks how long this replica process has run, for the days
// it has not yet recorded in full. Only Log.Run uses it.
type computeClock struct {
	pool, replica string
	price         float64
	started       time.Time
	// written is when the last successful write counted up to.
	written time.Time
}

func newComputeClock(pool string, price float64) computeClock {
	if pool == "" {
		pool = "default"
	}
	replica, _ := os.Hostname()
	if replica == "" {
		replica = "unknown"
	}
	now := time.Now()
	return computeClock{pool: pool, replica: replica, price: price, started: now, written: now}
}

// totals returns how long the process has run on each day from the one
// last written up to now's.
func (c *computeClock) totals(now time.Time) []Compute {
	var out []Compute
	for day := c.written.UTC().Truncate(24 * time.Hour); day.Before(now); day = day.Add(24 * time.Hour) {
		from, to := day, day.Add(24*time.Hour)
		if c.started.After(from) {
			from = c.started
		}
		if now.Before(to) {
			to = now
		}
		out = append(out, Compute{Day: day, Pool: c.pool, Replica: c.replica, Started: c.started,
			Seconds: to.Sub(from).Seconds(), PricePerHour: c.price})
	}
	return out
}

// writeCompute records how long this replica has run until now. Days that
// cannot be written are written again, with larger totals, the next time.
func (l *Log) writeCompute(now time.Time) {
	cs := l.compute.totals(now)
	if len(cs) == 0 {
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), writeTimeout)
	defer cancel()
	if err := l.store.WriteCompute(ctx, cs); err != nil {
		l.log.Warn("could not record this replica's running time", "err", err)
		return
	}
	l.compute.written = now
}

// Usage returns the usage v may see; see Store.
func (l *Log) Usage(ctx context.Context, v Viewer, since, until time.Time) ([]Usage, error) {
	if l == nil {
		return nil, nil
	}
	return l.store.Usage(ctx, v, since, until)
}

// usageKey groups statements.
type usageKey struct {
	day                        time.Time
	pool, user, role, queryTag string
}

type poolDay struct {
	day  time.Time
	pool string
}

func (m *MemoryStore) WriteCompute(_ context.Context, cs []Compute) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	for _, c := range cs {
		i := slices.IndexFunc(m.compute, func(o Compute) bool {
			return o.Day.Equal(c.Day) && o.Pool == c.Pool && o.Replica == c.Replica && o.Started.Equal(c.Started)
		})
		if i < 0 {
			m.compute = append(m.compute, c)
			continue
		}
		m.compute[i].Seconds = max(m.compute[i].Seconds, c.Seconds)
		m.compute[i].PricePerHour = c.PricePerHour
	}
	return nil
}

func (m *MemoryStore) Usage(_ context.Context, v Viewer, since, until time.Time) ([]Usage, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	groups := map[usageKey]*Usage{}
	totals := map[poolDay]time.Duration{}
	for _, q := range m.queries {
		if !since.IsZero() && q.Start.Before(since) || !until.IsZero() && !q.Start.Before(until) {
			continue
		}
		pool := q.Pool
		if pool == "" {
			pool = "default"
		}
		k := usageKey{day: q.Start.UTC().Truncate(24 * time.Hour), pool: pool, user: q.User, role: q.Role, queryTag: q.QueryTag}
		u := groups[k]
		if u == nil {
			u = &Usage{Day: k.day, Pool: k.pool, User: k.user, Role: k.role, QueryTag: k.queryTag}
			groups[k] = u
		}
		run := max(q.Duration-q.Queued, 0)
		u.Statements++
		if q.Outcome != OutcomeOK {
			u.Failed++
		}
		u.Run += run
		u.Queued += q.Queued
		if r := q.Resources; r != nil {
			u.CPUTime += r.CPUTime
			u.BytesRead += r.BytesRead
		}
		totals[poolDay{k.day, k.pool}] += run
	}
	var out []Usage
	for _, u := range groups {
		pd := poolDay{u.Day, u.Pool}
		if !visible(v, u.User) {
			continue
		}
		if t := totals[pd]; t > 0 {
			u.Share = float64(u.Run) / float64(t)
		}
		u.ComputeHours = -1
		var cost float64
		priced := false
		for _, c := range m.compute {
			if c.Day.Equal(pd.day) && c.Pool == pd.pool {
				if u.ComputeHours < 0 {
					u.ComputeHours, priced = 0, true
				}
				u.ComputeHours += c.Seconds / 3600
				cost += c.Seconds / 3600 * c.PricePerHour
				priced = priced && c.PricePerHour > 0
			}
		}
		if priced {
			share := cost * u.Share
			u.Cost = &share
		}
		out = append(out, *u)
	}
	sortUsage(out)
	return out, nil
}

// sortUsage orders usage newest day first, then by pool, user, role and
// tag.
func sortUsage(us []Usage) {
	slices.SortFunc(us, func(a, b Usage) int {
		if c := b.Day.Compare(a.Day); c != 0 {
			return c
		}
		for _, p := range [][2]string{{a.Pool, b.Pool}, {a.User, b.User}, {a.Role, b.Role}, {a.QueryTag, b.QueryTag}} {
			if c := strings.Compare(p[0], p[1]); c != 0 {
				return c
			}
		}
		return 0
	})
}

func (s *PostgresStore) WriteCompute(ctx context.Context, cs []Compute) error {
	batch := &pgx.Batch{}
	for _, c := range cs {
		batch.Queue(`INSERT INTO igloo_audit.compute_time (day, pool, replica, started, seconds, price_per_hour)
			VALUES ($1, $2, $3, $4, $5, $6)
			ON CONFLICT (day, pool, replica, started) DO UPDATE
			SET seconds = greatest(compute_time.seconds, EXCLUDED.seconds), price_per_hour = EXCLUDED.price_per_hour`,
			c.Day, c.Pool, c.Replica, c.Started, c.Seconds, c.PricePerHour)
	}
	return s.pool.SendBatch(ctx, batch).Close()
}

// usageQuery sums the history in the metadata database, so reading
// igloo.usage moves one row per group rather than every statement.
const usageQuery = `WITH q AS (
		SELECT (start_time AT TIME ZONE 'UTC')::date AS day, coalesce(pool, 'default') AS pool, user_name, role,
			coalesce(query_tag, '') AS query_tag, count(*) AS statements,
			count(*) FILTER (WHERE outcome <> 'ok') AS failed,
			sum(greatest(duration_us - coalesce(queued_us, 0), 0))::bigint AS run_us,
			sum(coalesce(queued_us, 0))::bigint AS queued_us, sum(coalesce(cpu_time_us, 0))::bigint AS cpu_time_us,
			sum(coalesce(bytes_read, 0))::bigint AS bytes_read
		FROM igloo_audit.query_history
		WHERE ($2::timestamptz IS NULL OR start_time >= $2) AND ($3::timestamptz IS NULL OR start_time < $3)
		GROUP BY 1, 2, 3, 4, 5
	), totals AS (
		SELECT day, pool, sum(run_us) AS run_us FROM q GROUP BY 1, 2
	), compute AS (
		SELECT day, pool, sum(seconds) / 3600 AS hours,
			CASE WHEN bool_and(price_per_hour > 0) THEN sum(seconds / 3600 * price_per_hour) END AS cost
		FROM igloo_audit.compute_time GROUP BY 1, 2
	)
	SELECT q.day, q.pool, q.user_name, q.role, q.query_tag, q.statements, q.failed, q.run_us, q.queued_us,
		q.cpu_time_us, q.bytes_read,
		coalesce(q.run_us::float8 / nullif(t.run_us, 0), 0), coalesce(c.hours, -1),
		c.cost * coalesce(q.run_us::float8 / nullif(t.run_us, 0), 0)
	FROM q JOIN totals t USING (day, pool) LEFT JOIN compute c USING (day, pool)
	WHERE $1::text IS NULL OR q.user_name = $1
	ORDER BY q.day DESC, q.pool, q.user_name, q.role, q.query_tag`

func (s *PostgresStore) Usage(ctx context.Context, v Viewer, since, until time.Time) ([]Usage, error) {
	var user any
	if !v.All {
		user = v.User
	}
	rows, err := s.pool.Query(ctx, usageQuery, user, nullTime(since), nullTime(until))
	if err != nil {
		return nil, err
	}
	return pgx.CollectRows(rows, func(row pgx.CollectableRow) (Usage, error) {
		var (
			u                      Usage
			runUS, queuedUS, cpuUS int64
		)
		err := row.Scan(&u.Day, &u.Pool, &u.User, &u.Role, &u.QueryTag, &u.Statements, &u.Failed, &runUS, &queuedUS,
			&cpuUS, &u.BytesRead, &u.Share, &u.ComputeHours, &u.Cost)
		u.Run, u.Queued, u.CPUTime = micros(runUS), micros(queuedUS), micros(cpuUS)
		return u, err
	})
}

func micros(us int64) time.Duration { return time.Duration(us) * time.Microsecond }
