package pgoutbox

const claimBaseSQL = `
	WITH candidate AS (
		SELECT job.id
		FROM outbox_jobs AS job
		WHERE job.kind = ANY($2::text[])
		  AND job.processed_at IS NULL
		  AND job.dead_lettered_at IS NULL
		  AND job.available_at <= $1
		  AND (job.scheduled_at IS NULL OR job.scheduled_at <= $1)
		  AND (job.lease_until IS NULL OR job.lease_until <= $1)
		ORDER BY job.available_at, job.id
		FOR UPDATE SKIP LOCKED
		LIMIT 1
	)
	UPDATE outbox_jobs AS job
	SET attempts = job.attempts + 1,
	    leased_at = $1,
	    lease_until = $3,
	    leased_by = $4
	FROM candidate
	WHERE job.id = candidate.id
	RETURNING job.id, job.kind, job.aggregate_type, job.aggregate_id,
	          job.generation, job.payload, job.attempts
`

const claimOrderedSQL = `
	WITH candidate AS (
		SELECT job.id
		FROM outbox_jobs AS job
		WHERE job.kind = ANY($2::text[])
		  AND job.processed_at IS NULL
		  AND job.dead_lettered_at IS NULL
		  AND job.available_at <= $1
		  AND (job.scheduled_at IS NULL OR job.scheduled_at <= $1)
		  AND (job.lease_until IS NULL OR job.lease_until <= $1)
		  AND NOT EXISTS (
			SELECT 1
			FROM outbox_jobs AS predecessor
			WHERE predecessor.aggregate_type = job.aggregate_type
			  AND predecessor.aggregate_id = job.aggregate_id
			  AND predecessor.kind = job.kind
			  AND predecessor.id < job.id
			  AND predecessor.processed_at IS NULL
			  AND predecessor.dead_lettered_at IS NULL
		  )
		ORDER BY job.available_at, job.id
		FOR UPDATE SKIP LOCKED
		LIMIT 1
	)
	UPDATE outbox_jobs AS job
	SET attempts = job.attempts + 1,
	    leased_at = $1,
	    lease_until = $3,
	    leased_by = $4
	FROM candidate
	WHERE job.id = candidate.id
	RETURNING job.id, job.kind, job.aggregate_type, job.aggregate_id,
	          job.generation, job.payload, job.attempts
`

func claimSQL(ordered bool) string {
	if ordered {
		return claimOrderedSQL
	}
	return claimBaseSQL
}
