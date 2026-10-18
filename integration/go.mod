module tsession/integration

go 1.26.0

require (
	github.com/jackc/pgx/v5 v5.7.6
	github.com/redis/go-redis/v9 v9.23.0
	tsession v0.0.0
	tsession/collector v0.0.0
	tsession/contrib/tqueue v0.0.0
	tsession/contrib/tredis v0.0.0
	tsession/contrib/tsql v0.0.0
)

replace (
	tsession => ../
	tsession/collector => ../collector
	tsession/contrib/tqueue => ../contrib/tqueue
	tsession/contrib/tredis => ../contrib/tredis
	tsession/contrib/tsql => ../contrib/tsql
)
