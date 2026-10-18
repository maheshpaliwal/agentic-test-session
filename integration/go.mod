module github.com/maheshpaliwal/agentic-test-session/integration

go 1.26.0

require (
	github.com/jackc/pgx/v5 v5.7.6
	github.com/redis/go-redis/v9 v9.23.0
	github.com/maheshpaliwal/agentic-test-session v0.0.0
	github.com/maheshpaliwal/agentic-test-session/collector v0.0.0
	github.com/maheshpaliwal/agentic-test-session/contrib/tqueue v0.0.0
	github.com/maheshpaliwal/agentic-test-session/contrib/tredis v0.0.0
	github.com/maheshpaliwal/agentic-test-session/contrib/tsql v0.0.0
)

replace (
	github.com/maheshpaliwal/agentic-test-session => ../
	github.com/maheshpaliwal/agentic-test-session/collector => ../collector
	github.com/maheshpaliwal/agentic-test-session/contrib/tqueue => ../contrib/tqueue
	github.com/maheshpaliwal/agentic-test-session/contrib/tredis => ../contrib/tredis
	github.com/maheshpaliwal/agentic-test-session/contrib/tsql => ../contrib/tsql
)
