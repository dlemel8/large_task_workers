module producer

go 1.16

require (
	github.com/go-redis/redis/v8 v8.11.4
	github.com/pkg/errors v0.9.1
	github.com/prometheus/client_golang v1.11.0
	github.com/sirupsen/logrus v1.8.1
	github.com/spf13/viper v1.9.0
	google.golang.org/protobuf v1.27.1
	protos v0.0.0
)

replace protos => ../protos/
