// Copyright [2025] Arnab Chakraborty.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package telemetry

import (
	"context"
	"sync"
	"time"

	"github.com/livekit/livekit-server/pkg/config"
	"github.com/livekit/livekit-server/pkg/telemetry/prometheus"
	"github.com/livekit/protocol/logger"
	redisLiveKit "github.com/livekit/protocol/redis"
	"github.com/redis/go-redis/v9"
)

const (
	defaultRedisDialTimeout  = 4000 // 4s
	defaultRedisReadTimeout  = 3000 // 3s
	defaultRedisWriteTimeout = 3000 // 3s
	defaultRedisPoolTimeout  = 4000 // 4s
)

var (
	configured  bool
	redisClient redis.UniversalClient
	initOnce    sync.Once
)

// InitBandwidthTracker initializes the Redis client for bandwidth tracking.
// It sets `configured` to true only if Redis is up and running.
// Returns an error if Redis was configured but the client failed to initialize.
func InitBandwidthTracker(conf *config.Config) error {
	var initErr error

	logger.Infow("Initializing bandwidth tracker", "redis", conf.Redis)

	initOnce.Do(func() {
		// If Redis isn't configured at all, mark disabled and return.
		if !conf.Redis.IsConfigured() {
			configured = false
			return
		}

		// apply default Redis timeouts if not specified
		if conf.Redis.DialTimeout == 0 {
			conf.Redis.DialTimeout = defaultRedisDialTimeout
		}
		if conf.Redis.ReadTimeout == 0 {
			conf.Redis.ReadTimeout = defaultRedisReadTimeout
		}
		if conf.Redis.WriteTimeout == 0 {
			conf.Redis.WriteTimeout = defaultRedisWriteTimeout
		}
		if conf.Redis.PoolTimeout == 0 {
			conf.Redis.PoolTimeout = defaultRedisPoolTimeout
		}

		// Try to get a Redis client; on error, mark disabled and return it.
		rc, err := redisLiveKit.GetRedisClient(&conf.Redis)
		if err != nil {
			configured, initErr = false, err
			logger.Errorw("Failed to initialize Redis client even though config provided", err)
			return
		}

		// verify connectivity
		ctxInit, cancel := context.WithTimeout(context.Background(), time.Duration(conf.Redis.DialTimeout))
		defer cancel()
		if err := rc.Ping(ctxInit).Err(); err != nil {
			configured, initErr = false, err
			logger.Errorw("Redis ping failed", err)
			return
		}

		// Success: store the client and mark enabled.
		redisClient = rc
		configured = true

		logger.Infow("Bandwidth tracker initialized and ready 👉🏼 ")
	})

	return initErr
}

// Reports whether InitBandwidthTracker succeeded or not.
func isBandwidthTrackerConfigured() bool {
	return configured
}

// Returns the underlying Redis client (may be nil if not configured).
func Client() redis.UniversalClient {
	return redisClient
}

// Acts as a switchboard to store incoming or outgoing bytes for the given room.
func (t *telemetryService) incrementRoomBytes(direction prometheus.Direction, roomName string, bytes uint64) {
	logger.Debugw("Logging room bytes", "direction", direction, "bytes", bytes)

	// if the tracker is not configured then drop the log call
	if !isBandwidthTrackerConfigured() {
		return
	}

	// add a 2s timeout to every call in case it gets stuck
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	if direction == prometheus.Incoming {
		incrementIncoming(ctx, roomName, bytes)
	} else {
		incrementOutgoing(ctx, roomName, bytes)
	}
}

// Increments incoming bytes
func incrementIncoming(ctx context.Context, key string, bytesIn uint64) {
	_, err := Client().HIncrBy(ctx, key, "incoming", int64(bytesIn)).Result()
	if err != nil {
		logger.Errorw("HIncrBy incoming failed: %v", err)
	}
}

// Increment outgoing bytes
func incrementOutgoing(ctx context.Context, key string, bytesOut uint64) {
	_, err := Client().HIncrBy(ctx, key, "outgoing", int64(bytesOut)).Result()
	if err != nil {
		logger.Errorw("HIncrBy outgoing failed: %v", err)
	}
}
