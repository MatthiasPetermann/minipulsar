package broker

import (
	"context"
	"crypto/tls"
	"log/slog"
	"net"
	"os"
	"sync"
	"sync/atomic"
	"time"

	"minipulsar/internal/logging"
	"minipulsar/internal/messaging"
	"minipulsar/internal/storage"
	pulsar "minipulsar/pb"
)

// Config controls broker runtime behavior and logging.
// It allows the CLI to align settings with Pulsar protocol expectations
// such as maximum message size and advertised broker URL.
type Config struct {
	// Logger is the base logger used by the broker for all connection and protocol events.
	Logger *logging.Logger
	// MaxFrameSize limits inbound frames to avoid allocating unbounded memory.
	MaxFrameSize uint32
	// MaxMessageSize is reported to clients during CONNECT as protocol metadata.
	MaxMessageSize int32
	// BrokerServiceURL is returned in LOOKUP responses so clients can connect.
	BrokerServiceURL string
	// BrokerServiceURLTLS is returned in LOOKUP responses for TLS-capable clients.
	BrokerServiceURLTLS string
	// ServerVersion is returned during CONNECT to identify this broker.
	ServerVersion string
	// Messaging configures optional messaging control-plane runtime behavior.
	Messaging *messaging.Runtime
	// JWTSecret is the shared secret used to verify HS256 JWTs from clients.
	JWTSecret []byte
	// TLSConfig enables TLS for the broker listener when non-nil.
	TLSConfig *tls.Config
	// ReadTimeout caps how long a client can block while sending a frame.
	ReadTimeout time.Duration
	// WriteTimeout caps how long the broker can block while writing a frame.
	WriteTimeout time.Duration
	// NamespaceMaintenanceInterval controls how often namespace maintenance runs.
	NamespaceMaintenanceInterval time.Duration
	// AckTimeout controls how long pending messages can remain unacked before redelivery.
	AckTimeout time.Duration
	// AckTimeoutCheckInterval controls how often to scan for expired pending messages.
	AckTimeoutCheckInterval time.Duration
	// MaxConnections rejects excess concurrent TCP connections. Zero disables the limit.
	MaxConnections int
	// MaxProducers rejects producer creation above the broker-wide limit. Zero disables the limit.
	MaxProducers int
	// MaxConsumers rejects consumer creation above the broker-wide limit. Zero disables the limit.
	MaxConsumers int
}

// producerKey scopes producer identifiers by connection to avoid collisions
// across independent client sessions.
type producerKey struct {
	conn net.Conn
	id   uint64
}

// consumerKey scopes consumer identifiers by connection to avoid collisions
// across independent client sessions.
type consumerKey struct {
	conn net.Conn
	id   uint64
}

// Broker manages Pulsar protocol sessions and orchestrates persistence-backed delivery.
// It focuses on connection lifecycle and dispatching commands to storage and protocol helpers.
type Broker struct {
	store *storage.Store
	cfg   Config

	// producers/consumers are keyed by (conn, id) to avoid collisions across connections.
	producers map[producerKey]*producer
	consumers map[consumerKey]*consumer
	// producerConds coordinates wait-for-exclusive producers per topic.
	producerConds map[string]*sync.Cond

	// subscription states keyed by (topic, subscription).
	subs map[subKey]*subState

	// nonPersistentSeq tracks message ids for non-persistent topics.
	nonPersistentSeq map[string]uint64

	mu sync.RWMutex

	// connWrite serializes writes per connection to avoid interleaved frames.
	connWrite sync.Map // net.Conn -> *sync.Mutex

	// nextConsumerUID is a server-side unique consumer uid (used in DB pending).
	nextConsumerUID int64

	// connRoles caches roles extracted from CONNECT authentication payloads.
	connRoles map[net.Conn][]string

	messageCounter    atomic.Int64
	throughputMu      sync.Mutex
	lastThroughputAt  time.Time
	lastThroughputCnt int64

	throttleDelay  atomic.Int64
	throttlePaused atomic.Bool

	lifecycleCtx    context.Context
	lifecycleCancel context.CancelFunc
	lifecycleWG     sync.WaitGroup
	shutdownOnce    sync.Once
	listeners       map[net.Listener]struct{}
	connections     map[net.Conn]struct{}
}

// producer represents a logical producer created by a client connection.
// Producers are scoped to the connection that created them.
type producer struct {
	id         uint64
	topic      string
	persistent bool
	conn       net.Conn
	accessMode pulsar.ProducerAccessMode
}

// consumer represents a Pulsar consumer that receives messages for a subscription.
// It tracks permit-based flow control for shared subscriptions.
type consumer struct {
	id           uint64 // protocol consumer id (per connection)
	uid          int64  // server unique id (global)
	topic        string
	subscription string
	persistent   bool
	conn         net.Conn
	subType      pulsar.CommandSubscribe_SubType
	priority     int

	mu      sync.Mutex
	permits int
}

// subKey uniquely identifies a subscription for a topic.
type subKey struct {
	topic string
	name  string
}

// subState holds in-memory delivery state for a subscription shared across consumers.
type subState struct {
	key subKey

	persistent bool
	subType    pulsar.CommandSubscribe_SubType
	mu         sync.Mutex
	consumers  []*consumer
	rr         int
	delivering bool
}

// New constructs a broker with defaults for any missing config fields.
func New(store *storage.Store, cfg Config) *Broker {
	logger := cfg.Logger
	if logger == nil {
		defaultLogger, err := logging.New(logging.Options{
			Format:        "text",
			WithTimestamp: true,
			Level:         slog.LevelInfo,
			Writer:        os.Stdout,
		})
		if err == nil {
			logger = defaultLogger.With("component", "broker")
		}
	}
	if cfg.MaxFrameSize == 0 {
		cfg.MaxFrameSize = 10 * 1024 * 1024
	}
	if cfg.MaxMessageSize == 0 {
		cfg.MaxMessageSize = 5 * 1024 * 1024
	}
	if cfg.BrokerServiceURL == "" {
		cfg.BrokerServiceURL = "pulsar://localhost:6650"
	}
	if cfg.BrokerServiceURLTLS == "" && cfg.TLSConfig != nil {
		cfg.BrokerServiceURLTLS = "pulsar+ssl://localhost:6651"
	}
	if cfg.ServerVersion == "" {
		cfg.ServerVersion = "minipulsar-0.1"
	}
	if cfg.ReadTimeout == 0 {
		cfg.ReadTimeout = 15 * time.Second
	}
	if cfg.WriteTimeout == 0 {
		cfg.WriteTimeout = 15 * time.Second
	}
	if cfg.NamespaceMaintenanceInterval == 0 {
		cfg.NamespaceMaintenanceInterval = defaultNamespaceMaintenanceInterval
	}
	cfg.Logger = logger

	lifecycleCtx, lifecycleCancel := context.WithCancel(context.Background())
	b := &Broker{
		store:            store,
		cfg:              cfg,
		producers:        make(map[producerKey]*producer),
		consumers:        make(map[consumerKey]*consumer),
		producerConds:    make(map[string]*sync.Cond),
		subs:             make(map[subKey]*subState),
		nonPersistentSeq: make(map[string]uint64),
		connRoles:        make(map[net.Conn][]string),
		lifecycleCtx:     lifecycleCtx,
		lifecycleCancel:  lifecycleCancel,
		listeners:        make(map[net.Listener]struct{}),
		connections:      make(map[net.Conn]struct{}),
	}
	b.startNamespaceMaintenance()
	b.startAckTimeoutMonitor()
	return b
}

// nextUID returns a unique consumer UID for tracking pending delivery state.
func (b *Broker) nextUID() int64 {
	return atomic.AddInt64(&b.nextConsumerUID, 1)
}
