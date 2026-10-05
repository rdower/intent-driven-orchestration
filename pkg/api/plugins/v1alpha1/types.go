package plugins

import (
	"context"
	"errors"
	"os"
	"sync"
	"time"

	protobufs "github.com/intel/intent-driven-orchestration/pkg/api/plugins/v1alpha1/protobufs"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"

	"github.com/intel/intent-driven-orchestration/pkg/planner/actuators"
)

// pluginVersion represents a string defining the plugin manager's version.
const pluginVersion = "v1alpha1"

const pluginAuthTokenEnvVar = "IDO_PLUGIN_AUTH_TOKEN"

const pluginAuthMetadataKey = "x-ido-plugin-auth-token"

var errMissingPluginAuthToken = errors.New("missing plugin authentication token")

// PInfo plugin info struct
type PInfo struct {
	// Plugin name that uniquely identifies the plugin for the given plugin type.
	Name string
	// Mandatory endpoint location, it usually represents an internal ip to the pod
	// which will handle all plugin requests.
	Endpoint string
	// Plugin service API versions the plugin supports.
	Version string
}

func getPluginAuthToken() (string, error) {
	token := os.Getenv(pluginAuthTokenEnvVar)
	if token == "" {
		return "", errMissingPluginAuthToken
	}
	return token, nil
}

func pluginAuthUnaryClientInterceptor(token string) grpc.UnaryClientInterceptor {
	return func(ctx context.Context, method string, req interface{}, reply interface{}, cc *grpc.ClientConn, invoker grpc.UnaryInvoker, opts ...grpc.CallOption) error {
		ctx = metadata.AppendToOutgoingContext(ctx, pluginAuthMetadataKey, token)
		return invoker(ctx, method, req, reply, cc, opts...)
	}
}

func pluginAuthStreamClientInterceptor(token string) grpc.StreamClientInterceptor {
	return func(ctx context.Context, desc *grpc.StreamDesc, cc *grpc.ClientConn, method string, streamer grpc.Streamer, opts ...grpc.CallOption) (grpc.ClientStream, error) {
		ctx = metadata.AppendToOutgoingContext(ctx, pluginAuthMetadataKey, token)
		return streamer(ctx, desc, cc, method, opts...)
	}
}

func requirePluginAuth(ctx context.Context, expectedToken string) error {
	md, ok := metadata.FromIncomingContext(ctx)
	if !ok {
		return status.Error(codes.Unauthenticated, "missing metadata")
	}
	values := md.Get(pluginAuthMetadataKey)
	if len(values) != 1 || values[0] != expectedToken {
		return status.Error(codes.Unauthenticated, "invalid plugin authentication token")
	}
	return nil
}

// ActuatorsPluginManager interface for actuator plugins
type ActuatorsPluginManager interface {
	PluginManager
	// Iter thread-safe iterator over registered actuators
	Iter(f func(a actuators.Actuator))
}

// PluginManager interface of an abstract plugin manager which can handle plugin registrations and de-registration
type PluginManager interface {
	// Start starts the grpc server responsible for plugin registrations
	Start() error
	// Stop stops the grpc server responsible for plugin registrations
	Stop() error
	// refreshRegisteredPlugin reconcile callback which checks if plugin connections are still alive and removes all dead connections
	refreshRegisteredPlugin(retries int)
}

type PluginMap map[string]*ActuatorClientStub

// PluginManagerServer implements PluginManager GRPC Server protocol.
type PluginManagerServer struct {
	protobufs.UnimplementedRegistrationServer
	ActuatorsPluginManager
	actuators                []actuators.Actuator
	endpoint                 string
	port                     int
	authToken                string
	registeredPlugins        PluginMap
	registeredPluginsRetries map[string]int
	server                   *grpc.Server
	reconcilePeriod          time.Duration
	retries                  int
	wg                       sync.WaitGroup
	mu                       sync.Mutex
	stop                     chan struct{}
}
