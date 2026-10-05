package plugins

import (
	"context"
	"fmt"
	"sync"
	"time"

	protobufs "github.com/intel/intent-driven-orchestration/pkg/api/plugins/v1alpha1/protobufs"

	"google.golang.org/grpc"
	"google.golang.org/grpc/connectivity"
	"google.golang.org/grpc/credentials/insecure"

	"k8s.io/klog/v2"

	"github.com/intel/intent-driven-orchestration/pkg/common"
	"github.com/intel/intent-driven-orchestration/pkg/planner"
	"github.com/intel/intent-driven-orchestration/pkg/planner/actuators"
)

// ActuatorClientStub GRPC client stub to actuator plugins
type ActuatorClientStub struct {
	actuators.Actuator
	pluginInfo PInfo
	authToken  string
	client     protobufs.ActuatorPluginClient
	clientConn *grpc.ClientConn
	stopTime   time.Time
	mutex      sync.Mutex
	stream     *protobufs.ActuatorPlugin_NextStateClient
}

// newActuatorClientStub creates new client stub for actuator plugins
func newActuatorClientStub(pInfo *protobufs.PluginInfo, numberOfRetries int, authToken string) (*ActuatorClientStub, error) {
	klog.V(2).Infof("Connecting to plugin endpoint: %s.", pInfo.Endpoint)

	// nolint:staticcheck // SA1019: grpc.Dial is deprecated — but supported in 1.0; for GRPC 2.0 we'll need to check if the connection is ready.
	conn, err := grpc.Dial(
		pInfo.Endpoint,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithBlock(),
		grpc.WithUnaryInterceptor(pluginAuthUnaryClientInterceptor(authToken)),
		grpc.WithStreamInterceptor(pluginAuthStreamClientInterceptor(authToken)),
	)
	if err != nil {
		klog.ErrorS(err, "Cannot connect to actuator plugin endpoint: ", pInfo.Endpoint)
	}
	retries := numberOfRetries
	for retries > 0 && err != nil && (conn == nil || conn.GetState() != connectivity.Ready) {
		time.Sleep(5 * time.Second) // TODO: make configurable.
		// nolint:staticcheck // SA1019: grpc.Dial is deprecated — but supported in 1.0; for GRPC 2.0 we'll need to check if the connection is ready.
		conn, err = grpc.Dial(
			pInfo.Endpoint,
			grpc.WithTransportCredentials(insecure.NewCredentials()),
			grpc.WithBlock(),
			grpc.WithUnaryInterceptor(pluginAuthUnaryClientInterceptor(authToken)),
			grpc.WithStreamInterceptor(pluginAuthStreamClientInterceptor(authToken)),
		)
		if err != nil {
			klog.ErrorS(err, "Cannot connect to actuator plugin endpoint: ", pInfo.Endpoint)
		}
		retries--
	}
	if conn == nil || conn.GetState() != connectivity.Ready {
		return nil, fmt.Errorf("failed establishing a connection to endpont %s after %d retries", pInfo.Endpoint, numberOfRetries)
	}
	return &ActuatorClientStub{
		pluginInfo: toPInfo(pInfo),
		authToken:  authToken,
		client:     protobufs.NewActuatorPluginClient(conn),
		clientConn: conn,
	}, nil
}

// toPInfo type convertor from grpc plugin info to internal struct
func toPInfo(p *protobufs.PluginInfo) PInfo {
	return PInfo{
		Name:     p.Name,
		Endpoint: p.Endpoint,
		Version:  p.SupportedVersions,
	}
}

// toGrpcState type convertor from internal state to grpc state
func toGrpcState(s *common.State) *protobufs.State {
	gs := protobufs.State{
		Intent: &protobufs.Intent{
			Key:        s.Intent.Key,
			Priority:   s.Intent.Priority,
			TargetKey:  s.Intent.TargetKey,
			TargetKind: s.Intent.TargetKind,
			Objectives: s.Intent.Objectives,
		},
		CurrentPods: make(map[string]*protobufs.PodState),
		CurrentData: make(map[string]*protobufs.DataEntry),
		Resources:   make(map[string]int64),
		Annotations: make(map[string]string),
	}

	for k, v := range s.CurrentPods {
		gs.CurrentPods[k] = &protobufs.PodState{

			Availability: v.Availability,
			NodeName:     v.NodeName,
			State:        v.State,
			QosClass:     v.QoSClass,
		}
	}
	for k, v := range s.CurrentData {
		gs.CurrentData[k] = &protobufs.DataEntry{Data: v}
	}
	for k, v := range s.Resources {
		gs.Resources[k] = v
	}
	for k, v := range s.Annotations {
		gs.Annotations[k] = v
	}
	return &gs
}

// toGrpcStates type convertor from internal states to grpc states
func toGrpcStates(states []common.State) []*protobufs.State {
	var res []*protobufs.State
	for i := range states {
		res = append(res, toGrpcState(&states[i]))
	}
	return res
}

// toGrpcProfile type convertor from internal profile to grpc profile
func toGrpcProfile(v *common.Profile) *protobufs.Profile {
	return &protobufs.Profile{
		Key:         v.Key,
		ProfileType: protobufs.ProfileType(v.ProfileType), //nolint:gosec // explanation: only limit profile types exists.
		Minimize:    v.Minimize,
	}
}

// toGrpcProfiles type convertor from internal profiles to grpc profiles
func toGrpcProfiles(p map[string]common.Profile) map[string]*protobufs.Profile {
	r := make(map[string]*protobufs.Profile)
	for k, v := range p {
		v := v
		r[k] = toGrpcProfile(&v)
	}
	return r
}

// getNextStateRequest create a new grpc request for the nextState function
func getNextStateRequest(state *common.State, goal *common.State, profiles map[string]common.Profile) *protobufs.NextStateRequest {
	req := protobufs.NextStateRequest{
		State:    toGrpcState(state),
		Goal:     toGrpcState(goal),
		Profiles: toGrpcProfiles(profiles),
	}
	return &req
}

// toGrpcActions type convertor from grpc actions to internal actions
func toGrpcActions(actions []planner.Action) []*protobufs.Action {
	var res []*protobufs.Action
	for _, a := range actions {
		iProp, iok := a.Properties.(map[string]int64)
		sProp, _ := a.Properties.(map[string]string)
		p := protobufs.ActionProperties{
			Type: protobufs.PropertyType_INT_PROPERTY,
		}
		if iok {
			p.IntProperties = iProp
		} else {
			p.Type = protobufs.PropertyType_STRING_PROPERTY
			p.StrProperties = sProp
		}
		res = append(res, &protobufs.Action{
			Name:       a.Name,
			Properties: &p,
		})
	}
	return res
}

// getNextStateResponse unpacks the given nextState response and returns only actions owned by the registered plugin.
func getNextStateResponse(r *protobufs.NextStateResponse, allowedActionName string) ([]common.State, []float64, []planner.Action) {
	var states []common.State
	for _, v := range r.States {
		s := common.State{
			Intent: common.Intent{
				Key:        v.Intent.Key,
				Priority:   v.Intent.Priority,
				TargetKey:  v.Intent.TargetKey,
				TargetKind: v.Intent.TargetKind,
				Objectives: v.Intent.Objectives,
			},
			CurrentPods: make(map[string]common.PodState),
			CurrentData: make(map[string]map[string]float64),
			Resources:   make(map[string]int64),
			Annotations: make(map[string]string),
		}
		for kp, vp := range v.CurrentPods {
			s.CurrentPods[kp] = common.PodState{
				Availability: vp.Availability,
				NodeName:     vp.NodeName,
				State:        vp.State,
				QoSClass:     vp.QosClass,
			}
		}
		for kd, vd := range v.CurrentData {
			s.CurrentData[kd] = vd.Data
		}
		for kd, vd := range v.Resources {
			s.Resources[kd] = vd
		}
		for kd, vd := range v.Annotations {
			s.Annotations[kd] = vd
		}
		states = append(states, s)
	}
	var a []planner.Action
	for idx, v := range r.Actions {
		if v.Name != allowedActionName {
			klog.Warningf("Ignoring unexpected action %q from plugin %q", v.Name, allowedActionName)
			continue
		}
		if idx >= len(r.Utilities) || idx >= len(states) {
			klog.Warningf("Ignoring malformed response from plugin %q due to inconsistent response lengths", allowedActionName)
			break
		}
		var p interface{}
		if v.Properties.Type == protobufs.PropertyType_INT_PROPERTY {
			p = v.Properties.IntProperties
		} else {
			p = v.Properties.StrProperties
		}
		a = append(a, planner.Action{
			Name:       v.Name,
			Properties: p,
		})
	}
	if len(a) == len(r.Actions) {
		return states, r.Utilities, a
	}
	filteredStates := make([]common.State, 0, len(a))
	filteredUtilities := make([]float64, 0, len(a))
	for idx, v := range r.Actions {
		if v.Name != allowedActionName {
			continue
		}
		if idx >= len(states) || idx >= len(r.Utilities) {
			break
		}
		filteredStates = append(filteredStates, states[idx])
		filteredUtilities = append(filteredUtilities, r.Utilities[idx])
	}
	return filteredStates, filteredUtilities, a
}

// getPerformRequest create a new grpc request for the perform function
func getPerformRequest(state *common.State, plan []planner.Action) *protobufs.PerformRequest {
	req := protobufs.PerformRequest{
		State: toGrpcState(state),
		Plan:  toGrpcActions(plan),
	}
	return &req
}

// getEffectRequest create a new grpc request for the effect function
func getEffectRequest(state *common.State, profiles map[string]common.Profile) *protobufs.EffectRequest {
	req := protobufs.EffectRequest{
		State:    toGrpcState(state),
		Profiles: make(map[string]*protobufs.Profile),
	}
	for k, v := range profiles {
		v := v
		req.Profiles[k] = toGrpcProfile(&v)
	}
	return &req
}

// stop the client connection to the plugin
func (a *ActuatorClientStub) stop() {
	a.mutex.Lock()
	defer a.mutex.Unlock()
	if a.clientConn != nil {
		a.clientConn.Close()
	}
	a.stopTime = time.Now()
}

// isStopped returns true if the client connection to plugin is stopped
func (a *ActuatorClientStub) isStopped() bool {
	a.mutex.Lock()
	defer a.mutex.Unlock()
	return !a.stopTime.IsZero()
}

// NextState triggers NextState RPC to plugin
func (a *ActuatorClientStub) NextState(state *common.State, goal *common.State, profiles map[string]common.Profile) ([]common.State, []float64, []planner.Action) {
	klog.V(2).Infof("Invoking NextState for actuator client name:%s endpoint: %s", a.pluginInfo.Name, a.pluginInfo.Endpoint)
	if a.isStopped() {
		klog.Error("NextState ended unexpectedly for the plugin (stop event)")
		return []common.State{}, []float64{}, []planner.Action{}
	}
	if a.stream == nil {

		stream, err := a.client.NextState(context.Background())
		a.stream = &stream
		if err != nil {
			klog.Errorf("Failed to call: %v.", err)
			return nil, nil, nil
		}
	}
	request := getNextStateRequest(state, goal, profiles)
	if err := (*a.stream).Send(request); err != nil {
		klog.Errorf("Failed to send request: %v.", err)
		return nil, nil, nil
	}

	// received stream from server
	response, err := (*a.stream).Recv()
	if err != nil {
		klog.Errorf("Failed to get response: %v.", err)
		return nil, nil, nil
	}

	return getNextStateResponse(response, a.pluginInfo.Name)
}

// Perform triggers Perform RPC to plugin
func (a *ActuatorClientStub) Perform(state *common.State, plan []planner.Action) {
	klog.V(2).Infof("Invoking Perform for actuator client name:%s endpoint: %s", a.pluginInfo.Name, a.pluginInfo.Endpoint)
	if a.isStopped() {
		klog.Error("Perform ended unexpectedly for the plugin (stop event)")
		return
	}
	_, err := a.client.Perform(context.Background(), getPerformRequest(state, plan))

	if err != nil {
		klog.Error(err)
	}
}

// Effect triggers Effect RPC to plugin
func (a *ActuatorClientStub) Effect(state *common.State, profiles map[string]common.Profile) {
	klog.V(2).Infof("Invoking Effect for actuator client name:%s endpoint: %s", a.pluginInfo.Name, a.pluginInfo.Endpoint)
	if a.isStopped() {
		klog.Error("Effect ended unexpectedly for the plugin (stop event)")
		return
	}
	_, err := a.client.Effect(context.Background(), getEffectRequest(state, profiles))

	if err != nil {
		klog.Error(err)
	}
}
