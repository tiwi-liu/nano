// Copyright (c) nano Authors. All Rights Reserved.
//
// Permission is hereby granted, free of charge, to any person obtaining a copy
// of this software and associated documentation files (the "Software"), to deal
// in the Software without restriction, including without limitation the rights
// to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
// copies of the Software, and to permit persons to whom the Software is
// furnished to do so, subject to the following conditions:
//
// The above copyright notice and this permission notice shall be included in all
// copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
// SOFTWARE.

package cluster

import (
	"context"
	"errors"
	"fmt"
	"github.com/lonng/nano/pkg/errcode"
	"google.golang.org/grpc/metadata"
	"net"
	"net/http"
	"reflect"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/gorilla/websocket"
	"github.com/lonng/nano/cluster/clusterpb"
	"github.com/lonng/nano/component"
	"github.com/lonng/nano/internal/env"
	"github.com/lonng/nano/internal/log"
	"github.com/lonng/nano/internal/message"
	"github.com/lonng/nano/pipeline"
	"github.com/lonng/nano/registry"
	"github.com/lonng/nano/scheduler"
	"github.com/lonng/nano/session"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// Options contains some configurations for current node
type Options struct {
	Pipeline                pipeline.Pipeline
	IsMaster                bool
	AdvertiseAddr           string
	MemberAddr              string
	LocalMemberID           string
	RetryInterval           time.Duration
	ClientAddr              string
	Components              *component.Components
	Label                   string
	IsWebsocket             bool
	TSLCertificate          string
	TSLKey                  string
	UnregisterCallback      func(Member)
	RemoteServiceRoute      CustomerRemoteServiceRoute
	RequestTimeout          time.Duration
	RPCTimeout              time.Duration
	WriteTimeout            time.Duration
	ClusterAuthToken        string
	ServiceRegistry         registry.Registry
	UnaryServerInterceptors []grpc.UnaryServerInterceptor
	UnaryClientInterceptors []grpc.UnaryClientInterceptor
	SessionBoundCallback    func(*session.Session)
	SessionClosedCallback   func(*session.Session)
}

const DefaultRequestTimeout = 5 * time.Second
const DefaultRPCTimeout = 3 * time.Second
const DefaultWriteTimeout = 5 * time.Second

// Node represents a node in nano cluster, which will contains a group of services.
// All services will register to cluster and messages will be forwarded to the node
// which provides respective service
type Node struct {
	Options            // current node options
	ServiceAddr string // current server service address (RPC)

	cluster   *cluster
	handler   *LocalHandler
	server    *grpc.Server
	rpcClient *rpcClient
	listener  net.Listener
	httpSrv   *http.Server

	mu       sync.RWMutex
	sessions map[int64]*session.Session

	once          sync.Once
	keepaliveExit chan struct{}
	registryExit  context.CancelFunc
	shuttingDown  int32
}

func (n *Node) Startup() error {
	if n.ServiceAddr == "" {
		return errors.New("service address cannot be empty in master node")
	}
	if n.MemberAddr == "" {
		n.MemberAddr = n.ServiceAddr
	}
	n.sessions = map[int64]*session.Session{}
	n.cluster = newCluster(n)
	n.handler = NewHandler(n, n.Pipeline)
	components := n.Components.List()
	for _, c := range components {
		err := n.handler.register(c.Comp, c.Opts)
		if err != nil {
			return err
		}
	}

	cache()
	if err := n.initNode(); err != nil {
		return err
	}

	// Initialize all components
	for _, c := range components {
		c.Comp.Init()
	}
	for _, c := range components {
		c.Comp.AfterInit()
	}

	if n.ClientAddr != "" {
		go func() {
			if n.IsWebsocket {
				if len(n.TSLCertificate) != 0 {
					n.listenAndServeWSTLS()
				} else {
					n.listenAndServeWS()
				}
			} else {
				n.listenAndServe()
			}
		}()
	}

	return nil
}

func (n *Node) Handler() *LocalHandler {
	return n.handler
}

func (n *Node) memberAddr() string {
	if n.MemberAddr != "" {
		return n.MemberAddr
	}
	return n.ServiceAddr
}

func (n *Node) initNode() error {
	// Current node is not master server and does not contain a discovery backend,
	// so it runs in singleton mode.
	if !n.IsMaster && n.AdvertiseAddr == "" && n.ServiceRegistry == nil {
		return nil
	}

	listener, err := net.Listen("tcp", n.ServiceAddr)
	if err != nil {
		return err
	}

	// Initialize the gRPC server and register service
	serverInterceptors := append([]grpc.UnaryServerInterceptor{n.authUnaryInterceptor()}, n.UnaryServerInterceptors...)
	n.server = grpc.NewServer(grpc.ChainUnaryInterceptor(serverInterceptors...))
	n.rpcClient = newRPCClient(n.UnaryClientInterceptors...)
	clusterpb.RegisterMemberServer(n.server, n)

	go func() {
		err := n.server.Serve(listener)
		if err != nil {
			log.Fatalf("Start current node failed: %v", err)
		}
	}()

	if n.IsMaster {
		clusterpb.RegisterMasterServer(n.server, n.cluster)
		member := &Member{
			isMaster: true,
			memberInfo: &clusterpb.MemberInfo{
				Label:       n.Label,
				ServiceAddr: n.memberAddr(),
				Services:    n.handler.LocalService(),
			},
		}
		n.cluster.members = append(n.cluster.members, member)
		n.cluster.setRpcClient(n.rpcClient)
	} else if n.ServiceRegistry != nil {
		if err := n.startRegistryDiscovery(); err != nil {
			return err
		}
	} else {
		pool, err := n.rpcClient.getConnPool(n.AdvertiseAddr)
		if err != nil {
			return err
		}
		client := clusterpb.NewMasterClient(pool.Get())
		request := &clusterpb.RegisterRequest{
			MemberInfo: &clusterpb.MemberInfo{
				Label:       n.Label,
				ServiceAddr: n.memberAddr(),
				Services:    n.handler.LocalService(),
			},
		}
		for {
			ctx, cancel := n.rpcContext(context.Background())
			resp, err := client.Register(ctx, request)
			cancel()
			if err == nil {
				n.handler.initRemoteService(resp.Members)
				n.cluster.initMembers(resp.Members)
				break
			}
			log.Println("Register current node to cluster failed", err, "and will retry in", n.RetryInterval.String())
			time.Sleep(n.RetryInterval)
		}
		n.once.Do(n.keepalive)
	}
	return nil
}

// Shutdowns all components registered by application, that
// call by reverse order against register
func (n *Node) Shutdown() {
	atomic.StoreInt32(&n.shuttingDown, 1)

	// reverse call `BeforeShutdown` hooks
	components := n.Components.List()
	length := len(components)
	for i := length - 1; i >= 0; i-- {
		components[i].Comp.BeforeShutdown()
	}

	// reverse call `Shutdown` hooks
	for i := length - 1; i >= 0; i-- {
		components[i].Comp.Shutdown()
	}
	// close sendHeartbeat
	if n.keepaliveExit != nil {
		close(n.keepaliveExit)
	}
	if n.registryExit != nil {
		n.registryExit()
	}
	if !n.IsMaster && n.AdvertiseAddr != "" && n.ServiceRegistry == nil {
		pool, err := n.rpcClient.getConnPool(n.AdvertiseAddr)
		if err != nil {
			log.Println("Retrieve master address error", err)
			goto EXIT
		}
		client := clusterpb.NewMasterClient(pool.Get())
		request := &clusterpb.UnregisterRequest{
			ServiceAddr: n.memberAddr(),
		}
		ctx, cancel := n.rpcContext(context.Background())
		_, err = client.Unregister(ctx, request)
		cancel()
		if err != nil {
			log.Println("Unregister current node failed", err)
			goto EXIT
		}
	}

EXIT:
	if n.httpSrv != nil {
		ctx, cancel := context.WithTimeout(context.Background(), n.rpcTimeout())
		if err := n.httpSrv.Shutdown(ctx); err != nil && !errors.Is(err, http.ErrServerClosed) {
			log.Println("HTTP server shutdown failed", err)
		}
		cancel()
	}
	if n.listener != nil {
		_ = n.listener.Close()
	}
	if n.server != nil {
		n.server.GracefulStop()
	}
	if n.rpcClient != nil {
		n.rpcClient.closePool()
	}
}

// Enable current server accept connection
func (n *Node) listenAndServe() {
	listener, err := net.Listen("tcp", n.ClientAddr)
	if err != nil {
		log.Fatal(err.Error())
	}
	n.listener = listener

	defer listener.Close()
	for {
		conn, err := listener.Accept()
		if err != nil {
			if atomic.LoadInt32(&n.shuttingDown) != 0 {
				return
			}
			log.Println(err.Error())
			continue
		}

		go n.handler.handle(conn)
	}
}

func (n *Node) listenAndServeWS() {
	var upgrader = websocket.Upgrader{
		ReadBufferSize:  1024,
		WriteBufferSize: 1024,
		CheckOrigin:     env.CheckOrigin,
	}

	mux := http.NewServeMux()
	mux.HandleFunc("/"+strings.TrimPrefix(env.WSPath, "/"), func(w http.ResponseWriter, r *http.Request) {
		conn, err := upgrader.Upgrade(w, r, nil)
		if err != nil {
			log.Println(fmt.Sprintf("Upgrade failure, URI=%s, Error=%s", r.RequestURI, err.Error()))
			return
		}

		n.handler.handleWS(conn)
	})

	n.httpSrv = &http.Server{Addr: n.ClientAddr, Handler: mux}
	if err := n.httpSrv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
		log.Fatal(err.Error())
	}
}

func (n *Node) listenAndServeWSTLS() {
	var upgrader = websocket.Upgrader{
		ReadBufferSize:  1024,
		WriteBufferSize: 1024,
		CheckOrigin:     env.CheckOrigin,
	}

	mux := http.NewServeMux()
	mux.HandleFunc("/"+strings.TrimPrefix(env.WSPath, "/"), func(w http.ResponseWriter, r *http.Request) {
		conn, err := upgrader.Upgrade(w, r, nil)
		if err != nil {
			log.Println(fmt.Sprintf("Upgrade failure, URI=%s, Error=%s", r.RequestURI, err.Error()))
			return
		}

		n.handler.handleWS(conn)
	})

	n.httpSrv = &http.Server{Addr: n.ClientAddr, Handler: mux}
	if err := n.httpSrv.ListenAndServeTLS(n.TSLCertificate, n.TSLKey); err != nil && !errors.Is(err, http.ErrServerClosed) {
		log.Fatal(err.Error())
	}
}

func (n *Node) storeSession(s *session.Session) {
	n.mu.Lock()
	n.sessions[s.ID()] = s
	n.mu.Unlock()
}

func (n *Node) removeSession(sid int64) (*session.Session, bool) {
	n.mu.Lock()
	s, found := n.sessions[sid]
	delete(n.sessions, sid)
	n.mu.Unlock()
	return s, found
}

func (n *Node) clearSessionRoutesForAddress(addr string) {
	n.mu.RLock()
	sessions := make([]*session.Session, 0, len(n.sessions))
	for _, s := range n.sessions {
		sessions = append(sessions, s)
	}
	n.mu.RUnlock()

	for _, s := range sessions {
		s.Router().DeleteAddress(addr)
	}
}

func (n *Node) findSession(sid int64) *session.Session {
	n.mu.RLock()
	s := n.sessions[sid]
	n.mu.RUnlock()
	return s
}

func (n *Node) findOrCreateSession(sid int64, gateAddr string) (*session.Session, error) {
	n.mu.RLock()
	s, found := n.sessions[sid]
	n.mu.RUnlock()
	if !found {
		conns, err := n.rpcClient.getConnPool(gateAddr)
		if err != nil {
			return nil, err
		}
		ac := &acceptor{
			sid:        sid,
			gateClient: clusterpb.NewMemberClient(conns.Get()),
			rpcHandler: n.handler.remoteProcess,
			gateAddr:   gateAddr,
			node:       n,
		}
		s = session.New(ac)
		ac.session = s
		n.mu.Lock()
		n.sessions[sid] = s
		n.mu.Unlock()
	}
	return s, nil
}

func (n *Node) Call(ctx context.Context, req *clusterpb.InternalCallRequest) (*clusterpb.InternalCallResponse, error) {
	response := &clusterpb.InternalCallResponse{ErrCode: uint64(errcode.CodeOk)}
	if req == nil || req.Route == "" || req.Uid < 0 {
		response.ErrCode = uint64(errcode.CodeBadRequest)
		return response, nil
	}
	handler, found := n.handler.localHandlers[req.Route]
	if !found {
		response.ErrCode = uint64(errcode.CodeMethodNotFound)
		return response, nil
	}

	var data interface{}
	if handler.IsRawArg {
		data = req.Data
	} else {
		data = reflect.New(handler.Type.Elem()).Interface()
		if err := env.Serializer.Unmarshal(req.Data, data); err != nil {
			response.ErrCode = uint64(errcode.CodeProtoParseFail)
			return response, nil
		}
	}

	var sendErr error
	requestContext, cancel := session.NewInternalRequestContext(ctx, req.Uid, n.handler, func(value interface{}, code errcode.Code) error {
		response.ErrCode = uint64(code)
		if code != errcode.CodeOk {
			response.Data = nil
			return nil
		}
		response.Data, sendErr = message.Serialize(value)
		return sendErr
	})
	defer cancel()
	args := []reflect.Value{handler.Receiver, reflect.ValueOf(requestContext), reflect.ValueOf(data)}
	invokeHandler(handler, args, requestContext, &message.Message{Type: message.Request, Route: req.Route})
	if sendErr != nil {
		return nil, sendErr
	}
	return response, nil
}

func (n *Node) HandleRequest(ctx context.Context, req *clusterpb.RequestMessage) (*clusterpb.MemberHandleResponse, error) {
	uid, identityErr := forwardedUID(ctx)
	s, err := n.findOrCreateSession(req.SessionId, req.GateAddr)
	if err != nil {
		fmt.Printf("findOrCreateSession uid=>%v \n", err)
		return nil, err
	}
	if identityErr != nil {
		s.NetworkEntity().SendResponse(req.Id, errcode.CodePermissionDenied, nil)
		log.Println(fmt.Sprintf("Reject forwarded request identity, SID=%d, Error=%v", req.SessionId, identityErr))
		return &clusterpb.MemberHandleResponse{}, nil
	}
	if !bindForwardedUID(s, uid, req.Id) {
		log.Println(fmt.Sprintf("Reject forwarded request identity mismatch, SID=%d", req.SessionId))
		return &clusterpb.MemberHandleResponse{}, nil
	}
	handler, found := n.handler.localHandlers[req.Route]
	if !found {
		s.NetworkEntity().SendResponse(req.Id, errcode.CodeMethodNotFound, nil)
		return nil, fmt.Errorf("service not found in current node: %v", req.Route)
	}

	msg := &message.Message{
		Type:  message.Request,
		ID:    req.Id,
		Route: req.Route,
		Data:  req.Data,
	}
	n.handler.localProcess(ctx, handler, req.Id, s, msg)
	return &clusterpb.MemberHandleResponse{}, nil
}

func (n *Node) HandleNotify(ctx context.Context, req *clusterpb.NotifyMessage) (*clusterpb.MemberHandleResponse, error) {
	uid, identityErr := forwardedUID(ctx)
	s, err := n.findOrCreateSession(req.SessionId, req.GateAddr)
	if err != nil {
		return nil, err
	}
	if identityErr != nil {
		return nil, fmt.Errorf("forwarded notify identity rejected: %w", identityErr)
	}
	if !bindForwardedUID(s, uid, 0) {
		return nil, fmt.Errorf("forwarded notify identity mismatch")
	}
	handler, found := n.handler.localHandlers[req.Route]
	if !found {
		return nil, fmt.Errorf("service not found in current node: %v", req.Route)
	}
	msg := &message.Message{
		Type:  message.Notify,
		Route: req.Route,
		Data:  req.Data,
	}
	n.handler.localProcess(ctx, handler, 0, s, msg)
	return &clusterpb.MemberHandleResponse{}, nil
}

func forwardedUID(ctx context.Context) (int64, error) {
	values := metadata.ValueFromIncomingContext(ctx, "uid")
	if len(values) != 1 {
		return 0, fmt.Errorf("expected one forwarded uid, got %d", len(values))
	}
	uid, err := strconv.ParseInt(values[0], 10, 64)
	if err != nil || uid < 0 {
		return 0, fmt.Errorf("invalid forwarded uid")
	}
	return uid, nil
}

func bindForwardedUID(s *session.Session, uid int64, mid uint64) bool {
	if s == nil || (uid == 0 && s.UID() != 0) {
		if s != nil && mid > 0 {
			s.NetworkEntity().SendResponse(mid, errcode.CodePermissionDenied, nil)
		}
		return false
	}
	if uid == 0 {
		return true
	}
	if err := s.Bind(uid); err != nil {
		if mid > 0 {
			s.NetworkEntity().SendResponse(mid, errcode.CodePermissionDenied, nil)
		}
		return false
	}
	return true
}

// 网关接受来之其他节点的中继Push
func (n *Node) HandlePush(_ context.Context, req *clusterpb.PushMessage) (*clusterpb.MemberHandleResponse, error) {
	s := n.findSession(req.SessionId)
	if s == nil {
		return &clusterpb.MemberHandleResponse{}, status.Errorf(codes.NotFound, "session not found: %v", req.SessionId)
	}
	if err := validateTargetSession(s, req.GetUid(), req.GetConnectionEpoch()); err != nil {
		return &clusterpb.MemberHandleResponse{}, err
	}
	return &clusterpb.MemberHandleResponse{}, s.Push(req.Route, req.Data)
}

// 网关接受来之其他节点的中继Response
func (n *Node) HandleResponse(_ context.Context, req *clusterpb.ResponseMessage) (*clusterpb.MemberHandleResponse, error) {
	s := n.findSession(req.SessionId)
	if s == nil {
		return &clusterpb.MemberHandleResponse{}, fmt.Errorf("session not found: %v", req.SessionId)
	}
	wasUnbound := s.UID() == 0
	if !bindForwardedUID(s, req.Uid, req.Id) {
		log.Println(fmt.Sprintf("Reject forwarded response identity mismatch, SID=%d", req.SessionId))
		return &clusterpb.MemberHandleResponse{}, nil
	}
	if wasUnbound && s.UID() > 0 && n.SessionBoundCallback != nil {
		n.SessionBoundCallback(s)
	}
	code, ok := errcode.FromWire(req.ErrCode)
	if !ok {
		code = errcode.CodeUnknown
	}
	return &clusterpb.MemberHandleResponse{}, s.NetworkEntity().SendResponse(req.Id, code, req.Data)
}

func (n *Node) NewMember(_ context.Context, req *clusterpb.NewMemberRequest) (*clusterpb.NewMemberResponse, error) {
	n.handler.addRemoteService(req.MemberInfo)
	n.cluster.addMember(req.MemberInfo)
	return &clusterpb.NewMemberResponse{}, nil
}

func (n *Node) DelMember(_ context.Context, req *clusterpb.DelMemberRequest) (*clusterpb.DelMemberResponse, error) {
	log.Println("DelMember member", req.String())
	n.handler.delMember(req.ServiceAddr)
	n.cluster.delMember(req.ServiceAddr)
	return &clusterpb.DelMemberResponse{}, nil
}

// SessionClosed implements the MemberServer interface
func (n *Node) SessionClosed(_ context.Context, req *clusterpb.SessionClosedRequest) (*clusterpb.SessionClosedResponse, error) {
	n.mu.Lock()
	s, found := n.sessions[req.SessionId]
	delete(n.sessions, req.SessionId)
	n.mu.Unlock()
	if found {
		scheduler.PushTask(func() {
			session.Lifetime.Close(s)
			s.Release()
		})
	}
	return &clusterpb.SessionClosedResponse{}, nil
}

// CloseSession implements the MemberServer interface
func (n *Node) CloseSession(_ context.Context, req *clusterpb.CloseSessionRequest) (*clusterpb.CloseSessionResponse, error) {
	n.mu.Lock()
	s, found := n.sessions[req.SessionId]
	if found {
		if err := validateTargetSession(s, req.GetUid(), req.GetConnectionEpoch()); err != nil {
			n.mu.Unlock()
			return &clusterpb.CloseSessionResponse{}, err
		}
	}
	delete(n.sessions, req.SessionId)
	n.mu.Unlock()
	if !found && (req.GetUid() > 0 || req.GetConnectionEpoch() > 0) {
		return &clusterpb.CloseSessionResponse{}, status.Errorf(codes.NotFound, "session not found: %v", req.SessionId)
	}
	if found {
		if req.GetKick() {
			if err := s.Kick(req.GetData()); err != nil {
				s.Close()
			}
		} else {
			s.Close()
		}
	}
	return &clusterpb.CloseSessionResponse{}, nil
}

func validateTargetSession(client *session.Session, uid int64, epoch uint64) error {
	if client == nil {
		return status.Error(codes.NotFound, "session not found")
	}
	if uid > 0 && client.UID() != uid {
		return status.Errorf(codes.FailedPrecondition, "session uid mismatch: got %d, want %d", client.UID(), uid)
	}
	if epoch > 0 && client.ConnectionEpoch() != epoch {
		return status.Errorf(codes.FailedPrecondition, "session epoch mismatch: got %d, want %d", client.ConnectionEpoch(), epoch)
	}
	return nil
}

// ticker send heartbeat register info to master
func (n *Node) keepalive() {
	if n.keepaliveExit == nil {
		n.keepaliveExit = make(chan struct{})
	}
	if n.AdvertiseAddr == "" || n.IsMaster {
		return
	}
	heartbeat := func() {
		pool, err := n.rpcClient.getConnPool(n.AdvertiseAddr)
		if err != nil {
			log.Println("rpcClient master conn", err)
			return
		}
		masterCli := clusterpb.NewMasterClient(pool.Get())
		ctx, cancel := n.rpcContext(context.Background())
		_, err = masterCli.Heartbeat(ctx, &clusterpb.HeartbeatRequest{
			MemberInfo: &clusterpb.MemberInfo{
				Label:       n.Label,
				ServiceAddr: n.memberAddr(),
				Services:    n.handler.LocalService(),
			},
		})
		cancel()
		if err != nil {
			log.Println("Member send heartbeat error", err)
		}
	}
	go func() {
		ticker := time.NewTicker(env.Heartbeat)
		for {
			select {
			case <-ticker.C:
				heartbeat()
			case <-n.keepaliveExit:
				log.Println("Exit member node heartbeat ")
				ticker.Stop()
				return
			}
		}
	}()
}
