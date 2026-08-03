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
	"crypto/sha256"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/lonng/nano/pkg/errcode"
	"math/rand"
	"net"
	"reflect"
	"runtime/debug"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/gorilla/websocket"
	"github.com/lonng/nano/cluster/clusterpb"
	"github.com/lonng/nano/component"
	"github.com/lonng/nano/internal/codec"
	"github.com/lonng/nano/internal/env"
	"github.com/lonng/nano/internal/log"
	"github.com/lonng/nano/internal/message"
	"github.com/lonng/nano/internal/packet"
	"github.com/lonng/nano/pipeline"
	"github.com/lonng/nano/registry"
	"github.com/lonng/nano/scheduler"
	"github.com/lonng/nano/service"
	"github.com/lonng/nano/session"
)

var (
	// cached serialized data
	hrd []byte // handshake response data
	hbd []byte // heartbeat packet data
)

type rpcHandler func(session *session.Session, msg *message.Message, noCopy bool)

// CustomerRemoteServiceRoute customer remote service route
type CustomerRemoteServiceRoute func(service string, session *session.Session, members []*clusterpb.MemberInfo) *clusterpb.MemberInfo

func cache() {
	hrdata := map[string]interface{}{
		"code": 200,
		"sys": map[string]interface{}{
			"heartbeat":  env.Heartbeat.Seconds(),
			"servertime": time.Now().UTC().Unix(),
		},
	}
	if dict, ok := message.GetDictionary(); ok {
		hrdata = map[string]interface{}{
			"code": 200,
			"sys": map[string]interface{}{
				"heartbeat":  env.Heartbeat.Seconds(),
				"servertime": time.Now().UTC().Unix(),
				"dict":       dict,
			},
		}
	}
	// data, err := json.Marshal(map[string]interface{}{
	// 	"code": 200,
	// 	"sys": map[string]float64{
	// 		"heartbeat": env.Heartbeat.Seconds(),
	// 	},
	// })
	data, err := json.Marshal(hrdata)
	if err != nil {
		panic(err)
	}

	hrd, err = codec.Encode(packet.Handshake, data)
	if err != nil {
		panic(err)
	}

	hbd, err = codec.Encode(packet.Heartbeat, nil)
	if err != nil {
		panic(err)
	}
}

type LocalHandler struct {
	localServices map[string]*component.Service // all registered service
	localHandlers map[string]*component.Handler // all handler method

	mu             sync.RWMutex
	remoteServices map[string][]*clusterpb.MemberInfo
	instances      map[string]registry.Member

	pipeline    pipeline.Pipeline
	currentNode *Node
}

type InternalCallError struct {
	Code errcode.Code
}

func (e *InternalCallError) Error() string {
	return fmt.Sprintf("internal call failed with system code %d", e.Code)
}

func (h *LocalHandler) Call(ctx context.Context, uid int64, route string, request, response interface{}) error {
	index := strings.LastIndex(route, ".")
	if index < 1 || response == nil {
		return &InternalCallError{Code: errcode.CodeBadRequest}
	}
	data, err := message.Serialize(request)
	if err != nil {
		return err
	}
	callRequest := &clusterpb.InternalCallRequest{Route: route, Data: data, Uid: uid}

	var callResponse *clusterpb.InternalCallResponse
	if _, local := h.localHandlers[route]; local {
		callResponse, err = h.currentNode.Call(ctx, callRequest)
	} else {
		service := route[:index]
		members := h.findMembers(service)
		if len(members) == 0 {
			return &InternalCallError{Code: errcode.CodeServiceNotFound}
		}
		member := members[rand.Intn(len(members))]
		pool, poolErr := h.currentNode.rpcClient.getConnPool(member.ServiceAddr)
		if poolErr != nil {
			return poolErr
		}
		rpcCtx, cancel := h.currentNode.rpcContext(ctx)
		callResponse, err = clusterpb.NewMemberClient(pool.Get()).Call(rpcCtx, callRequest)
		cancel()
	}
	if err != nil {
		return err
	}
	return decodeInternalCallResponse(callResponse, response)
}

func (h *LocalHandler) CallTo(ctx context.Context, uid int64, memberID, route string, request, response interface{}) error {
	index := strings.LastIndex(route, ".")
	if index < 1 || response == nil || memberID == "" {
		return &InternalCallError{Code: errcode.CodeBadRequest}
	}
	if h.currentNode != nil && memberID == h.currentNode.LocalMemberID {
		if _, local := h.localHandlers[route]; !local {
			return &InternalCallError{Code: errcode.CodeServiceNotFound}
		}
		return h.Call(ctx, uid, route, request, response)
	}
	member, ok := h.findAddressableInstance(memberID)
	if !ok || !registryMemberHasService(member, route[:index]) {
		return &InternalCallError{Code: errcode.CodeServiceNotFound}
	}
	data, err := message.Serialize(request)
	if err != nil {
		return err
	}
	callRequest := &clusterpb.InternalCallRequest{Route: route, Data: data, Uid: uid}
	pool, err := h.currentNode.rpcClient.getConnPool(member.ServiceAddr)
	if err != nil {
		return err
	}
	rpcCtx, cancel := h.currentNode.rpcContext(ctx)
	callResponse, err := clusterpb.NewMemberClient(pool.Get()).Call(rpcCtx, callRequest)
	cancel()
	if err != nil {
		return err
	}
	return decodeInternalCallResponse(callResponse, response)
}

func (h *LocalHandler) SelectByKey(service, key string) (string, error) {
	member, ok := h.selectInstanceByKey(service, key)
	if ok {
		return member.ID, nil
	}
	if h.currentNode != nil && h.currentNode.LocalMemberID != "" {
		if _, local := h.localServices[service]; local {
			return h.currentNode.LocalMemberID, nil
		}
	}
	return "", &InternalCallError{Code: errcode.CodeServiceNotFound}
}

func (h *LocalHandler) newRequestContext(parent context.Context, s *session.Session, mid uint64) (*session.RequestContext, context.CancelFunc) {
	timeout := h.currentNode.RequestTimeout
	if timeout <= 0 {
		timeout = DefaultRequestTimeout
	}
	if parent == nil {
		parent = context.Background()
	}
	// Forwarded handlers run asynchronously after the gRPC method returns. Keep
	// trace values, but let the request timeout own the handler cancellation.
	parent, parentCancel := context.WithTimeout(context.WithoutCancel(parent), timeout)
	ctx, cancel := session.NewRequestContext(parent, s, mid, h)
	return ctx, func() {
		cancel()
		parentCancel()
	}
}

func NewHandler(currentNode *Node, pipeline pipeline.Pipeline) *LocalHandler {
	h := &LocalHandler{
		localServices:  make(map[string]*component.Service),
		localHandlers:  make(map[string]*component.Handler),
		remoteServices: map[string][]*clusterpb.MemberInfo{},
		instances:      map[string]registry.Member{},
		pipeline:       pipeline,
		currentNode:    currentNode,
	}

	return h
}

func decodeInternalCallResponse(callResponse *clusterpb.InternalCallResponse, response interface{}) error {
	if callResponse == nil {
		return &InternalCallError{Code: errcode.CodeUnknown}
	}
	code, ok := errcode.FromWire(callResponse.ErrCode)
	if !ok {
		code = errcode.CodeUnknown
	}
	if code != errcode.CodeOk {
		return &InternalCallError{Code: code}
	}
	if raw, ok := response.(*[]byte); ok {
		*raw = append((*raw)[:0], callResponse.Data...)
		return nil
	}
	return env.Serializer.Unmarshal(callResponse.Data, response)
}

func (h *LocalHandler) syncRegistryInstances(members []registry.Member, selfAddr string) {
	next := make(map[string]registry.Member, len(members))
	for _, member := range members {
		if member.ID == "" || member.ServiceAddr == "" || member.ServiceAddr == selfAddr || !registry.IsAddressable(member.Status) {
			continue
		}
		next[member.ID] = member
	}
	h.mu.Lock()
	h.instances = next
	h.mu.Unlock()
}

func (h *LocalHandler) applyRegistryInstance(event registry.Event, selfAddr string) {
	h.mu.Lock()
	defer h.mu.Unlock()
	if event.Type == registry.EventDelete || event.Member.ID == "" || event.Member.ServiceAddr == "" || event.Member.ServiceAddr == selfAddr || !registry.IsAddressable(event.Member.Status) {
		delete(h.instances, event.Member.ID)
		return
	}
	h.instances[event.Member.ID] = event.Member
}

func (h *LocalHandler) findAddressableInstance(memberID string) (registry.Member, bool) {
	h.mu.RLock()
	defer h.mu.RUnlock()
	member, ok := h.instances[memberID]
	return member, ok && registry.IsAddressable(member.Status)
}

func (h *LocalHandler) selectInstanceByKey(service, key string) (registry.Member, bool) {
	h.mu.RLock()
	defer h.mu.RUnlock()
	var selected registry.Member
	var selectedScore uint64
	found := false
	for _, member := range h.instances {
		if !registry.IsRoutable(member.Status) || !registryMemberHasService(member, service) {
			continue
		}
		digest := sha256.Sum256([]byte(service + "\x00" + key + "\x00" + member.ID))
		score := binary.BigEndian.Uint64(digest[:8])
		if !found || score > selectedScore || score == selectedScore && member.ID < selected.ID {
			selected = member
			selectedScore = score
			found = true
		}
	}
	return selected, found
}

func registryMemberHasService(member registry.Member, service string) bool {
	for _, candidate := range member.Services {
		if candidate == service {
			return true
		}
	}
	return false
}

func (h *LocalHandler) register(comp component.Component, opts []component.Option) error {
	s := component.NewService(comp, opts)

	if _, ok := h.localServices[s.Name]; ok {
		return fmt.Errorf("handler: service already defined: %s", s.Name)
	}

	if err := s.ExtractHandler(); err != nil {
		return err
	}

	// register all localHandlers
	h.localServices[s.Name] = s
	for name, handler := range s.Handlers {
		n := fmt.Sprintf("%s.%s", s.Name, name)
		log.Println("Register local handler", n)
		h.localHandlers[n] = handler
	}
	return nil
}

func (h *LocalHandler) initRemoteService(members []*clusterpb.MemberInfo) {
	for _, m := range members {
		h.addRemoteService(m)
	}
}

func (h *LocalHandler) addRemoteService(member *clusterpb.MemberInfo) {
	h.mu.Lock()
	defer h.mu.Unlock()

	h.delMemberLocked(member.ServiceAddr)
	for _, s := range member.Services {
		log.Println("Register remote service", s)
		h.remoteServices[s] = append(h.remoteServices[s], member)
	}
}

func (h *LocalHandler) delMember(addr string) {
	h.mu.Lock()
	h.delMemberLocked(addr)
	h.mu.Unlock()

	h.currentNode.clearSessionRoutesForAddress(addr)
}

func (h *LocalHandler) delMemberLocked(addr string) {
	for name, members := range h.remoteServices {
		filtered := members[:0]
		for _, member := range members {
			if addr != member.ServiceAddr {
				filtered = append(filtered, member)
			}
		}
		if len(filtered) == 0 {
			delete(h.remoteServices, name)
		} else {
			h.remoteServices[name] = filtered
		}
	}
}

func (h *LocalHandler) syncRemoteMembers(members []*clusterpb.MemberInfo) {
	next := make(map[string][]*clusterpb.MemberInfo)
	nextAddrs := make(map[string]struct{})
	for _, member := range members {
		if member == nil || member.ServiceAddr == "" {
			continue
		}
		nextAddrs[member.ServiceAddr] = struct{}{}
		for _, service := range member.Services {
			if service == "" {
				continue
			}
			next[service] = append(next[service], member)
		}
	}

	h.mu.Lock()
	removed := make([]string, 0)
	oldAddrs := make(map[string]struct{})
	for _, serviceMembers := range h.remoteServices {
		for _, member := range serviceMembers {
			oldAddrs[member.ServiceAddr] = struct{}{}
		}
	}
	for addr := range oldAddrs {
		if _, ok := nextAddrs[addr]; !ok {
			removed = append(removed, addr)
		}
	}
	h.remoteServices = next
	h.mu.Unlock()

	for _, addr := range removed {
		h.currentNode.clearSessionRoutesForAddress(addr)
	}
}

func (h *LocalHandler) LocalService() []string {
	var result []string
	for service := range h.localServices {
		result = append(result, service)
	}
	sort.Strings(result)
	return result
}

func (h *LocalHandler) RemoteService() []string {
	h.mu.RLock()
	defer h.mu.RUnlock()

	var result []string
	for service := range h.remoteServices {
		result = append(result, service)
	}
	sort.Strings(result)
	return result
}

func (h *LocalHandler) handle(conn net.Conn) {
	// create a client agent and startup write gorontine
	agent := newAgent(conn, h.pipeline, h.remoteProcess, h.currentNode.writeTimeout())
	h.currentNode.storeSession(agent.session)
	service.Connections.Increment()

	// startup write goroutine
	go agent.write()

	if env.Debug {
		log.Println(fmt.Sprintf("New session established: %s", agent.String()))
	}

	// guarantee agent related resource be destroyed
	defer func() {
		if h.currentNode.SessionClosedCallback != nil {
			h.currentNode.SessionClosedCallback(agent.session)
		}
		h.currentNode.removeSession(agent.session.ID())
		agent.session.Release()
		service.Connections.Decrement()
		request := &clusterpb.SessionClosedRequest{
			SessionId: agent.session.ID(),
		}

		members := h.currentNode.cluster.remoteAddrs()
		for _, remote := range members {
			log.Println("Notify remote server", remote)
			pool, err := h.currentNode.rpcClient.getConnPool(remote)
			if err != nil {
				log.Println("Cannot retrieve connection pool for address", remote, err)
				continue
			}
			client := clusterpb.NewMemberClient(pool.Get())
			ctx, cancel := h.currentNode.rpcContext(context.Background())
			_, err = client.SessionClosed(ctx, request)
			cancel()
			if err != nil {
				log.Println("Cannot closed session in remote address", remote, err)
				continue
			}
			if env.Debug {
				log.Println("Notify remote server success", remote)
			}
		}

		agent.Close()
		if env.Debug {
			log.Println(fmt.Sprintf("Session read goroutine exit, SessionID=%d, UID=%d", agent.session.ID(), agent.session.UID()))
		}
	}()

	// read loop
	buf := make([]byte, 2048)
	for {
		n, err := conn.Read(buf)
		if err != nil {
			log.Println(fmt.Sprintf("Read message error: %s, session will be closed immediately", err.Error()))
			return
		}

		// TODO(warning): decoder use slice for performance, packet data should be copy before next Decode
		packets, err := agent.decoder.Decode(buf[:n])
		if err != nil {
			log.Println(err.Error())

			// process packets decoded
			for _, p := range packets {
				if err := h.processPacket(agent, p); err != nil {
					log.Println(err.Error())
					return
				}
			}
			return
		}

		// process all packets
		for _, p := range packets {
			if err := h.processPacket(agent, p); err != nil {
				log.Println(err.Error())
				return
			}
		}
	}
}

func (h *LocalHandler) processPacket(agent *agent, p *packet.Packet) error {
	switch p.Type {
	case packet.Handshake:
		if err := env.HandshakeValidator(p.Data); err != nil {
			return err
		}

		if _, err := agent.conn.Write(hrd); err != nil {
			return err
		}

		agent.setStatus(statusHandshake)
		if env.Debug {
			log.Println(fmt.Sprintf("Session handshake Id=%d, Remote=%s", agent.session.ID(), agent.conn.RemoteAddr()))
		}

	case packet.HandshakeAck:
		agent.setStatus(statusWorking)
		if env.Debug {
			log.Println(fmt.Sprintf("Receive handshake ACK Id=%d, Remote=%s", agent.session.ID(), agent.conn.RemoteAddr()))
		}

	case packet.Data:
		if agent.status() < statusWorking {
			return fmt.Errorf("receive data on socket which not yet ACK, session will be closed immediately, remote=%s",
				agent.conn.RemoteAddr().String())
		}

		msg, err := message.Decode(p.Data)
		if err != nil {
			return err
		}
		h.processMessage(agent, msg)

	case packet.Heartbeat:
		// expected
	}

	agent.lastAt = time.Now().Unix()
	return nil
}

func (h *LocalHandler) findMembers(service string) []*clusterpb.MemberInfo {
	h.mu.RLock()
	defer h.mu.RUnlock()
	members := h.remoteServices[service]
	if len(members) == 0 {
		return nil
	}
	result := make([]*clusterpb.MemberInfo, len(members))
	copy(result, members)
	return result
}

func (h *LocalHandler) remoteProcess(session *session.Session, msg *message.Message, noCopy bool) {
	index := strings.LastIndex(msg.Route, ".")
	if index < 0 {
		log.Println(fmt.Sprintf("nano/handler: invalid route %s", msg.Route))
		if msg.Type == message.Request {
			session.NetworkEntity().SendResponse(msg.ID, errcode.CodeBadRequest, nil)
		}
		return
	}

	service := msg.Route[:index]
	members := h.findMembers(service)
	if len(members) == 0 {
		if msg.Type == message.Request {
			session.NetworkEntity().SendResponse(msg.ID, errcode.CodeServiceNotFound, nil)
		}
		log.Println(fmt.Sprintf("nano/handler: %s not found(forgot registered?)", msg.Route))
		return
	}

	// Select a remote service address
	// 1. if exist customer remote service route ,use it, otherwise use default strategy
	// 2. Use the service address directly if the router contains binding item
	// 3. Select a remote service address randomly and bind to router
	var remoteAddr string
	if h.currentNode.Options.RemoteServiceRoute != nil {
		if addr, found := session.Router().Find(service); found {
			remoteAddr = addr
		} else {
			member := h.currentNode.Options.RemoteServiceRoute(service, session, members)
			if member == nil {
				log.Println(fmt.Sprintf("customize remoteServiceRoute handler: %s is not found", msg.Route))
				if msg.Type == message.Request {
					session.NetworkEntity().SendResponse(msg.ID, errcode.CodeServiceUnavailable, nil)
				}
				return
			}
			remoteAddr = member.ServiceAddr
			session.Router().Bind(service, remoteAddr)
		}
	} else {
		if addr, found := session.Router().Find(service); found {
			remoteAddr = addr
		} else {
			remoteAddr = members[rand.Intn(len(members))].ServiceAddr
			session.Router().Bind(service, remoteAddr)
		}
	}
	pool, err := h.currentNode.rpcClient.getConnPool(remoteAddr)
	if err != nil {
		log.Println(err)
		if msg.Type == message.Request {
			session.NetworkEntity().SendResponse(msg.ID, errcode.CodeNetworkException, nil)
		}
		return
	}
	var data = msg.Data
	if !noCopy && len(msg.Data) > 0 {
		data = make([]byte, len(msg.Data))
		copy(data, msg.Data)
	}

	gateAddr, sessionId := h.gatewayRoute(session)

	client := clusterpb.NewMemberClient(pool.Get())
	ctx, cancel := h.currentNode.rpcContext(context.Background(), "uid", strconv.FormatInt(session.UID(), 10))
	defer cancel()
	switch msg.Type {
	case message.Request:
		request := &clusterpb.RequestMessage{
			GateAddr:  gateAddr,
			SessionId: sessionId,
			Id:        msg.ID,
			Route:     msg.Route,
			Data:      data,
		}
		_, err = client.HandleRequest(ctx, request)
	case message.Notify:
		request := &clusterpb.NotifyMessage{
			GateAddr:  gateAddr,
			SessionId: sessionId,
			Route:     msg.Route,
			Data:      data,
		}
		_, err = client.HandleNotify(ctx, request)
	}
	if err != nil {
		if msg.Type == message.Request {
			session.NetworkEntity().SendResponse(msg.ID, errcode.CodeNetworkException, nil)
		}
		log.Println(fmt.Sprintf("Process remote message (%d:%s) error: %+v", msg.ID, msg.Route, err))
	}
}

func (h *LocalHandler) gatewayRoute(s *session.Session) (string, int64) {
	gateAddr := h.currentNode.memberAddr()
	sessionId := s.ID()
	switch v := s.NetworkEntity().(type) {
	case *acceptor:
		gateAddr = v.gateAddr
		sessionId = v.sid
	}
	return gateAddr, sessionId
}

func (h *LocalHandler) processMessage(agent *agent, msg *message.Message) {
	var mid uint64
	switch msg.Type {
	case message.Request:
		mid = msg.ID
	case message.Notify:
		mid = 0
	default:
		log.Println("Invalid message type: " + msg.Type.String())
		return
	}

	handler, found := h.localHandlers[msg.Route]
	if !found {
		h.remoteProcess(agent.session, msg, false)
	} else {
		h.localProcess(context.Background(), handler, mid, agent.session, msg)
	}
}

func (h *LocalHandler) handleWS(conn *websocket.Conn) {
	c, err := newWSConn(conn)
	if err != nil {
		log.Println(err)
		return
	}
	go h.handle(c)
}

func (h *LocalHandler) localProcess(parent context.Context, handler *component.Handler, mid uint64, session *session.Session, msg *message.Message) {
	if pipe := h.pipeline; pipe != nil {
		err := pipe.Inbound().Process(session, msg)
		if err != nil {
			log.Println("Pipeline process failed: " + err.Error())
			if msg.Type == message.Request {
				session.NetworkEntity().SendResponse(mid, errcode.CodeProtoParseFail, nil)
			}
			return
		}
	}

	index := strings.LastIndex(msg.Route, ".")
	if index < 0 {
		log.Println(fmt.Sprintf("nano/handler: invalid route %s", msg.Route))
		if msg.Type == message.Request {
			session.NetworkEntity().SendResponse(mid, errcode.CodeBadRequest, nil)
		}
		return
	}
	var payload = msg.Data
	var data interface{}
	if handler.IsRawArg {
		data = payload
	} else {
		data = reflect.New(handler.Type.Elem()).Interface()
		err := env.Serializer.Unmarshal(payload, data)
		if err != nil {
			if msg.Type == message.Request {
				session.NetworkEntity().SendResponse(mid, errcode.CodeProtoParseFail, nil)
			}
			log.Println(fmt.Sprintf("Deserialize to %T failed: %+v (%v)", data, err, payload))
			return
		}
	}

	if env.Debug {
		log.Println(fmt.Sprintf("UID=%d, Message={%s}, Data=%+v", session.UID(), msg.String(), data))
	}

	requestContext, cancel := h.newRequestContext(parent, session, mid)
	stopTimeoutResponse := context.AfterFunc(requestContext, func() {
		if errors.Is(requestContext.Err(), context.DeadlineExceeded) {
			requestContext.RespondSystemError(errcode.CodeRequestTimeout)
		}
	})
	args := []reflect.Value{handler.Receiver, reflect.ValueOf(requestContext), reflect.ValueOf(data)}
	task := func() {
		defer cancel()
		defer stopTimeoutResponse()
		invokeHandler(handler, args, requestContext, msg)
	}

	// A message can be dispatch to global thread or a user customized thread
	service := msg.Route[:index]
	if s, found := h.localServices[service]; found && s.SchedName != "" {
		sched := session.Value(s.SchedName)
		log.Println(fmt.Sprintf("nano/handler: SchedName %s", s.SchedName))
		if sched == nil {
			log.Println(fmt.Sprintf("nanl/handler: cannot found `schedular.LocalScheduler` by %s", s.SchedName))
			stopTimeoutResponse()
			cancel()
			if msg.Type == message.Request {
				session.NetworkEntity().SendResponse(mid, errcode.CodeQueueNotFund, nil)
			}
			return
		}

		local, ok := sched.(scheduler.LocalScheduler)
		if !ok {
			log.Println(fmt.Sprintf("nanl/handler: Type %T does not implement the `schedular.LocalScheduler` interface",
				sched))
			stopTimeoutResponse()
			cancel()
			if msg.Type == message.Request {
				session.NetworkEntity().SendResponse(mid, errcode.CodeQueueNotFund, nil)
			}
			return
		}
		local.Schedule(task)
	} else {
		if !scheduler.TryPushTask(task) {
			stopTimeoutResponse()
			cancel()
			if msg.Type == message.Request {
				session.NetworkEntity().SendResponse(mid, errcode.CodeServerBusy, nil)
			}
			// For Notify, drop on overload to avoid blocking caller.
		}
	}
}

func invokeHandler(handler *component.Handler, args []reflect.Value, requestContext *session.RequestContext, msg *message.Message) {
	var handlerErr error
	defer func() {
		if recovered := recover(); recovered != nil {
			log.Println(fmt.Sprintf("Service %s panic: %+v\n%s", msg.Route, recovered, debug.Stack()))
		}
		if msg.Type == message.Request && !requestContext.Responded() {
			code := errcode.CodeInternalErr
			if errors.Is(handlerErr, session.ErrUIDMismatch) {
				code = errcode.CodePermissionDenied
			}
			requestContext.RespondSystemError(code)
			log.Println(fmt.Sprintf("Service %s returned without a response", msg.Route))
		}
	}()

	result := handler.Method.Func.Call(args)
	if len(result) > 0 && !result[0].IsNil() {
		handlerErr, _ = result[0].Interface().(error)
		log.Println(fmt.Sprintf("Service %s error: %+v", msg.Route, handlerErr))
	}
}
