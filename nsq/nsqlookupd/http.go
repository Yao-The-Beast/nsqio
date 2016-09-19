package nsqlookupd

import (
	"fmt"
	"net/http"
	"net/http/pprof"
	"sync/atomic"

	"github.com/julienschmidt/httprouter"
	"github.com/nsqio/nsq/internal/http_api"
	"github.com/nsqio/nsq/internal/protocol"
	"github.com/nsqio/nsq/internal/version"
)

type httpServer struct {
	ctx    *Context
	router http.Handler
}

func newHTTPServer(ctx *Context) *httpServer {
	log := http_api.Log(ctx.nsqlookupd.opts.Logger)

	router := httprouter.New()
	router.HandleMethodNotAllowed = true
	router.PanicHandler = http_api.LogPanicHandler(ctx.nsqlookupd.opts.Logger)
	router.NotFound = http_api.LogNotFoundHandler(ctx.nsqlookupd.opts.Logger)
	router.MethodNotAllowed = http_api.LogMethodNotAllowedHandler(ctx.nsqlookupd.opts.Logger)
	s := &httpServer{
		ctx:    ctx,
		router: router,
	}

	router.Handle("GET", "/ping", http_api.Decorate(s.pingHandler, log, http_api.PlainText))

	// v1 negotiate
	router.Handle("GET", "/debug", http_api.Decorate(s.doDebug, log, http_api.NegotiateVersion))
	router.Handle("GET", "/lookup", http_api.Decorate(s.doLookup, log, http_api.NegotiateVersion))
	router.Handle("GET", "/topics", http_api.Decorate(s.doTopics, log, http_api.NegotiateVersion))
	router.Handle("GET", "/channels", http_api.Decorate(s.doChannels, log, http_api.NegotiateVersion))
	router.Handle("GET", "/nodes", http_api.Decorate(s.doNodes, log, http_api.NegotiateVersion))
	//yao
	// /priority_nodes priority
	router.Handle("GET", "/priority_nodes", http_api.Decorate(s.doPriorityNodes,log,http_api.NegotiateVersion))
	// /register_topic_priority
	router.Handle("GET", "/register_topic_priority", http_api.Decorate(s.doRegisterTopicPriority,log,http_api.NegotiateVersion))
	// /unregister_topic_priority
	router.Handle("GET", "/unregister_topic_priority", http_api.Decorate(s.doUnregisterTopicPriority,log,http_api.NegotiateVersion))	
	//lookup for the producers, based on topic
	router.Handle("GET", "/producer_lookup", http_api.Decorate(s.doProducerLookup,log,http_api.NegotiateVersion))	
	//lookup for the producers v2, based on specified priority
	router.Handle("GET", "/producer_lookup_v2", http_api.Decorate(s.doProducerLookupV2,log,http_api.NegotiateVersion))	

	// only v1
	router.Handle("POST", "/topic/create", http_api.Decorate(s.doCreateTopic, log, http_api.V1))
	router.Handle("POST", "/topic/delete", http_api.Decorate(s.doDeleteTopic, log, http_api.V1))
	router.Handle("POST", "/channel/create", http_api.Decorate(s.doCreateChannel, log, http_api.V1))
	router.Handle("POST", "/channel/delete", http_api.Decorate(s.doDeleteChannel, log, http_api.V1))
	router.Handle("POST", "/topic/tombstone", http_api.Decorate(s.doTombstoneTopicProducer, log, http_api.V1))

	// deprecated, v1 negotiate
	router.Handle("GET", "/info", http_api.Decorate(s.doInfo, log, http_api.NegotiateVersion))
	router.Handle("POST", "/create_topic", http_api.Decorate(s.doCreateTopic, log, http_api.NegotiateVersion))
	router.Handle("POST", "/delete_topic", http_api.Decorate(s.doDeleteTopic, log, http_api.NegotiateVersion))
	router.Handle("POST", "/create_channel", http_api.Decorate(s.doCreateChannel, log, http_api.NegotiateVersion))
	router.Handle("POST", "/delete_channel", http_api.Decorate(s.doDeleteChannel, log, http_api.NegotiateVersion))
	router.Handle("POST", "/tombstone_topic_producer", http_api.Decorate(s.doTombstoneTopicProducer, log, http_api.NegotiateVersion))
	router.Handle("GET", "/create_topic", http_api.Decorate(s.doCreateTopic, log, http_api.NegotiateVersion))
	router.Handle("GET", "/delete_topic", http_api.Decorate(s.doDeleteTopic, log, http_api.NegotiateVersion))
	router.Handle("GET", "/create_channel", http_api.Decorate(s.doCreateChannel, log, http_api.NegotiateVersion))
	router.Handle("GET", "/delete_channel", http_api.Decorate(s.doDeleteChannel, log, http_api.NegotiateVersion))
	router.Handle("GET", "/tombstone_topic_producer", http_api.Decorate(s.doTombstoneTopicProducer, log, http_api.NegotiateVersion))

	// debug
	router.HandlerFunc("GET", "/debug/pprof", pprof.Index)
	router.HandlerFunc("GET", "/debug/pprof/cmdline", pprof.Cmdline)
	router.HandlerFunc("GET", "/debug/pprof/symbol", pprof.Symbol)
	router.HandlerFunc("POST", "/debug/pprof/symbol", pprof.Symbol)
	router.HandlerFunc("GET", "/debug/pprof/profile", pprof.Profile)
	router.Handler("GET", "/debug/pprof/heap", pprof.Handler("heap"))
	router.Handler("GET", "/debug/pprof/goroutine", pprof.Handler("goroutine"))
	router.Handler("GET", "/debug/pprof/block", pprof.Handler("block"))
	router.Handler("GET", "/debug/pprof/threadcreate", pprof.Handler("threadcreate"))

	return s
}

func (s *httpServer) ServeHTTP(w http.ResponseWriter, req *http.Request) {
	s.router.ServeHTTP(w, req)
}

func (s *httpServer) pingHandler(w http.ResponseWriter, req *http.Request, ps httprouter.Params) (interface{}, error) {
	return "OK", nil
}

func (s *httpServer) doInfo(w http.ResponseWriter, req *http.Request, ps httprouter.Params) (interface{}, error) {
	return struct {
		Version string `json:"version"`
	}{
		Version: version.Binary,
	}, nil
}

func (s *httpServer) doTopics(w http.ResponseWriter, req *http.Request, ps httprouter.Params) (interface{}, error) {
	topics := s.ctx.nsqlookupd.DB.FindRegistrations("topic", "*", "").Keys()
	return map[string]interface{}{
		"topics": topics,
	}, nil
}

func (s *httpServer) doChannels(w http.ResponseWriter, req *http.Request, ps httprouter.Params) (interface{}, error) {
	reqParams, err := http_api.NewReqParams(req)
	if err != nil {
		return nil, http_api.Err{400, "INVALID_REQUEST"}
	}

	topicName, err := reqParams.Get("topic")
	if err != nil {
		return nil, http_api.Err{400, "MISSING_ARG_TOPIC"}
	}

	channels := s.ctx.nsqlookupd.DB.FindRegistrations("channel", topicName, "*").SubKeys()
	return map[string]interface{}{
		"channels": channels,
	}, nil
}


func (s *httpServer) doLookup(w http.ResponseWriter, req *http.Request, ps httprouter.Params) (interface{}, error) {
	reqParams, err := http_api.NewReqParams(req)
	if err != nil {
		return nil, http_api.Err{400, "INVALID_REQUEST"}
	}

	topicName, err := reqParams.Get("topic")
	if err != nil {
		return nil, http_api.Err{400, "MISSING_ARG_TOPIC"}
	}

	registration := s.ctx.nsqlookupd.DB.FindRegistrations("topic", topicName, "")
	if len(registration) == 0 {
		return nil, http_api.Err{404, "TOPIC_NOT_FOUND"}
	}

	channels := s.ctx.nsqlookupd.DB.FindRegistrations("channel", topicName, "*").SubKeys()
	producers := s.ctx.nsqlookupd.DB.FindProducers("topic", topicName, "")
	producers = producers.FilterByActive(s.ctx.nsqlookupd.opts.InactiveProducerTimeout,
		s.ctx.nsqlookupd.opts.TombstoneLifetime)
	return map[string]interface{}{
		"channels":  channels,
		"producers": producers.PeerInfo(),
	}, nil
}



func (s *httpServer) doProducerLookup(w http.ResponseWriter, req *http.Request, ps httprouter.Params) (interface{}, error) {
	reqParams, err := http_api.NewReqParams(req)
	if err != nil {
		return nil, http_api.Err{400, "INVALID_REQUEST"}
	}

	topicName, err := reqParams.Get("topic")
	if err != nil {
		return nil, http_api.Err{400, "MISSING_ARG_TOPIC"}
	}

	//check if the we need to return the address of another blank nsqd incase the producer and the active nsqd is not on the same host
	okFlag := false
	senderHostname, err := reqParams.Get("hostname")
	if err != nil {
		return nil, http_api.Err{400, "MISSING_ARG_TOPIC"}
	}
	tempProducers := s.ctx.nsqlookupd.DB.FindProducers("topic", topicName, "")
	tempProducers = tempProducers.FilterByActive(s.ctx.nsqlookupd.opts.InactiveProducerTimeout,
		s.ctx.nsqlookupd.opts.TombstoneLifetime)
	for _, thisProducer := range tempProducers {
		if thisProducer.peerInfo.Hostname == senderHostname {
			okFlag = true
			break
		}
	}

	registration := s.ctx.nsqlookupd.DB.FindRegistrations("topic", topicName, "")
	if len(registration) == 0 || okFlag == false {
		//return nil, http_api.Err{404, "TOPIC_NOT_FOUND"}
		//yao
		//now we do a lookup in the registration table to find if there is a preset topic
		//the preset topic in most time should be a high prioirty topic
		//in that case we should return a NSQd which has a high priority
		//if htere is no such high priority daemon, return error
		
		//check if the topic is in the high priority list
		registration = s.ctx.nsqlookupd.DB.FindRegistrations("high_priority_topic", topicName, "")
		if len(registration) == 0 {
			return nil, http_api.Err{404, "TOPIC_NOT_FOUND"}
		}else if len(registration) > 1 {
			println("YAO WARNING: DUPLICATE NAME OF HIGH PRIOIRTY TOPIC")
		}
		//if the topic is among the high prioirty list
		//get a daemon with the same prioirty of  the topic
		producers := s.ctx.nsqlookupd.DB.FindProducers("client", "", "").FilterByActive(s.ctx.nsqlookupd.opts.InactiveProducerTimeout, 0)
		var tempPeerInfo []*PeerInfo
		for _, p := range producers {
			priority := p.peerInfo.DaemonPriority
			if priority == "HIGH" {
				tempPeerInfo = append(tempPeerInfo,p.peerInfo)
			}
		}
		var channels []string
		if len(tempPeerInfo) > 0 {
				//s.ctx.nsqlookupd.logf("TOPIC: %s is handled by Daemon %d, Length is: %d", topicName, tempPeerInfo[0].TCPPort, len(tempPeerInfo))
				return map[string]interface{}{
					"channels":  channels,
					"producers": tempPeerInfo,
				}, nil
		}
		//if we dont have any high priority dameons, return nil
		return nil, http_api.Err{404, "NO HIGH PRIORITY NSQds AVAILABLE"}
	}
	//we do find a NSQd serving the topic
	//return that nsqd's info
	channels := s.ctx.nsqlookupd.DB.FindRegistrations("channel", topicName, "*").SubKeys()
	producers := s.ctx.nsqlookupd.DB.FindProducers("topic", topicName, "")
	producers = producers.FilterByActive(s.ctx.nsqlookupd.opts.InactiveProducerTimeout,
		s.ctx.nsqlookupd.opts.TombstoneLifetime)
	
	//even if the topic is registered in the lookupd, it might be the case that no lookupds are currently holding it
	if len(producers) == 0 {
		return nil, http_api.Err{404, "NO NSQd IS HOLDING THE TOPIC"}
	}
	return map[string]interface{}{
		"channels":  channels,
		"producers": producers.PeerInfo(),
	}, nil
}

func (s *httpServer) doCreateTopic(w http.ResponseWriter, req *http.Request, ps httprouter.Params) (interface{}, error) {
	reqParams, err := http_api.NewReqParams(req)
	if err != nil {
		return nil, http_api.Err{400, "INVALID_REQUEST"}
	}

	topicName, err := reqParams.Get("topic")
	if err != nil {
		return nil, http_api.Err{400, "MISSING_ARG_TOPIC"}
	}

	if !protocol.IsValidTopicName(topicName) {
		return nil, http_api.Err{400, "INVALID_ARG_TOPIC"}
	}

	s.ctx.nsqlookupd.logf("DB: adding topic(%s)", topicName)
	key := Registration{"topic", topicName, ""}
	s.ctx.nsqlookupd.DB.AddRegistration(key)

	return nil, nil
}

func (s *httpServer) doDeleteTopic(w http.ResponseWriter, req *http.Request, ps httprouter.Params) (interface{}, error) {
	reqParams, err := http_api.NewReqParams(req)
	if err != nil {
		return nil, http_api.Err{400, "INVALID_REQUEST"}
	}

	topicName, err := reqParams.Get("topic")
	if err != nil {
		return nil, http_api.Err{400, "MISSING_ARG_TOPIC"}
	}

	registrations := s.ctx.nsqlookupd.DB.FindRegistrations("channel", topicName, "*")
	for _, registration := range registrations {
		s.ctx.nsqlookupd.logf("DB: removing channel(%s) from topic(%s)", registration.SubKey, topicName)
		s.ctx.nsqlookupd.DB.RemoveRegistration(registration)
	}

	registrations = s.ctx.nsqlookupd.DB.FindRegistrations("topic", topicName, "")
	for _, registration := range registrations {
		s.ctx.nsqlookupd.logf("DB: removing topic(%s)", topicName)
		s.ctx.nsqlookupd.DB.RemoveRegistration(registration)
	}

	return nil, nil
}

func (s *httpServer) doTombstoneTopicProducer(w http.ResponseWriter, req *http.Request, ps httprouter.Params) (interface{}, error) {
	reqParams, err := http_api.NewReqParams(req)
	if err != nil {
		return nil, http_api.Err{400, "INVALID_REQUEST"}
	}

	topicName, err := reqParams.Get("topic")
	if err != nil {
		return nil, http_api.Err{400, "MISSING_ARG_TOPIC"}
	}

	node, err := reqParams.Get("node")
	if err != nil {
		return nil, http_api.Err{400, "MISSING_ARG_NODE"}
	}

	s.ctx.nsqlookupd.logf("DB: setting tombstone for producer@%s of topic(%s)", node, topicName)
	producers := s.ctx.nsqlookupd.DB.FindProducers("topic", topicName, "")
	for _, p := range producers {
		thisNode := fmt.Sprintf("%s:%d", p.peerInfo.BroadcastAddress, p.peerInfo.HTTPPort)
		if thisNode == node {
			p.Tombstone()
		}
	}

	return nil, nil
}

func (s *httpServer) doCreateChannel(w http.ResponseWriter, req *http.Request, ps httprouter.Params) (interface{}, error) {
	reqParams, err := http_api.NewReqParams(req)
	if err != nil {
		return nil, http_api.Err{400, "INVALID_REQUEST"}
	}

	topicName, channelName, err := http_api.GetTopicChannelArgs(reqParams)
	if err != nil {
		return nil, http_api.Err{400, err.Error()}
	}

	s.ctx.nsqlookupd.logf("DB: adding channel(%s) in topic(%s)", channelName, topicName)
	key := Registration{"channel", topicName, channelName}
	s.ctx.nsqlookupd.DB.AddRegistration(key)

	s.ctx.nsqlookupd.logf("DB: adding topic(%s)", topicName)
	key = Registration{"topic", topicName, ""}
	s.ctx.nsqlookupd.DB.AddRegistration(key)

	return nil, nil
}

func (s *httpServer) doDeleteChannel(w http.ResponseWriter, req *http.Request, ps httprouter.Params) (interface{}, error) {
	reqParams, err := http_api.NewReqParams(req)
	if err != nil {
		return nil, http_api.Err{400, "INVALID_REQUEST"}
	}

	topicName, channelName, err := http_api.GetTopicChannelArgs(reqParams)
	if err != nil {
		return nil, http_api.Err{400, err.Error()}
	}

	registrations := s.ctx.nsqlookupd.DB.FindRegistrations("channel", topicName, channelName)
	if len(registrations) == 0 {
		return nil, http_api.Err{404, "CHANNEL_NOT_FOUND"}
	}

	s.ctx.nsqlookupd.logf("DB: removing channel(%s) from topic(%s)", channelName, topicName)
	for _, registration := range registrations {
		s.ctx.nsqlookupd.DB.RemoveRegistration(registration)
	}

	return nil, nil
}

type node struct {
	RemoteAddress    string   `json:"remote_address"`
	Hostname         string   `json:"hostname"`
	BroadcastAddress string   `json:"broadcast_address"`
	TCPPort          int      `json:"tcp_port"`
	HTTPPort         int      `json:"http_port"`
	Version          string   `json:"version"`
	Tombstones       []bool   `json:"tombstones"`
	Topics           []string `json:"topics"`
}

func (s *httpServer) doNodes(w http.ResponseWriter, req *http.Request, ps httprouter.Params) (interface{}, error) {
	
	// dont filter out tombstoned nodes
	producers := s.ctx.nsqlookupd.DB.FindProducers("client", "", "").FilterByActive(
		s.ctx.nsqlookupd.opts.InactiveProducerTimeout, 0)
	//nodes := make([]*node, len(producers))
	var nodes []*node

	numHighPrioNSQds := 0
	tolerantFlag := false
	for _, q := range producers {
		if q.peerInfo.DaemonPriority == "HIGH" {
			numHighPrioNSQds++
		}
	}
	//if all nsqds are high prio, we tolerant low prioirty topics to be served by the nsqds
	if numHighPrioNSQds == len(producers) {
		tolerantFlag = true
	}

	for _, p := range producers {
		//yao
		//we are not going to allow low processes to connect ot high prio daemons
		if !tolerantFlag && p.peerInfo.DaemonPriority == "HIGH"{
			continue
		}

		topics := s.ctx.nsqlookupd.DB.LookupRegistrations(p.peerInfo.id).Filter("topic", "*", "").Keys()

		// for each topic find the producer that matches this peer
		// to add tombstone information
		tombstones := make([]bool, len(topics))
		for j, t := range topics {
			topicProducers := s.ctx.nsqlookupd.DB.FindProducers("topic", t, "")
			for _, tp := range topicProducers {
				if tp.peerInfo == p.peerInfo {
					tombstones[j] = tp.IsTombstoned(s.ctx.nsqlookupd.opts.TombstoneLifetime)
				}
			}
		}

		nodes = append(nodes, &node{
			RemoteAddress:    p.peerInfo.RemoteAddress,
			Hostname:         p.peerInfo.Hostname,
			BroadcastAddress: p.peerInfo.BroadcastAddress,
			TCPPort:          p.peerInfo.TCPPort,
			HTTPPort:         p.peerInfo.HTTPPort,
			Version:          p.peerInfo.Version,
			Tombstones:       tombstones,
			Topics:           topics,
		})
	}



	return map[string]interface{}{
		"producers": nodes,
	}, nil
}

func (s *httpServer) doDebug(w http.ResponseWriter, req *http.Request, ps httprouter.Params) (interface{}, error) {
	s.ctx.nsqlookupd.DB.RLock()
	defer s.ctx.nsqlookupd.DB.RUnlock()

	data := make(map[string][]map[string]interface{})
	for r, producers := range s.ctx.nsqlookupd.DB.registrationMap {
		key := r.Category + ":" + r.Key + ":" + r.SubKey
		for _, p := range producers {
			m := map[string]interface{}{
				"id":                p.peerInfo.id,
				"hostname":          p.peerInfo.Hostname,
				"broadcast_address": p.peerInfo.BroadcastAddress,
				"tcp_port":          p.peerInfo.TCPPort,
				"http_port":         p.peerInfo.HTTPPort,
				"version":           p.peerInfo.Version,
				"last_update":       atomic.LoadInt64(&p.peerInfo.lastUpdate),
				"tombstoned":        p.tombstoned,
				"tombstoned_at":     p.tombstonedAt.UnixNano(),
			}
			data[key] = append(data[key], m)
		}
	}

	return data, nil
}

//yao
//register topic priority
//register_topic_priority topicName 
func (s *httpServer) doRegisterTopicPriority(w http.ResponseWriter, req *http.Request, ps httprouter.Params) (interface{}, error) {
	
	
	reqParams, err := http_api.NewReqParams(req)
	if err != nil {
		return nil, http_api.Err{400, "INVALID_REQUEST"}
	}

	topicName, err := http_api.GetTopicPriorityArgs(reqParams)
	if err != nil {
		return nil, http_api.Err{400, err.Error()}
	}

	s.ctx.nsqlookupd.logf("DB: adding topic (%s) to high priority", topicName)

	//if there has already had such topic in the high prio list, return
	if len(s.ctx.nsqlookupd.DB.FindRegistrations("high_priority_topic",topicName,"")) > 0 {
			return map[string]interface{}{
			"status": "success",
		}, nil
	}
	//else add it
	key := Registration{"high_priority_topic",topicName,""}
	s.ctx.nsqlookupd.DB.AddRegistration(key)
	
	//we need to check if the topic is currently served by any other daemons
	//if there is a daemon and the daemon is low priority, we need to modify this registration so that next time
	//the topic can be served by high priority nsqds
	//set the #inactive flag to make the currently active producer sleep
	producers := s.ctx.nsqlookupd.DB.FindProducers("topic", topicName, "")
	producers = producers.FilterByActive(s.ctx.nsqlookupd.opts.InactiveProducerTimeout, s.ctx.nsqlookupd.opts.TombstoneLifetime)
	for _, p := range producers {
		if p.peerInfo.DaemonPriority == "LOW" {
			previousReg := Registration{"topic",topicName,""}
			currentReg := Registration{"topic",topicName,"#inactive"}
			//s.ctx.nsqlookupd.DB.RemoveRegistration(previousReg)
			s.ctx.nsqlookupd.DB.RemoveProducer(previousReg, p.peerInfo.id)
			s.ctx.nsqlookupd.DB.AddProducer(currentReg, p)
		}
	}
	//find an inactive correpsonding high priority nsqd
	//switch its flag and set to active
	producers = s.ctx.nsqlookupd.DB.FindProducers("topic", topicName, "#inactive")
	producers = producers.FilterByActive(s.ctx.nsqlookupd.opts.InactiveProducerTimeout, s.ctx.nsqlookupd.opts.TombstoneLifetime)
	if len(producers) == 0 || len(producers) == len(findNSQds(producers,"LOW")) {
		//there is no high prio nsqds having experience serving this topic
		//just delete the registration, as the new nsqd will register it 
		reg := Registration{"topic", topicName, ""}
		s.ctx.nsqlookupd.DB.RemoveRegistration(reg)
	} else {
		for _, p := range producers {
			if p.peerInfo.DaemonPriority == "HIGH" {
				previousReg := Registration{"topic",topicName,"#inactive"}
				currentReg := Registration{"topic",topicName,""}
				s.ctx.nsqlookupd.DB.RemoveProducer(previousReg, p.peerInfo.id)
				s.ctx.nsqlookupd.DB.AddProducer(currentReg, p)
			}
		}
	}
	return map[string]interface{}{
		"status": "success",
	}, nil
}

//yao
//unregister topic priority
//unregister_topic_priority topicName 
func (s *httpServer) doUnregisterTopicPriority(w http.ResponseWriter, req *http.Request, ps httprouter.Params) (interface{}, error) {
		
	reqParams, err := http_api.NewReqParams(req)
	if err != nil {
		return nil, http_api.Err{400, "INVALID_REQUEST"}
	}

	topicName, err := http_api.GetTopicPriorityArgs(reqParams)
	if err != nil {
		return nil, http_api.Err{400, err.Error()}
	}

	s.ctx.nsqlookupd.logf("DB: deleting topic (%s) from high priority", topicName)

	//if there is no such topic in the high prio list, return
	if len(s.ctx.nsqlookupd.DB.FindRegistrations("high_priority_topic",topicName,"")) == 0 {
			return map[string]interface{}{
			"status": "success",
		}, nil
	}
	//else, remove it
	key := Registration{"high_priority_topic",topicName,""}
	s.ctx.nsqlookupd.DB.RemoveRegistration(key)

	//we need to check if the topic is currently served by any other daemons
	//if there is a daemon and the daemon is high priority, we need to modify this registration so that next time
	//the topic can be served by high priority nsqds
	//set the #inactive flag to make the currently active producer inactive
	producers := s.ctx.nsqlookupd.DB.FindProducers("topic", topicName, "")
	producers = producers.FilterByActive(s.ctx.nsqlookupd.opts.InactiveProducerTimeout, s.ctx.nsqlookupd.opts.TombstoneLifetime)
	for _, p := range producers {
		if p.peerInfo.DaemonPriority == "HIGH" {
			previousReg := Registration{"topic",topicName,""}
			currentReg := Registration{"topic",topicName,"#inactive"}
			s.ctx.nsqlookupd.DB.RemoveProducer(previousReg, p.peerInfo.id)
			s.ctx.nsqlookupd.DB.AddProducer(currentReg, p)
		}
	}
	//find an inactive correpsonding low priority nsqd
	//switch its flag and set to active
	producers = s.ctx.nsqlookupd.DB.FindProducers("topic", topicName, "#inactive")
	producers = producers.FilterByActive(s.ctx.nsqlookupd.opts.InactiveProducerTimeout, s.ctx.nsqlookupd.opts.TombstoneLifetime)
	if len(producers) == 0 || len(producers) == len(findNSQds(producers,"HIGH")) {
		//there is no low prio nsqds having experience serving this topic
		//just delete the registration, as the new nsqd will register it 
		reg := Registration{"topic", topicName, ""}
		s.ctx.nsqlookupd.DB.RemoveRegistration(reg)
	} else {
		for _, p := range producers {
			if p.peerInfo.DaemonPriority == "LOW" {
				previousReg := Registration{"topic",topicName,"#inactive"}
				currentReg := Registration{"topic",topicName,""}
				s.ctx.nsqlookupd.DB.RemoveProducer(previousReg, p.peerInfo.id)
				s.ctx.nsqlookupd.DB.AddProducer(currentReg, p)
			}
		}
	}

	return map[string]interface{}{
		"status": "success",
	}, nil
}


//yao
//find certain daemon_priorioty nsqds
//havent finished this part
func (s *httpServer) doPriorityNodes(w http.ResponseWriter, req *http.Request, ps httprouter.Params) (interface{}, error) {
	return nil, nil
}

func findNSQds(producers Producers, priority string) (Producers) {
	var outputProducers Producers
	for _, p := range producers {
		if p.peerInfo.DaemonPriority == priority {
			outputProducers = append(outputProducers, p)
		}
	}
	return outputProducers
}

//yao
//find nsqd address based on the priority specifies
//return a nsqd with appropriate nsqd address
//but might return a random nsqd address if a speicifc kind of nsqd doesnt exist
//eg: ask for high prio, but there is no high prio nsqd. In that case, return addresses of low prio daemons
//Moreover, return all the addresses of specific kind of daemons and let producer decide by itself
func (s *httpServer) doProducerLookupV2(w http.ResponseWriter, req *http.Request, ps httprouter.Params) (interface{}, error) {

	// dont filter out tombstoned nodes
	producers := s.ctx.nsqlookupd.DB.FindProducers("client", "", "").FilterByActive(
		s.ctx.nsqlookupd.opts.InactiveProducerTimeout, 0)

	reqParams, err := http_api.NewReqParams(req)
	if err != nil {
		return nil, http_api.Err{400, "INVALID_REQUEST"}
	}
	priorityLevel, err := http_api.GetPriorityArgs(reqParams)
	if err != nil {
		return nil, http_api.Err{400, err.Error()}
	}

	numNSQds := 0
	tolerantFlag := false;
	for _, q := range producers {
		if q.peerInfo.DaemonPriority == priorityLevel {
			numNSQds++
		}
	}
	if numNSQds == 0 {
		tolerantFlag = true
	}

	var nodes []*node
	for _, p := range producers {
		if p.peerInfo.DaemonPriority != priorityLevel && tolerantFlag == false{
			continue
		}

		topics := s.ctx.nsqlookupd.DB.LookupRegistrations(p.peerInfo.id).Filter("topic", "*", "").Keys()
		// for each topic find the producer that matches this peer
		// to add tombstone information
		tombstones := make([]bool, len(topics))
		for j, t := range topics {
			topicProducers := s.ctx.nsqlookupd.DB.FindProducers("topic", t, "")
			for _, tp := range topicProducers {
				if tp.peerInfo == p.peerInfo {
					tombstones[j] = tp.IsTombstoned(s.ctx.nsqlookupd.opts.TombstoneLifetime)
				}
			}
		}

		nodes = append(nodes, &node{
			RemoteAddress:    p.peerInfo.RemoteAddress,
			Hostname:         p.peerInfo.Hostname,
			BroadcastAddress: p.peerInfo.BroadcastAddress,
			TCPPort:          p.peerInfo.TCPPort,
			HTTPPort:         p.peerInfo.HTTPPort,
			Version:          p.peerInfo.Version,
			Tombstones:       tombstones,
			Topics:           topics,
		})
	}

	return map[string]interface{}{
		"producers": nodes,
	}, nil
}
