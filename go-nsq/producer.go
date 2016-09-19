package nsq

import (
	"fmt"
	"log"
	"os"
	"sync"
	"sync/atomic"
	"time"
	"math/rand"
	"strings"
	"errors"
	"net"
	"net/url"
	"strconv"
)

type producerConn interface {
	String() string
	SetLogger(logger, LogLevel, string)
	Connect() (*IdentifyResponse, error)
	Close() error
	WriteCommand(*Command) error
}

//yao
type registerResponse struct {
	Status  string    `json:"status"`
}

// Producer is a high-level type to publish to NSQ.
//
// A Producer instance is 1:1 with a destination `nsqd`
// and will lazily connect to that instance (and re-connect)
// when Publish commands are executed.
type Producer struct {
	id     int64
	addr   string
	conn   producerConn
	config Config

	logger   logger
	logLvl   LogLevel
	logGuard sync.RWMutex

	responseChan chan []byte
	errorChan    chan []byte
	closeChan    chan int

	transactionChan chan *ProducerTransaction
	transactions    []*ProducerTransaction
	state           int32

	concurrentProducers int32
	stopFlag            int32
	exitChan            chan int
	wg                  sync.WaitGroup
	guard               sync.Mutex

	//lookupd
	lookupdRecheckChan chan int
	lookupdHTTPAddrs   []string
	lookupdQueryIndex  int

	behaviorDelegate interface{}

	mtx sync.RWMutex

}

// ProducerTransaction is returned by the async publish methods
// to retrieve metadata about the command after the
// response is received.
type ProducerTransaction struct {
	cmd      *Command
	doneChan chan *ProducerTransaction
	Error    error         // the error (or nil) of the publish command
	Args     []interface{} // the slice of variadic arguments passed to PublishAsync or MultiPublishAsync
}

func (t *ProducerTransaction) finish() {
	if t.doneChan != nil {
		t.doneChan <- t
	}
}

// NewProducer returns an instance of Producer for the specified address
//
// The only valid way to create a Config is via NewConfig, using a struct literal will panic.
// After Config is passed into NewProducer the values are no longer mutable (they are copied).
func NewProducer(addr string, config *Config) (*Producer, error) {
	config.assertInitialized()
	err := config.Validate()
	if err != nil {
		return nil, err
	}

	p := &Producer{
		id: atomic.AddInt64(&instCount, 1),

		addr:   addr,
		config: *config,

		logger: log.New(os.Stderr, "", log.Flags()),
		logLvl: LogLevelInfo,

		transactionChan: make(chan *ProducerTransaction),
		exitChan:        make(chan int),
		responseChan:    make(chan []byte),
		errorChan:       make(chan []byte),
	}
	return p, nil
}

// Ping causes the Producer to connect to it's configured nsqd (if not already
// connected) and send a `Nop` command, returning any error that might occur.
//
// This method can be used to verify that a newly-created Producer instance is
// configured correctly, rather than relying on the lazy "connect on Publish"
// behavior of a Producer.
func (w *Producer) Ping() error {
	if atomic.LoadInt32(&w.state) != StateConnected {
		err := w.connect()
		if err != nil {
			return err
		}
	}

	return w.conn.WriteCommand(Nop())
}

// SetLogger assigns the logger to use as well as a level
//
// The logger parameter is an interface that requires the following
// method to be implemented (such as the the stdlib log.Logger):
//
//    Output(calldepth int, s string)
//
func (w *Producer) SetLogger(l logger, lvl LogLevel) {
	w.logGuard.Lock()
	defer w.logGuard.Unlock()

	w.logger = l
	w.logLvl = lvl
}

func (w *Producer) getLogger() (logger, LogLevel) {
	w.logGuard.RLock()
	defer w.logGuard.RUnlock()

	return w.logger, w.logLvl
}

// String returns the address of the Producer
func (w *Producer) String() string {
	return w.addr
}

// Stop initiates a graceful stop of the Producer (permanent)
//
// NOTE: this blocks until completion
func (w *Producer) Stop() {
	w.guard.Lock()
	if !atomic.CompareAndSwapInt32(&w.stopFlag, 0, 1) {
		w.guard.Unlock()
		return
	}
	w.log(LogLevelInfo, "stopping")
	close(w.exitChan)
	w.close()
	w.guard.Unlock()
	w.wg.Wait()
}

// PublishAsync publishes a message body to the specified topic
// but does not wait for the response from `nsqd`.
//
// When the Producer eventually receives the response from `nsqd`,
// the supplied `doneChan` (if specified)
// will receive a `ProducerTransaction` instance with the supplied variadic arguments
// and the response error if present
func (w *Producer) PublishAsync(topic string, body []byte, doneChan chan *ProducerTransaction,
	args ...interface{}) error {
	return w.sendCommandAsync(Publish(topic, body), doneChan, args)
}

// MultiPublishAsync publishes a slice of message bodies to the specified topic
// but does not wait for the response from `nsqd`.
//
// When the Producer eventually receives the response from `nsqd`,
// the supplied `doneChan` (if specified)
// will receive a `ProducerTransaction` instance with the supplied variadic arguments
// and the response error if present
func (w *Producer) MultiPublishAsync(topic string, body [][]byte, doneChan chan *ProducerTransaction,
	args ...interface{}) error {
	cmd, err := MultiPublish(topic, body)
	if err != nil {
		return err
	}
	return w.sendCommandAsync(cmd, doneChan, args)
}

// Publish synchronously publishes a message body to the specified topic, returning
// an error if publish failed
func (w *Producer) Publish(topic string, body []byte) error {
	return w.sendCommand(Publish(topic, body))
}

// MultiPublish synchronously publishes a slice of message bodies to the specified topic, returning
// an error if publish failed
func (w *Producer) MultiPublish(topic string, body [][]byte) error {
	cmd, err := MultiPublish(topic, body)
	if err != nil {
		return err
	}
	return w.sendCommand(cmd)
}

// DeferredPublish synchronously publishes a message body to the specified topic
// where the message will queue at the channel level until the timeout expires, returning
// an error if publish failed
func (w *Producer) DeferredPublish(topic string, delay time.Duration, body []byte) error {
	return w.sendCommand(DeferredPublish(topic, delay, body))
}

// DeferredPublishAsync publishes a message body to the specified topic
// where the message will queue at the channel level until the timeout expires
// but does not wait for the response from `nsqd`.
//
// When the Producer eventually receives the response from `nsqd`,
// the supplied `doneChan` (if specified)
// will receive a `ProducerTransaction` instance with the supplied variadic arguments
// and the response error if present
func (w *Producer) DeferredPublishAsync(topic string, delay time.Duration, body []byte,
	doneChan chan *ProducerTransaction, args ...interface{}) error {
	return w.sendCommandAsync(DeferredPublish(topic, delay, body), doneChan, args)
}

func (w *Producer) sendCommand(cmd *Command) error {
	doneChan := make(chan *ProducerTransaction)
	err := w.sendCommandAsync(cmd, doneChan, nil)
	if err != nil {
		close(doneChan)
		return err
	}
	t := <-doneChan
	return t.Error
}

func (w *Producer) sendCommandAsync(cmd *Command, doneChan chan *ProducerTransaction,
	args []interface{}) error {
	// keep track of how many outstanding producers we're dealing with
	// in order to later ensure that we clean them all up...
	atomic.AddInt32(&w.concurrentProducers, 1)
	defer atomic.AddInt32(&w.concurrentProducers, -1)

	if atomic.LoadInt32(&w.state) != StateConnected {
		err := w.connect()
		if err != nil {
			return err
		}
	}

	t := &ProducerTransaction{
		cmd:      cmd,
		doneChan: doneChan,
		Args:     args,
	}

	select {
	case w.transactionChan <- t:
	case <-w.exitChan:
		return ErrStopped
	}

	return nil
}

func (w *Producer) connect() error {

	w.guard.Lock()
	defer w.guard.Unlock()
	
	//yao wait for w to be nonempty
	for w.addr == "" {
		time.Sleep(1 * time.Microsecond)
	}

	if atomic.LoadInt32(&w.stopFlag) == 1 {
		return ErrStopped
	}

	switch state := atomic.LoadInt32(&w.state); state {
	case StateInit:
	case StateConnected:
		return nil
	default:
		return ErrNotConnected
	}

	w.log(LogLevelInfo, "(%s) connecting to nsqd", w.addr)

	logger, logLvl := w.getLogger()

	w.conn = NewConn(w.addr, &w.config, &producerConnDelegate{w})
	w.conn.SetLogger(logger, logLvl, fmt.Sprintf("%3d (%%s)", w.id))

	_, err := w.conn.Connect()
	if err != nil {
		w.conn.Close()
		w.log(LogLevelError, "(%s) error connecting to nsqd - %s", w.addr, err)
		return err
	}
	atomic.StoreInt32(&w.state, StateConnected)
	w.closeChan = make(chan int)
	w.wg.Add(1)
	go w.router()

	return nil
}

func (w *Producer) close() {
	if !atomic.CompareAndSwapInt32(&w.state, StateConnected, StateDisconnected) {
		return
	}
	w.conn.Close()
	go func() {
		// we need to handle this in a goroutine so we don't
		// block the caller from making progress
		w.wg.Wait()
		atomic.StoreInt32(&w.state, StateInit)
	}()
}

func (w *Producer) router() {
	for {
		select {
		case t := <-w.transactionChan:
			w.transactions = append(w.transactions, t)
			err := w.conn.WriteCommand(t.cmd)
			if err != nil {
				w.log(LogLevelError, "(%s) sending command - %s", w.conn.String(), err)
				w.close()
			}
		case data := <-w.responseChan:
			w.popTransaction(FrameTypeResponse, data)
		case data := <-w.errorChan:
			w.popTransaction(FrameTypeError, data)
		case <-w.closeChan:
			goto exit
		case <-w.exitChan:
			goto exit
		}
	}

exit:
	w.transactionCleanup()
	w.wg.Done()
	w.log(LogLevelInfo, "exiting router")
}

func (w *Producer) popTransaction(frameType int32, data []byte) {
	t := w.transactions[0]
	w.transactions = w.transactions[1:]
	if frameType == FrameTypeError {
		t.Error = ErrProtocol{string(data)}
	}
	t.finish()
}

func (w *Producer) transactionCleanup() {
	// clean up transactions we can easily account for
	for _, t := range w.transactions {
		t.Error = ErrNotConnected
		t.finish()
	}
	w.transactions = w.transactions[:0]

	// spin and free up any writes that might have raced
	// with the cleanup process (blocked on writing
	// to transactionChan)
	for {
		select {
		case t := <-w.transactionChan:
			t.Error = ErrNotConnected
			t.finish()
		default:
			// keep spinning until there are 0 concurrent producers
			if atomic.LoadInt32(&w.concurrentProducers) == 0 {
				return
			}
			// give the runtime a chance to schedule other racing goroutines
			time.Sleep(5 * time.Millisecond)
		}
	}
}

func (w *Producer) log(lvl LogLevel, line string, args ...interface{}) {
	logger, logLvl := w.getLogger()

	if logger == nil {
		return
	}

	if logLvl > lvl {
		return
	}

	logger.Output(2, fmt.Sprintf("%-4s %3d %s", lvl, w.id, fmt.Sprintf(line, args...)))
}

func (w *Producer) onConnResponse(c *Conn, data []byte) { w.responseChan <- data }
func (w *Producer) onConnError(c *Conn, data []byte)    { w.errorChan <- data }
func (w *Producer) onConnHeartbeat(c *Conn)             {}
func (w *Producer) onConnIOError(c *Conn, err error)    { w.close() }
func (w *Producer) onConnClose(c *Conn) {
	w.guard.Lock()
	defer w.guard.Unlock()
	close(w.closeChan)
}



//yao
/*------------------------------------------------*/

// SetBehaviorDelegate blablabla
func (w *Producer) SetBehaviorDelegate(cb interface{}) {
	matched := false

	if _, ok := cb.(DiscoveryFilter); ok {
		matched = true
	}

	if !matched {
		panic("behavior delegate does not have any recognized methods")
	}

	w.behaviorDelegate = cb
}





//yao
func (w *Producer) ConnectToNSQLookupd(addr string, topic string) error {
	if atomic.LoadInt32(&w.stopFlag) == 1 {
		return errors.New("producer stopped")
	}

	if err := validatedLookupAddr(addr); err != nil {
		return err
	}

	//atomic.StoreInt32(&r.connectedFlag, 1)

	for _, x := range w.lookupdHTTPAddrs {
		if x == addr {
			return nil
		}
	}
	w.lookupdHTTPAddrs = append(w.lookupdHTTPAddrs, addr)
	numLookupd := len(w.lookupdHTTPAddrs)

	// if this is the first one, kick off the go loop
	if numLookupd == 1 {
		w.addr = w.queryLookupd(topic)
		//w.wg.Add(1)
		//go w.lookupdLoop(topic)
	}
	return nil
}

//yao
//administrative function //quite op actually
//register high priority topic
func (w *Producer) RegisterHighPriorityTopic(addr string, topic string) error {
	
	if atomic.LoadInt32(&w.stopFlag) == 1 {
		return errors.New("producer stopped")
	}

	if err := validatedLookupAddr(addr); err != nil {
		return err
	}

	for _, x := range w.lookupdHTTPAddrs {
		if x == addr {
			return nil
		}
	}
	w.lookupdHTTPAddrs = append(w.lookupdHTTPAddrs, addr)

	endpoint := w.nextLookupdEndpoint("register_topic_priority", topic)
	w.log(LogLevelInfo, "querying nsqlookupd %s", endpoint)
	var data registerResponse
	err := apiRequestNegotiateV1("GET", endpoint, nil, &data)
	if err != nil {
		w.log(LogLevelError, "error querying nsqlookupd (%s) - %s", endpoint, err)
	}
	return nil
}

//yao
//administrative function 
//unregister high priority topic
func (w *Producer) UnregisterHighPriorityTopic(addr string, topic string) error {
	if atomic.LoadInt32(&w.stopFlag) == 1 {
		return errors.New("producer stopped")
	}

	if err := validatedLookupAddr(addr); err != nil {
		return err
	}

	for _, x := range w.lookupdHTTPAddrs {
		if x == addr {
			return nil
		}
	}
	w.lookupdHTTPAddrs = append(w.lookupdHTTPAddrs, addr)

	endpoint := w.nextLookupdEndpoint("unregister_topic_priority", topic)
	w.log(LogLevelInfo, "querying nsqlookupd %s", endpoint)
	var data registerResponse
	err := apiRequestNegotiateV1("GET", endpoint, nil, &data)
	if err != nil {
		w.log(LogLevelError, "error querying nsqlookupd (%s) - %s", endpoint, err)
	}
	return nil
}

// return the next lookupd endpoint to query
// keeping track of which one was last used
func (w *Producer) nextLookupdEndpoint(command string, data string) string {
	w.mtx.RLock()
	if w.lookupdQueryIndex >= len(w.lookupdHTTPAddrs) {
		w.lookupdQueryIndex = 0
	}
	addr := w.lookupdHTTPAddrs[w.lookupdQueryIndex]
	num := len(w.lookupdHTTPAddrs)
	w.mtx.RUnlock()
	w.lookupdQueryIndex = (w.lookupdQueryIndex + 1) % num


	urlString := addr
	if !strings.Contains(urlString, "://") {
		urlString = "http://" + addr
	}

	u, err := url.Parse(urlString)
	if err != nil {
		panic(err)
	}

	if command == "producer_lookup" {
		if u.Path == "/" || u.Path == "" {
			u.Path = "/producer_lookup"
		}
		v, _ := url.ParseQuery(u.RawQuery)
		v.Add("topic", data)
		hostname, _ := os.Hostname()
		v.Add("hostname", hostname)
		u.RawQuery = v.Encode()
	} else if command == "nodes" {
		if u.Path == "/" || u.Path == "" {
			u.Path = "/nodes"
		}
		v, _ := url.ParseQuery(u.RawQuery)
		u.RawQuery = v.Encode()
	} else if command == "register_topic_priority" {
		if u.Path == "/" || u.Path == "" {
			u.Path = "/register_topic_priority"
		}
		v, _ := url.ParseQuery(u.RawQuery)
		v.Add("topic",data)
		u.RawQuery = v.Encode()
	} else if command == "unregister_topic_priority" {
		if u.Path == "/" || u.Path == "" {
			u.Path = "/unregister_topic_priority"
		}
		v, _ := url.ParseQuery(u.RawQuery)
		v.Add("topic",data)
		u.RawQuery = v.Encode()
	} else if command == "producer_lookup_v2" {
		if u.Path == "/" || u.Path == "" {
			u.Path = "/producer_lookup_v2"
		}
		v, _ := url.ParseQuery(u.RawQuery)
		v.Add("priority",data)
		hostname, _ := os.Hostname()
		v.Add("hostname", hostname)
		u.RawQuery = v.Encode()
	}

	return u.String()
}


func (w *Producer) queryLookupd(topic string) string{
	//first lookup the topic
	//if there is no such topic registered in the nsqlookupd
	//find a random nsqd and connects to it
	endpoint := w.nextLookupdEndpoint("producer_lookup", topic)
	w.log(LogLevelInfo, "querying nsqlookupd %s", endpoint)
	var data lookupResp
	err := apiRequestNegotiateV1("GET", endpoint, nil, &data)
	if err != nil {
		w.log(LogLevelError, "error querying nsqlookupd (%s) - %s", endpoint, err)
		//query /nodes
		//grab a random NSQd to establish the link
		endpoint = w.nextLookupdEndpoint("nodes", "")
		w.log(LogLevelInfo, "querying nsqlookupd %s", endpoint)
		err = apiRequestNegotiateV1("GET", endpoint, nil, &data)
		if err != nil {
			w.log(LogLevelError, "error querying nsqlookupd (%s) - %s", endpoint, err)
			return "ERROR"
		}
	}


	var nsqdAddrs []string
	for _, producer := range data.Producers {
		broadcastAddress := producer.BroadcastAddress
		port := producer.TCPPort
		joined := net.JoinHostPort(broadcastAddress, strconv.Itoa(port))
		nsqdAddrs = append(nsqdAddrs, joined)
	}

	if discoveryFilter, ok := w.behaviorDelegate.(DiscoveryFilter); ok {
		nsqdAddrs = discoveryFilter.Filter(nsqdAddrs)
	}

	address := ""
	

	if len(nsqdAddrs) == 0 {
		panic("NO DAEMON AVAILABLE")
	}else {
		i := 0
		for _, thisProducer := range data.Producers {
			hostName := thisProducer.Hostname
			myHostName,_ := os.Hostname()
			if myHostName == hostName {
			//	println("YAO: NSQd Hostname is: ", hostName)
				address = nsqdAddrs[i]
				break
			}
			i++
		}	
	}
	if address == "" {
		println("YAO: NO OTHER CHOICE, Randomly Pick One")
		address = nsqdAddrs[0]
	}
	//println(data.Producers[0].BroadcastAddress)
	println("YAO: TOPIC:", topic, "; NSQd ADDRESS is: ", address)
	return address
	
}

// poll all known lookup servers every LookupdPollInterval
func (w *Producer) lookupdLoop(topic string) {
	// add some jitter so that multiple consumers discovering the same topic,
	// when restarted at the same time, dont all connect at once.
	s1 := rand.NewSource(time.Now().UnixNano())
    r1 := rand.New(s1)
	jitter := time.Millisecond * (time.Duration(r1.Intn(100) + 500))
	var ticker *time.Ticker

	select {
		case <-time.After(jitter):
		case <-w.exitChan:
			goto exit
	}

	//ticker = time.NewTicker(r.config.LookupdPollInterval)
	ticker = time.NewTicker(jitter)
	for {
		select {
			case <-ticker.C:
				w.addr = w.queryLookupd(topic)
			case <-w.exitChan:
				goto exit
		}
	}

exit:
	if ticker != nil {
		ticker.Stop()
	}
	w.log(LogLevelInfo, "exiting lookupdLoop")
	w.wg.Done()
}




//yao
//in this connection protocol, we dont connect to nsqds based on topics
//but based on priority
func (w *Producer) ConnectToNSQLookupd_v2(addr string, priority string) error {
	if atomic.LoadInt32(&w.stopFlag) == 1 {
		return errors.New("producer stopped")
	}

	if err := validatedLookupAddr(addr); err != nil {
		return err
	}

	//atomic.StoreInt32(&r.connectedFlag, 1)

	for _, x := range w.lookupdHTTPAddrs {
		if x == addr {
			return nil
		}
	}
	w.lookupdHTTPAddrs = append(w.lookupdHTTPAddrs, addr)
	numLookupd := len(w.lookupdHTTPAddrs)

	// if this is the first one, kick off the go loop
	if numLookupd == 1 {
		w.addr = w.queryLookupd_v2(priority)
	}
	return nil	
}

func (w *Producer) queryLookupd_v2(priority string) string {
	//find a nsqd with the appropriate priority
	endpoint := w.nextLookupdEndpoint("producer_lookup_v2", priority)
	w.log(LogLevelInfo, "querying nsqlookupd v2 %s", endpoint)
	var data lookupResp
	err := apiRequestNegotiateV1("GET", endpoint, nil, &data)
	if err != nil {
		w.log(LogLevelError, "error querying nsqlookupd (%s) - %s", endpoint, err)
	}

	var nsqdAddrs []string
	for _, producer := range data.Producers {
		broadcastAddress := producer.BroadcastAddress
		port := producer.TCPPort
		joined := net.JoinHostPort(broadcastAddress, strconv.Itoa(port))
		nsqdAddrs = append(nsqdAddrs, joined)
	}

	if discoveryFilter, ok := w.behaviorDelegate.(DiscoveryFilter); ok {
		nsqdAddrs = discoveryFilter.Filter(nsqdAddrs)
	}

	address := ""
	
	if len(nsqdAddrs) == 0 {
		panic("NO DAEMON AVAILABLE")
	}else {
		i := 0
		for _, thisProducer := range data.Producers {
			hostName := thisProducer.Hostname
			myHostName,_ := os.Hostname()
			if myHostName == hostName {
				address = nsqdAddrs[i]
				break
			}
			i++
		}	
	}
	if address == "" {
		println("YAO: NO OTHER CHOICE, Randomly Pick One")
		address = nsqdAddrs[0]
	}
	//println(data.Producers[0].BroadcastAddress)
	println("YAO; NSQd ADDRESS v2 is: ", address)
	return address
}