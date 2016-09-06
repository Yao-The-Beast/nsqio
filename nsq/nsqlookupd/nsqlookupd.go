package nsqlookupd

import (
	"fmt"
	"net"
	"os"
	"sync"
	"io/ioutil"
	"strings"

	"github.com/nsqio/nsq/internal/http_api"
	"github.com/nsqio/nsq/internal/protocol"
	"github.com/nsqio/nsq/internal/util"
	"github.com/nsqio/nsq/internal/version"
)

type NSQLookupd struct {
	sync.RWMutex
	opts         *Options
	tcpListener  net.Listener
	httpListener net.Listener
	waitGroup    util.WaitGroupWrapper
	DB           *RegistrationDB
}

func New(opts *Options) *NSQLookupd {
	n := &NSQLookupd{
		opts: opts,
		DB:   NewRegistrationDB(),
	}
	n.logf(version.String("nsqlookupd"))
	return n
}

func (l *NSQLookupd) logf(f string, args ...interface{}) {
	if l.opts.Logger == nil {
		return
	}
	l.opts.Logger.Output(2, fmt.Sprintf(f, args...))
}

func (l *NSQLookupd) Main() {
	ctx := &Context{l}

	//yao
	l.grabHighPriorityTopics("TOPICS")

	tcpListener, err := net.Listen("tcp", l.opts.TCPAddress)
	if err != nil {
		l.logf("FATAL: listen (%s) failed - %s", l.opts.TCPAddress, err)
		os.Exit(1)
	}
	l.Lock()
	l.tcpListener = tcpListener
	l.Unlock()
	tcpServer := &tcpServer{ctx: ctx}
	l.waitGroup.Wrap(func() {
		protocol.TCPServer(tcpListener, tcpServer, l.opts.Logger)
	})

	httpListener, err := net.Listen("tcp", l.opts.HTTPAddress)
	if err != nil {
		l.logf("FATAL: listen (%s) failed - %s", l.opts.HTTPAddress, err)
		os.Exit(1)
	}
	l.Lock()
	l.httpListener = httpListener
	l.Unlock()
	httpServer := newHTTPServer(ctx)
	l.waitGroup.Wrap(func() {
		http_api.Serve(httpListener, httpServer, "HTTP", l.opts.Logger)
	})
}

//YAO
//Initilize the high_priority topic list
func (l *NSQLookupd) grabHighPriorityTopics(fileName string) {
	
	content, err := ioutil.ReadFile(fileName)
	if err != nil {
		println("YAO:READ ERROR")
	}
	lines := strings.Split(string(content), "\n")

	for _, line := range lines {
		temp := strings.Split(line," ")
		topicName := temp[0]
		key := Registration{"high_priority_topic",topicName,""}
		l.DB.AddRegistration(key)
	}

}

func (l *NSQLookupd) RealTCPAddr() *net.TCPAddr {
	l.RLock()
	defer l.RUnlock()
	return l.tcpListener.Addr().(*net.TCPAddr)
}

func (l *NSQLookupd) RealHTTPAddr() *net.TCPAddr {
	l.RLock()
	defer l.RUnlock()
	return l.httpListener.Addr().(*net.TCPAddr)
}

func (l *NSQLookupd) Exit() {
	if l.tcpListener != nil {
		l.tcpListener.Close()
	}

	if l.httpListener != nil {
		l.httpListener.Close()
	}
	l.waitGroup.Wait()
}
