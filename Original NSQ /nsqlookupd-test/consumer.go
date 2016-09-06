package main

import (
  "log"
  "sync"
  "os"
  "time"
  "github.com/nsqio/go-nsq"
  "strconv"
)

func main() {

  wg := &sync.WaitGroup{}
  wg.Add(100)

  config := nsq.NewConfig()
  q, _ := nsq.NewConsumer(os.Args[1], "whatever", config)
  q.AddHandler(nsq.HandlerFunc(func(message *nsq.Message) error {
      currentTime := time.Now().UnixNano()
      sentTime,_ := strconv.ParseInt(string(message.Body),10,64)
      println("Latency", currentTime - sentTime)
      wg.Done()
      return nil
  }))
  err := q.ConnectToNSQLookupd("127.0.0.1:4161")
  if err != nil {
      log.Panic("Could not connect")
  }
  wg.Wait()
}
