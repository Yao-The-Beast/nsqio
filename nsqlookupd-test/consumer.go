package main

import (
  "log"
  "sync"
  "os"
  "github.com/nsqio/go-nsq"
)

func main() {

  wg := &sync.WaitGroup{}
  wg.Add(10)

  config := nsq.NewConfig()
  q, _ := nsq.NewConsumer(os.Args[1], "whatever", config)
  q.AddHandler(nsq.HandlerFunc(func(message *nsq.Message) error {
      log.Printf("Got a message: %v", message)
      wg.Done()
      return nil
  }))
  err := q.ConnectToNSQLookupd("127.0.0.1:4161")
  if err != nil {
      log.Panic("Could not connect")
  }
  wg.Wait()
}
