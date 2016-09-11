package main

import (
//  "log"
  "github.com/nsqio/go-nsq"
  "os"
//  "time"
//  "encoding/binary"
)

func main() {
  config := nsq.NewConfig()
  w, _ := nsq.NewProducer("", config)
//  w, _ := nsq.NewProducer("127.0.0.1:4150", config)
//  _ = w.ConnectToNSQLookupd("127.0.0.1:4161",os.Args[1])
  _ = w.RegisterHighPriorityTopic("127.0.0.1:4161",os.Args[1])
}

