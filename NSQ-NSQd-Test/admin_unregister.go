package main

import (
  "github.com/nsqio/go-nsq"
  "os"
)

func main() {
  config := nsq.NewConfig()
  w, _ := nsq.NewProducer("", config)
  _ = w.UnregisterHighPriorityTopic("127.0.0.1:4161",os.Args[1])
}

