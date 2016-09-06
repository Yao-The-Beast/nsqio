package main

import (
  "log"
  "github.com/nsqio/go-nsq"
  "os"
)

func main() {
  config := nsq.NewConfig()
  w, _ := nsq.NewProducer("", config)
//  w, _ := nsq.NewProducer("127.0.0.1:4150", config)
  _ = w.ConnectToNSQLookupd("127.0.0.1:4161",os.Args[1])
  i := 0
  for i < 10 {
  	err := w.Publish(os.Args[1], []byte("1"))
  	if err != nil {
      		log.Panic("Could not connect")
  	}
 	i = i + 1
   }

  w.Stop()
}
