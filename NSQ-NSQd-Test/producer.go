package main

import (
  "log"
  "github.com/nsqio/go-nsq"
  "os"
  "time"
  "encoding/binary"
)

func main() {
  config := nsq.NewConfig()
  w, _ := nsq.NewProducer("", config)
//  w, _ := nsq.NewProducer("127.0.0.1:4150", config)
  _ = w.ConnectToNSQLookupd("127.0.0.1:4161",os.Args[1])
  i := 0
  for i < 50 {
  	err := w.PublishAsync(os.Args[1], Int64ToBytes(time.Now().UnixNano()),nil)
  	time.Sleep(100 * time.Millisecond)
	if err != nil {
      		log.Panic("Could not connect")
  	}
 	i = i + 1
   }

  w.Stop()
}

func Int64ToBytes(i int64) []byte {
    var buf = make([]byte, 8)
    binary.BigEndian.PutUint64(buf, uint64(i))
    return buf
}
