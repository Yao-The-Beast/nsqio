package main

import (
  "log"
  "github.com/nsqio/go-nsq"
  "os"
  "time"
  "encoding/binary"
  "strconv"
)

func producer(topic string, channel string, flag string) {
    messageSize := 1000
    messageNum := 1000

    if flag == "ephemeral" {
        topic += "#ephemeral"
        channel += "#ephemeral"
    }
    
    config := nsq.NewConfig()
    w, _ := nsq.NewProducer("", config)
    _ = w.ConnectToNSQLookupd("127.0.0.1:4161",topic)

    

    //hello message
    b := make([]byte, messageSize)
    binary.PutVarint(b, time.Now().UnixNano())
    w.PublishAsync(topic, b , nil)
    time.Sleep(20 * time.Second)

    i := 1
    for i < messageNum {
        b = make([]byte, messageSize)
        binary.PutVarint(b, time.Now().UnixNano())
        //publish
        err := w.PublishAsync(topic, b ,nil)
  	
        time.Sleep(10 * time.Millisecond)
	    if err != nil {
      		log.Panic("Could not connect")
  	    }
 	    i = i + 1
    }
    w.Stop()
}


func main() {

    producersNum,_ := strconv.Atoi(os.Args[1])

    for i := 0; i < producersNum; i++ {
        go producer(strconv.Itoa(i), strconv.Itoa(i), os.Args[2])
    }

    time.Sleep(60 * time.Second)

}