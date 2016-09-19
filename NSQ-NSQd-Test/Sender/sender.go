package main

import (
  //"log"
  "github.com/nsqio/go-nsq"
  "os"
  "time"
  "encoding/binary"
  "strconv"
)

func producer(topic string, channel string, flag string) {
    messageSize := 1000
    messageNum := 1000

    
    config := nsq.NewConfig()
    w, _ := nsq.NewProducer("", config)

    tmp, _ := strconv.Atoi(topic)
    priority := ""
    if tmp < 3{
        priority = "HIGH"
    }else{
        priority = "LOW"
    }
     _ = w.ConnectToNSQLookupd_v2("127.0.0.1:4161",priority)

     //_ = w.ConnectToNSQLookupd("127.0.0.1:4161",topic)

    if flag == "ephemeral" {
        topic += "#ephemeral"
        channel += "#ephemeral"
    }
    

    //hello message
    b := make([]byte, messageSize)
    binary.PutVarint(b, time.Now().UnixNano())
    w.PublishAsync(topic, b , nil)
    time.Sleep(20 * time.Second)

    i := 1
    for i < messageNum {
        b = make([]byte, messageSize)
        binary.PutVarint(b, time.Now().UnixNano())
        //err = w.PublishAsync("SUPER" + topic, b, nil)

        err := w.PublishAsync(topic, b ,nil)

        if err != nil {
            panic("CONNECTION ERROR")
        }

        time.Sleep(10 * time.Millisecond)

 	    i = i + 1
    }
    w.Stop()
}


func main() {

    producersNum,_ := strconv.Atoi(os.Args[1])

    for i := 0; i < producersNum; i++ {
        go producer(strconv.Itoa(i), strconv.Itoa(i), os.Args[2])
        //go producer("0", "0", os.Args[2])
    }

    time.Sleep(60 * time.Second)

}