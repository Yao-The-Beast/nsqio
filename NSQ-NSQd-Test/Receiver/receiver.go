package main
import (
  "log"
  //"sync"
  "os"
  "io/ioutil"
  "time"
  "github.com/nsqio/go-nsq"
  "strconv"
  "encoding/binary"
)

var messageSize = 1000
var messageNum = 1000

//ReceiveMessage hahah
func ReceiveMessage(topic string, channel string, message []byte, Latencies *[]float32, Results *[]byte){
    now := time.Now().UnixNano()
	then, _ := binary.Varint(message)
	if then != 0 {
		*Latencies = append(*Latencies, (float32(now-then))/1000/1000)
		if channel == "0#ephemeral" {
			//log.Printf("%d \n", handler.messageCounter);
			b:=make([]byte,8)
			binary.PutVarint(b, (now-then))
			x:=strconv.FormatInt(now-then, 10)
			*Results=append(*Results, x...)
			*Results=append(*Results, "\n"...)
		}
	}

	if len(*Latencies) == messageNum{
		sum := float32(0)
		for _, latency := range *Latencies {
			sum += latency
		}
		avgLatency := float32(sum) / float32(len(*Latencies))
		log.Printf("Mean latency for %d messages: %f ms\n", messageNum,
			avgLatency)
		if channel == "0#ephemeral" {
			ioutil.WriteFile("latency", *Results, 0777)
		}
	}
}

func consumer(topic string, channel string) {
    
    topic += "#ephemeral"
    channel += "#ephemeral"

   // wg := &sync.WaitGroup{}
   // wg.Add(100)

    config := nsq.NewConfig()
    q, _ := nsq.NewConsumer(topic, channel, config)

    //store data
    var Latencies []float32
	var Results		[]byte

    q.AddHandler(nsq.HandlerFunc(func(message *nsq.Message) error {
		ReceiveMessage(topic, channel, message.Body, &Latencies, &Results)
    //    wg.Done()
		return nil
	}))

    err := q.ConnectToNSQLookupd("127.0.0.1:4161")
    if err != nil {
        log.Panic("Could not connect")
    }
  //  wg.Wait()
}

func main() {

    consumersNum,_ := strconv.Atoi(os.Args[1])

    for i := 0; i < consumersNum; i++ {
        go consumer(strconv.Itoa(i), strconv.Itoa(i))
    }

    time.Sleep(60 * time.Second)

  
}