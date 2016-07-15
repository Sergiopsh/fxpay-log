package main

import (
    "fmt"
    "github.com/hpcloud/tail"
    regex "regexp"
    elasticapi "gopkg.in/olivere/elastic.v3"
    "time"
/*    "strings"*/
    "os"
)

var logNAME             string = "/var/log/fxpay/cluster-logs/fxpay.log"
var newStringPtrn       string = "^[0-9]{4}-[0-9]{2}-[0-9]{2} [0-9]{2}:[0-9]{2}:[0-9]{2}[:0-9]* "
var elasticUrl          string = "http://185.19.218.2:9288"

var elasticClient       *elasticapi.Client = nil;

func elasticInit(){
    var err error
    elasticClient = nil
    for elasticClient == nil {
        elasticClient, err = elasticapi.NewClient(
            elasticapi.SetURL(elasticUrl),
            elasticapi.SetSniff(false))
        if err != nil {
            fmt.Println(err)
            elasticClient = nil
            time.Sleep(5 * time.Second)
        }
    }

}
func elasticPing(){
    var status bool = false;
    for status == false{
        status = elasticClient.IsRunning()
        if !status {
            elasticInit()
        }
    }

}

var ArrMap map[string]map[string]string

func main() {

    //get first arg as file_name

    if len(os.Args)==2 {
        logNAME = os.Args[1]
    }
    fmt.Print(logNAME)
    ArrMap = make (map[string]map[string]string)

    elasticInit()
    var c chan []string = make(chan []string)
    go routine(c);
    go routine(c);
    go routine(c);
    go routine(c);
    go routine(c);
    go routine(c);
    go routine(c);
    go routine(c);

    var messMap []string
    tc := tail.Config{Follow: true, ReOpen: true, MustExist: true, Poll: true}
    t, _ := tail.TailFile(logNAME, tc  )
/*    for line := range t.Lines {
        //try to find start INBOUND or OUBOUND message
        if res,_ := regex.MatchString(newStringPtrn,line.Text); res == true{
            if messMap != nil{
                //sendES(messMap)
                c <- messMap
            }
            //Drop okd message from map
            messMap = nil
            messMap = append(messMap,line.Text)
            continue
        }
        if res,_ := regex.MatchString(newStringPtrn,line.Text); res == false && messMap != nil {//MESSAGE BODY
            messMap = append(messMap, line.Text)
            continue
        }
    }*/
    for line := range t.Lines {
        //try to find start INBOUND or OUBOUND message
        if res,_ := regex.MatchString(newStringPtrn,line.Text); res == true{
            if messMap != nil{
                //sendES(messMap)
                c <- messMap
            }
            //Drop okd message from map
            messMap = nil
            messMap = append(messMap,line.Text)
            continue
        }
        if res,_ := regex.MatchString(newStringPtrn,line.Text); res == false && messMap != nil {//MESSAGE BODY
            messMap = append(messMap, line.Text)
            continue
        }
    }

}

func routine(c chan []string){
    for {
        sendES(<- c)
    }

}


func sendES(messMap []string){
    //only error strings

    //if res,_ := regex.MatchString("^[0-9]{4}-[0-9]{2}-[0-9]{2} [0-9]{2}:[0-9]{2}:[0-9]{2}[:0-9]* ERROR ",messMap[0]); res == false {
      //  return
    //}

    sendArr := make(map[string]string)

    first := regex.MustCompile("^([0-9]{4}-[0-9]{2}-[0-9]{2}) ([0-9]{2}:[0-9]{2}:[0-9]{2})(:[0-9]{3})[\t ]+([A-Z]*)")
    strfirst := first.FindAllStringSubmatch(messMap[0], -1)

    tt, _ := time.Parse("2006-01-02 15:04:05", strfirst[0][1] + " " + strfirst[0][2])
    sendArr["timestamp"] = tt.Format("2006-01-02T15:04:05.000Z07:00")
    sendArr["level"] = strfirst[0][4]

    for _,mess := range messMap {
        sendArr["val"] = sendArr["val"] + "\n" + mess
    }


    var ok bool = false

            //send to Elastic
            ok = false
            for ok == false {
                elasticPing()
                _, err := elasticClient.Index().
                Index("fxpay-"+strfirst[0][1]).
                Type("ws-log").
                BodyJson(sendArr).
                Do()
                if err == nil {
                    ok = true
                }else {
                    fmt.Println(err)
                    time.Sleep(1 * time.Second)
                }
            }




}
