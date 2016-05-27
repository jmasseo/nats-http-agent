package main

import (
	"encoding/json"
	"flag"
	"github.com/nats-io/nats"
	"io/ioutil"
	"log"
	"net/http"
	"os"
	"runtime"
	"strings"
)

type HTTPRequest struct {
	ID      string              `json:"id"`
	Method  string              `json:"method"`
	URL     string              `json:"url"`
	Headers map[string][]string `json:"headers"`
	Cookies map[string]string   `json:"cookies"`
	Body    string              `json:"body"`
}

type HTTPResponse struct {
	ID         string              `json:"id"`
	WorkerName string              `json:"workername"`
	Method     string              `json:"method"`
	Status     string              `json:"status"`
	StatusCode int                 `json:"statuscode"`
	URL        string              `json:"url"`
	Headers    map[string][]string `json:"headers"`
	Body       string              `json:"body"`
}

var (
	client     *http.Client
	workername string
	nc         *nats.Conn
)

// NOTE: Use tls scheme for TLS, e.g. nats-qsub -s tls://demo.nats.io:4443 foo
func usage() {
	log.Fatalf("Usage: nats-http-agent [-s server] [-t] <subject> <queue-group>\n")
}

type JobReq struct {
	HTTPReq HTTPRequest
	Reply   string
}

func workerTask(id int, jobs <-chan JobReq, nc *nats.Conn) {
	i := 0
	log.Printf("Loading worker %d", id)
	for j := range jobs {
		i++
		myreq := j.HTTPReq
		reply := j.Reply
		req, err := http.NewRequest(myreq.Method, myreq.URL, strings.NewReader(myreq.Body))
		for k, v := range myreq.Headers {
			req.Header[k] = v
		}
		resp, err := client.Do(req)
		if err != nil {
			log.Printf("[ERROR] [%d] Error Issuing HTTP Request[%v]: %+v", id, myreq.ID, err)
			str, _ := json.Marshal(err)
			nc.Publish("error", str)
		}
		bodystr, _ := ioutil.ReadAll(resp.Body)
		myresp := &HTTPResponse{
			ID:         myreq.ID,
			WorkerName: workername,
			Method:     myreq.Method,
			URL:        myreq.URL,
			Status:     resp.Status,
			Body:       string(bodystr),
			StatusCode: resp.StatusCode,
			Headers:    resp.Header,
		}
		resp.Body.Close()

		responseblock, _ := json.Marshal(myresp)
		log.Printf("[INFO] [%d]  Replying to %+v with %+v", id, reply, string(responseblock))
		err = nc.Publish(reply, responseblock)
		if err != nil {
			log.Printf("[ERROR] [%d] Replying Error [%v]: %+v", id, myreq.ID, err)
			str, _ := json.Marshal(err)
			nc.Publish("error", str)
		}

	}
	log.Printf("Worker %d logged %d jobs.", id, i)
}

/*
func printMsg(m *nats.Msg, i int, nconn *nats.Conn) {
	log.Printf("[#%d] Received on [%s] Queue[%s] Pid[%d]: '%s'\n", i, m.Subject, m.Sub.Queue, os.Getpid(), string(m.Data))
	myreq := HTTPRequest{}
	err := json.Unmarshal(m.Data, &myreq)
	if err != nil {
		log.Printf("[ERROR] Invalid HTTP Request: %+v", err)
		str, _ := json.Marshal(err)
		nconn.Publish("error", str)
	}
	jobreq := JobReq{HTTPReq: myreq, Reply: m.Reply}
	jobchan <- jobreq

	req, err := http.NewRequest(myreq.Method, myreq.URL, strings.NewReader(myreq.Body))
	for k, v := range myreq.Headers {
		req.Header[k] = v
	}
	resp, err := client.Do(req)
	if err != nil {
		log.Printf("[ERROR] Error Issuing HTTP Request[%v]: %+v", myreq.ID, err)
		str, _ := json.Marshal(err)
		nconn.Publish("error", str)
	}
	defer resp.Body.Close()
	bodystr, _ := ioutil.ReadAll(resp.Body)
	myresp := &HTTPResponse{
		ID:         myreq.ID,
		WorkerName: workername,
		Method:     myreq.Method,
		URL:        myreq.URL,
		Status:     resp.Status,
		Body:       string(bodystr),
		StatusCode: resp.StatusCode,
		Headers:    resp.Header,
	}
	responseblock, _ := json.Marshal(myresp)
	log.Printf("[INFO]  Replying to %+v with %+v", m.Reply, string(responseblock))
	err = nconn.Publish(m.Reply, responseblock)
	if err != nil {
		log.Printf("[ERROR] Replying Error [%v]: %+v", myreq.ID, err)
		str, _ := json.Marshal(err)
		nconn.Publish("error", str)
	}
}
*/
func main() {
	client = &http.Client{}
	var urls = flag.String("s", nats.DefaultURL, "The nats server URLs (separated by comma)")
	var showTime = flag.Bool("t", false, "Display timestamps")
	var workerCount = flag.Int("w", 1, "Worker Count")
	log.SetFlags(0)
	flag.Usage = usage
	flag.Parse()

	args := flag.Args()
	if len(args) < 2 {
		usage()
	}

	nc, err := nats.Connect(*urls)
	if err != nil {
		log.Fatalf("Can't connect: %v\n", err)
	}
	workername, err = os.Hostname()
	if err != nil {
		log.Fatalf("Can't get system hostname: %v\n", err)
	}
	subj, queue, i := args[0], args[1], 0
	jobchan := make(chan JobReq, 8096)
	for w := 1; w <= *workerCount; w++ {
		log.Printf("Launching worker %d", w)
		go workerTask(w, jobchan, nc)
	}
	nc.QueueSubscribe(subj, queue, func(m *nats.Msg) {
		i++
		log.Printf("[#%d] Received on [%s] Queue[%s] Pid[%d]: '%s'\n", i, m.Subject, m.Sub.Queue, os.Getpid(), string(m.Data))
		myreq := HTTPRequest{}
		err := json.Unmarshal(m.Data, &myreq)
		if err != nil {
			log.Printf("[ERROR] Invalid HTTP Request: %+v", err)
			str, _ := json.Marshal(err)
			nc.Publish("error", str)
		}
		jobreq := JobReq{HTTPReq: myreq, Reply: m.Reply}
		jobchan <- jobreq

		//printMsg(msg, i, nc)
	})

	log.Printf("Listening on [%s]\n", subj)
	if *showTime {
		log.SetFlags(log.LstdFlags)
	}

	runtime.Goexit()
}
