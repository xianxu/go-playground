package main

import (
	"flag"
	"fmt"
	"github.com/xianxu/gassandra"
	"github.com/xianxu/gostrich"
	"github.com/xianxu/rpcx"
	"log"
	"net/http"
	"net/url"
	"strings"
	"time"
)

var (
	readTimeout      = flag.Duration("read_timeout", 10*time.Second, "Read timeout")
	writeTimeout     = flag.Duration("write_timeout", 10*time.Second, "Write timeout")
	binding          = flag.String("binding", ":8888", "Binding to serve tweetbutton count traffic")
	cassandras       = flag.String("cassandras", "localhost:9160", "Cassandra hosts to use")
	cassandraTimeout = flag.Duration("cassandra_timeout", 3*time.Second, "Cassandra timeout")
	concurrency      = flag.Int("concurrency", 5, "How many Cassandra connection to open per Cassandra host")

	// intervals
	path = &gassandra.ColumnPath{"SUM_ALLTIME", nil, []byte{0, 0, 0, 0, 0, 0, 0, 0}}
)

type ServerState struct {
	countService rpcx.Service
}

// The actual tweetbutton count logic
func (s *ServerState) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if !strings.HasPrefix(r.URL.Path, "/1/urls/count.json") {
		w.WriteHeader(404)
		return
	}

	if err := r.ParseForm(); err != nil {
		w.WriteHeader(503)
		w.Write([]byte("Failed to parse variables"))
		return
	}

	if url := r.Form["url"]; url == nil || len(url) == 0 {
		w.WriteHeader(503)
		w.Write([]byte("required param missing"))
		return
	}

	// get first url param
	urlParam := r.Form["url"][0]

	// TODO: canonicalization, we have some complicated logic there...
	// science/src/java/com/twitter/url_canonicalizer/core/UrlCanonicalizerImpl.java
	url, err := url.Parse(urlParam)
	if err != nil {
		w.WriteHeader(503)
		w.Write([]byte("url is not well formed"))
		return
	}

	hostAndPort := strings.Split(url.Host, ":")
	host := strings.Split(hostAndPort[0], ".")
	hostLen := len(host)

	var d1, d2, d3 string
	if hostLen >= 1 {
		d1 = host[hostLen-1]
	}
	if hostLen >= 2 {
		d2 = host[hostLen-2]
	}
	if hostLen >= 3 {
		d3 = host[hostLen-3]
	}
	key := fmt.Sprintf("pru:p:%v:%v:%v:%v", d1, d2, d3, urlParam)

	req := rpcx.RpcReq{"get", &gassandra.CassandraGetRequest{
		Key:              []byte(key),
		ColumnPath:       path,
		ConsistencyLevel: gassandra.ConsistencyLevelOne,
	}}

	var rsp gassandra.CassandraGetResponse
	//TODO: timeout and cancel?
	err = s.countService.Serve(&req, &rsp, nil)

	if err != nil {
		w.WriteHeader(503)
		w.Write([]byte("failed to query count"))
		return
	}

	var count int64
	if rsp.Value != nil && rsp.Value.CounterColumn != nil {
		count = rsp.Value.CounterColumn.Value
	}

	w.WriteHeader(200)
	w.Write([]byte(fmt.Sprintf("{count:%v,url:\"%v\"}", count, url)))
	return
}

func makeSingle(host string, timeout time.Duration) gassandra.Keyspace {
	hostPort := host
	if !strings.Contains(host, ":") {
		hostPort = host + ":9160"
	}
	return gassandra.Keyspace{hostPort, "RB", timeout}
}
func makeAll(hostsString string, timeout time.Duration) []rpcx.ServiceMaker {
	hosts := strings.Split(hostsString, ",")

	ks := make([]rpcx.ServiceMaker, len(hosts))
	for i, host := range hosts {
		ks[i] = makeSingle(host, timeout)
	}

	return ks
}

func main() {
	flag.Parse()
	portOffset := *gostrich.PortOffset
	newBinding := gostrich.UpdatePort(*binding, portOffset)

	conf := rpcx.ReliableServiceConf{
		Name:        "tweetbutton",
		Makers:      makeAll(*cassandras, *cassandraTimeout),
		Concurrency: *concurrency,
		Stats:       gostrich.AdminServer().GetStats(),
	}
	cas := rpcx.NewReliableService(conf)

	state := ServerState{cas}

	server := http.Server{
		newBinding,
		&state,
		*readTimeout,
		*writeTimeout,
		0,
		nil,
	}
	log.Printf("Tweetbutton starting up...\n")
	go func() {
		log.Fatal(server.ListenAndServe())
	}()

	gostrich.StartToLive(nil)
}
