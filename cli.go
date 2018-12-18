package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"io/ioutil"
	log "logger"
	"os"
	"pqsclient"
	"time"
)

//ResultSet .....
type ResultSet struct {
	Source  string
	Columns []interface{}
	Rows    []interface{}
}

func prepareQuery(function pqsclient.ActionType, statement, user, destip string, output chan string) {
	query := &pqsclient.QueryHandler{}
	query.Event = function
	query.Data = pqsclient.Request{Query: statement, User: user}

	client := pqsclient.Client{
		Url:   fmt.Sprintf("http://%s:8111", destip),
		Debug: true,
	}

	log.Noticef("Post Query Start Executed at %s", time.Now())
	err := client.Query(query)
	if err != nil {
		fmt.Println("Error happend, ", err)
		output <- err.Error()
	}

	output <- client.LastResponse

}

func colWriter(w []interface{}) []byte {
	var out []byte
	for _pos, _val := range w {
		if _pos != len(w)-1 {
			out = append(out, []byte(fmt.Sprintf("%v|", _val))...)
			// fmt.Printf("%s|", _val)
		} else {
			out = append(out, []byte(fmt.Sprintf("%v\n", _val))...)
			// fmt.Printf("%s\n", _val)
		}
	}
	return out
}

func main() {
	// defer profile.Start().Stop()
	var (
		destip      string
		username    string
		querystring string
		queryfile   string
		destfile    string

		store  bool
		header bool
	)
	flag.StringVar(&destip, "destip", "localhost", "destination IP for paques server")
	flag.StringVar(&username, "user", "administrator", "user name , default administrator")
	flag.StringVar(&querystring, "querystring", "", "query string provided")
	flag.StringVar(&queryfile, "queryfile", "", "file where query strings sits")
	flag.StringVar(&destfile, "destfile", "", "destination file for saving result")

	flag.BoolVar(&store, "store", false, "Identify if query to be store of not")
	flag.BoolVar(&header, "header", false, "use header in csv result ")

	flag.Parse()

	if querystring == "" && queryfile == "" {
		log.Notice("Please specify file with query string, for example -querystring='search from file xxxx into b' or query file -queryfile==/tmp/qry.qr")
		flag.PrintDefaults()
		os.Exit(1)
	}

	if store == true && destfile == "" {
		log.Notice("Please specify destination file to store result, for example -destfile=/tmp/file.csv")
		flag.PrintDefaults()
		os.Exit(1)
	}

	url := fmt.Sprintf("http://%s:8111?event=stream&quid=", destip)
	serverstat := make(chan string, 1)

	getres := make(chan []byte, 10)
	done := make(chan bool)
	var query string
	if querystring != "" {
		query = string(querystring)
	} else {
		fq, err := os.Open(queryfile)
		if err != nil {
			panic(err)
		}
		q, err := ioutil.ReadAll(fq)
		if err != nil {
			panic(err)
		}
		query = string(q)
	}
	log.Noticef("query sets: %s", query)
	//post query implementation
	evt := pqsclient.ActionTypeQuery
	// query = string(querystring)
	// user = username
	strtime := time.Now()
	go prepareQuery(evt, query, username, destip, serverstat)

	serverstatus := <-serverstat
	var data pqsclient.QueryResults

	errjson := json.Unmarshal([]byte(serverstatus), &data)
	if errjson != nil {
		os.Exit(1)
	}
	if data.Event == "error" {
		log.Noticef("Error happened: %s", data.Data.(map[string]interface{})["message"])
	} else {
		body := data.Data.(map[string]interface{})
		qresp := body["body"].(map[string]interface{})
		tgturl := fmt.Sprintf("%s%s", url, qresp["quid"])

		go pqsclient.ParseStream(tgturl, getres, done)

		if store == true {
			head := 0
			f, err := os.Create(destfile)
			if err != nil {
				fmt.Println("cannot crete file")
				panic(err)
			}
			for v := range getres {
				err := json.Unmarshal(v, &data)
				if err != nil {
					os.Exit(1)
				}

				if data.Event == "data" {
					rset := ResultSet{}
					res := data.Data.(map[string]interface{})
					rsets := res["rset"].(map[string]interface{})

					rset.Rows = rsets["rows"].([]interface{})
					rset.Source = rsets["source"].(string)
					rset.Columns = rsets["columns"].([]interface{})
					if header == true {
						if head == 0 {
							aa := colWriter(rset.Columns)
							head++
							// fmt.Print(fmt.Sprintf("%s", aa))
							f.Write(aa)
						}

					}

					for _, v := range rset.Rows {
						_v := v.([]interface{})
						bb := colWriter(_v)
						// fmt.Print(fmt.Sprintf("%s", bb))
						f.Write(bb)
					}
				}
			}
			f.Sync()
			f.Close()
		} else {
			head := 0
			for v := range getres {
				err := json.Unmarshal(v, &data)
				if err != nil {
					os.Exit(1)
				}
				if data.Event == "data" {
					rset := ResultSet{}
					res := data.Data.(map[string]interface{})
					rsets := res["rset"].(map[string]interface{})

					rset.Rows = rsets["rows"].([]interface{})
					rset.Source = rsets["source"].(string)
					rset.Columns = rsets["columns"].([]interface{})
					if header == true {
						if head == 0 {
							aa := colWriter(rset.Columns)
							head++
							fmt.Print(fmt.Sprintf("%s", aa))
						}
					}

					for _, v := range rset.Rows {
						_v := v.([]interface{})
						bb := colWriter(_v)
						fmt.Print(fmt.Sprintf("%s", bb))
					}
				}
			}
		}
	}

	endtime := time.Now()
	log.Noticef("Query Process Ended at %s", endtime)
	duration := endtime.Sub(strtime)
	log.Noticef("Process Duration is %s", duration)
}
