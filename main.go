package main

import (
	"fmt"

	log "github.com/Sirupsen/logrus"
	r "github.com/dancannon/gorethink"
	"github.com/dustin/go-humanize"
	"github.com/namsral/flag"
)

var (
	rethinkdbPort *string
	rethinkdbHost *string
	rethinkdbSess *r.Session
	env           *string
)

func init() {
	rethinkdbHost = flag.String("rethinkdb_host", "localhost", "RethinkDB hostname or IP")
	rethinkdbPort = flag.String("rethinkdb_port", "28015", "Port to connect to RethinkDB")
	env = flag.String("env", "dev", "Application environment - dev/staging/prod")
	flag.Parse()
}

func main() {
	// Connecting to the database
	addr := *rethinkdbHost + ":" + *rethinkdbPort
	log.Info("Connecting to RethinkDB at ", addr)
	rethinkdbSess, err := r.Connect(r.ConnectOpts{
		Address: addr,
	})
	if err != nil {
		log.Fatal(err.Error())
	}
	defer rethinkdbSess.Close()

	// Computing the amount of emails
	cursor, err := r.Db("prod").Table("emails").Map(func(row r.Term) interface{} {
		return row.CoerceTo("string").CoerceTo("binary").Count()
	}).Reduce(func(left, right r.Term) interface{} {
		return left.Add(right)
	}).Run(rethinkdbSess)
	if err != nil {
		log.Fatal(err.Error())
	}
	defer cursor.Close()

	var x uint64
	if err := cursor.One(&x); err != nil {
		log.Fatal("Wrong type when unfolding cursor")
	}
	fmt.Println(humanize.Bytes(x))
}
