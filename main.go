package main

import (
	"math/rand"
	"time"

	log "github.com/Sirupsen/logrus"
	r "github.com/dancannon/gorethink"
	"github.com/namsral/flag"
	"stablelib.com/v1/uniuri"
)

var (
	rethinkdbPort *string
	rethinkdbHost *string
	env           *string
	tmpTable      *string
	tables        = []string{
		"accounts",
		"addresses",
		"contacts",
		"emails",
		"files",
		"keys",
		"labels",
		"onboarding",
		"threads",
		"tokens",
		"webhooks"}
)

func init() {
	rand.Seed(time.Now().Unix())

	rethinkdbHost = flag.String("rethinkdb_host", "localhost", "RethinkDB hostname or IP")
	rethinkdbPort = flag.String("rethinkdb_port", "12345", "Port to connect to RethinkDB")
	env = flag.String("env", "prod", "Application environment - dev/staging/prod")
	// rethinkdbHost = flag.String("rethinkdb_host", "localhost", "RethinkDB hostname or IP")
	// rethinkdbPort = flag.String("rethinkdb_port", "28015", "Port to connect to RethinkDB")
	// env = flag.String("env", "dev", "Application environment - dev/staging/prod")
	tmpTable = flag.String("tmp_table", "tmp_"+uniuri.New(), "Table to save temporary results")
	flag.Parse()
}

func main() {
	// Connecting to the database
	addr := *rethinkdbHost + ":" + *rethinkdbPort
	log.Info("Connecting to RethinkDB at ", addr)
	sess, err := r.Connect(r.ConnectOpts{
		Address:  addr,
		Database: *env,
	})
	if err != nil {
		log.Fatal(err.Error())
	}
	defer sess.Close()

	_, err = r.Db(*env).TableCreate(tmpTable).RunWrite(sess)
	if err != nil {
		log.Fatal("Couldn't create temp table. ", err.Error())
	}
	log.Info("Created temp table ", *tmpTable, " in db ", *env)

	// init tmp table with accounts
	// process array of table names (i.e. emails, files, etc.)

	_, err = r.Db(*env).TableDrop(*tmpTable).RunWrite(sess)
	if err != nil {
		log.Fatal("Couldn't drop temp table. ", err.Error())
	}
	log.Info("Dropped temp table ", *tmpTable, " in db ", *env)
}
