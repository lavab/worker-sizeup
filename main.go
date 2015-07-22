package main

import (
	"fmt"
	"math/rand"
	"time"

	log "github.com/Sirupsen/logrus"
	r "github.com/dancannon/gorethink"
	"github.com/namsral/flag"
	"stablelib.com/v1/uniuri"
)

const (
	accountsTable = "accounts"
)

var (
	rethinkdbPort *string
	rethinkdbHost *string
	env           *string
	tmpTable      *string
	bigTables     = []string{"emails", "files"}
)

func init() {
	rand.Seed(time.Now().Unix())

	rethinkdbHost = flag.String("rethinkdb_host", "localhost", "RethinkDB hostname or IP")
	rethinkdbPort = flag.String("rethinkdb_port", "28015", "Port to connect to RethinkDB")
	env = flag.String("env", "dev", "Application environment - dev/staging/prod")
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
	for _, t := range bigTables {
		processTable(sess, t)
	}

	_, err = r.Db(*env).TableDrop(*tmpTable).RunWrite(sess)
	if err != nil {
		log.Fatal("Couldn't drop temp table. ", err.Error())
	}
	log.Info("Dropped temp table ", *tmpTable, " in db ", *env)
}

func processTable(sess *r.Session, table string) {
	rows, err := r.Db(*env).Table(table).Run(sess)
	if err != nil {
		log.Fatal("Couldn't fetch rows for table ", table)
	}
	defer rows.Close()

	var doc map[string]interface{}
	var size uint64
	var id string
	var ok bool

	// WIP
	for rows.Next(&doc) {
		if size, ok = doc["size"].(uint64); !ok {
			if id, ok = doc["id"].(string); !ok {
				log.Warn("Found a document without ID! ", doc)
				continue
			}
			sizeTerm, err := r.Db(*env).Table(table).Get(id).
				CoerceTo("string").CoerceTo("binary").Count().Run(sess)
			err = sizeTerm.One(&size)
			if err != nil {
				log.Warn("Couldn't compute the size of document ", id, ". It might've been deleted.", err)
				continue
			}
		}
		fmt.Println(doc)
		return
	} else {

	}
}
