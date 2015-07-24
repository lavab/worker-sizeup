package main

import (
	"bufio"
	"fmt"
	"math/rand"
	"os"
	"time"

	"stablelib.com/v1/uniuri"

	log "github.com/Sirupsen/logrus"
	r "github.com/dancannon/gorethink"
	"github.com/namsral/flag"
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

	interactive *bool
)

func init() {
	rand.Seed(time.Now().Unix())

	rethinkdbHost = flag.String("rethinkdb_host", "localhost", "RethinkDB hostname or IP")
	rethinkdbPort = flag.String("rethinkdb_port", "28015", "Port to connect to RethinkDB")
	env = flag.String("env", "dev", "Application environment - dev/staging/prod")
	flag.Parse()
}

func setupTmpTable(sess *r.Session, tmpTable string) {
	log.Info("Copying fields {id, size=0} from table 'accounts' to table ", tmpTable, "...")
	log.Info("Starting setting up the tmp table.")
	startTime := time.Now()
	res, err := r.Db(*env).Table("accounts").Field("id").ForEach(func(account r.Term) interface{} {
		return r.Db(*env).Table(tmpTable).Insert(map[string]interface{}{
			"id":   account,
			"size": 0,
		})
	}).RunWrite(sess)
	if err != nil {
		log.WithFields(map[string]interface{}{"error": err.Error()}).Fatal("Setting up tmp table failed.")
	}
	log.WithFields(map[string]interface{}{
		"sttDuration": time.Now().Sub(startTime),
		"sttTable":    tmpTable,
		"sttResponse": res,
	}).Info("Finished setting up the tmp table.")
}

func main() {
	sess := dbConnect(*rethinkdbHost + ":" + *rethinkdbPort)
	tmpTable := "tmp_" + uniuri.New()

	tableCreate(sess, *env, tmpTable)
	defer func() {
		tableDrop(sess, *env, tmpTable)
		sess.Close()
	}()

	setupTmpTable(sess, tmpTable)

	fmt.Println("Done. Enter anything to end.")
	bufio.NewReader(os.Stdin).ReadString('\n')
}

func dbConnect(addr string) *r.Session {
	log.Info("Connecting to RethinkDB at ", addr)
	sess, err := r.Connect(r.ConnectOpts{Address: addr})
	if err != nil {
		log.Fatal(err.Error())
	}
	return sess
}

func tableCreate(sess *r.Session, db string, table string) {
	log.Info("Creating temp table ", table, " in db ", db, "...")
	_, err := r.Db(db).TableCreate(table).RunWrite(sess)
	if err != nil {
		log.Fatal("Couldn't create temp table. ", err.Error())
	}
}

func tableDrop(sess *r.Session, db string, table string) {
	log.Info("Dropping temp table ", table, " in db ", db, "...")
	_, err := r.Db(db).TableDrop(table).RunWrite(sess)
	if err != nil {
		log.Fatal("Couldn't drop temp table. ", err.Error())
	}
}

// Not using WaitGroup because len(tables) might be too high for gorethink to handle comfortably (?)
func processMultipleTablesConcurrently(sess *r.Session, tables []string, tableModifier func(*r.Session, string)) {
	nWorkers := 4
	jobs := make(chan string, len(tables))
	done := make(chan struct{})

	for i := 0; i < nWorkers; i++ {
		go func(<-chan string, chan<- struct{}) {
			for table := range jobs {
				tableModifier(sess, table)
			}
			done <- struct{}{}
		}(jobs, done)
	}

	for _, t := range tables {
		jobs <- t
	}
	close(jobs)

	for w := 0; w < nWorkers; w++ {
		<-done
		fmt.Println("Done!")
	}
}
