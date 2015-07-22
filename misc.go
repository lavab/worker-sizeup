// Ignore this file, or else
package main

import (
	log "github.com/Sirupsen/logrus"
	r "github.com/dancannon/gorethink"
	"github.com/gonum/plot"
	"github.com/gonum/plot/plotter"
	"github.com/gonum/plot/plotutil"
	"github.com/gonum/plot/vg"
)

func tableSize(sess *r.Session, table string) uint64 {
	cursor, err := r.Table(table).Map(func(row r.Term) interface{} {
		return row.CoerceTo("string").CoerceTo("binary").Count()
	}).Reduce(func(left, right r.Term) interface{} {
		return left.Add(right)
	}).Run(sess)
	if err != nil {
		log.Fatal(err.Error())
	}
	defer cursor.Close()

	var out uint64
	if err := cursor.One(&out); err != nil {
		log.Fatal("Wrong type when unfolding cursor")
	}
	return out
}

func plotTableSizes(sess *r.Session) {
	sizes := []float64{}
	for _, t := range tables {
		sizes = append(sizes, float64(averageDocumentSize(sess, t)))
	}

	p, _ := plot.New()
	p.Title.Text = "Average document sizes"
	p.Y.Label.Text = "Size in bytes"

	w := vg.Points(20)
	bars, _ := plotter.NewBarChart(plotter.Values(sizes), w)
	bars.Color = plotutil.Color(0)
	bars.LineStyle.Width = vg.Length(0)

	p.Add(bars)
	p.NominalX(tables...)

	p.Save(6*vg.Inch, 6*vg.Inch, "avg_doc_sizes.png")
}

func averageDocumentSize(sess *r.Session, table string) uint64 {
	var count uint64
	cur, _ := r.Db(*env).Table(table).Count().Run(sess)
	cur.One(&count)
	return tableSize(sess, table) / count
}
