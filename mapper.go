package mapper

import (
	"encoding/csv"
	"io"
	"io/ioutil"
	"log"
	"os"
	"runtime/debug"

	"github.com/smartystreets/mafsa"
)

// Mapper maps string keys to string values with a low memory footprint.  It should be suitable for
// very large datasets
type Mapper struct {
	mt   *mafsa.MinTree
	vals []string
	From string
	To   string
}

// Get returns a value for a key, and a boolean indicating whether the key was found.
func (m Mapper) Get(key string) (value string, ok bool) {
	_, pos := m.mt.IndexedTraverse([]rune(key))
	if pos < 0 {
		return // returns zero values for value ("") and ok (false)
	}
	ok = true
	value = m.vals[pos-1]
	return
}

// NewMapperFromCSV takes an io.Reader of CSV data and returns a Mapper ready for use.
// The CSV data must be in UTF-8 and have only two fields with a header row. The resulting
// Mapper will map values from the first column to values in the second column.  The data
// after the header row must be sorted lexographically by the first column.
func New(csvData io.Reader) (m *Mapper, err error) {
	m = &Mapper{}
	r := csv.NewReader(csvData)
	r.LazyQuotes = true
	r.FieldsPerRecord = 2
	var row []string
	row, err = r.Read()
	if err != nil {
		log.Fatalln(err)
	}
	m.From = row[0]
	m.To = row[1]
	bt := mafsa.New()
	i := 0
	for {
		i++
		row, err = r.Read()
		if err != nil {
			if err == io.EOF {
				break
			}
			return
		}
		bt.Insert(row[0])
		m.vals = append(m.vals, row[1])
	}
	bt.Finish()
	tmp, err := ioutil.TempFile("", "mafsa")
	if err != nil {
		return
	}
	// defer statemenets are executed in LIFO order
	defer os.Remove(tmp.Name())
	defer tmp.Close()
	var data []byte
	data, err = bt.MarshalBinary()
	if err != nil {
		return
	}
	_, err = tmp.Write(data)
	if err != nil {
		return
	}
	bt = nil
	data = nil
	_, err = tmp.Seek(0, 0)
	if err != nil {
		return
	}
	// release the memory for the BuildTree before we load the MinTree
	debug.FreeOSMemory()
	m.mt, err = new(mafsa.Decoder).ReadFrom(tmp)
	return
}
