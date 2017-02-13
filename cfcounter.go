package main

import (
	"flag"
	"fmt"
	"github.com/gizak/termui"
	"github.com/gocql/gocql"
	"log"
	"math"
	_ "runtime"
	"strings"
	"sync"
	"time"
)

var (
	TOKEN_MIN = int64(-9223372036854775808) // cql murmur3 hash minimum value, - (2 ** 63)
	TOKEN_MAX = int64(9223372036854775807)  // cql murmur3 hash maximum value, 2 ** 63 -1

	CQL_CLUSTER *gocql.ClusterConfig

	CQL_KEYSPACE         *string
	CQL_COLUMNFAMILY     *string
	CQL_PRIMARYKEYCOLUMN *string

	NumWorkers    *int
	NumPartitions *int

	TOTAL_COUNT       = int64(0)
	TOTAL_COUNT_RLOCK = sync.Mutex{}

	WorkersWaitGroup sync.WaitGroup

	// counters
	FinishedTasks int64
	EnqueuedTasks int64

	// a buffered channel to send work requests
	TokenQueue chan TOKEN_RANGE
	UIFinished chan bool
)

// this uses the dispatcher pattern described in http://marcio.io/2015/07/handling-1-million-requests-per-minute-with-golang/

// the struct type that describes the job payload
type TOKEN_RANGE struct {
	MIN_RANGE int64
	MAX_RANGE int64

	RANGE_SIZE int64

	quit bool
}

// Worker that executes the job
type Worker struct {
	WorkerPool   chan chan TOKEN_RANGE
	TokenChannel chan TOKEN_RANGE
}

func NewWorker(workerPool chan chan TOKEN_RANGE) Worker {
	return Worker{
		WorkerPool:   workerPool,
		TokenChannel: make(chan TOKEN_RANGE),
	}
}

type Dispatcher struct {
	WorkerPool chan chan TOKEN_RANGE
}

func NewDispatcher() *Dispatcher {
	pool := make(chan chan TOKEN_RANGE, *NumWorkers)
	return &Dispatcher{WorkerPool: pool}
}

func (d *Dispatcher) Run() {
	for i := 0; i < *NumWorkers; i++ {
		worker := NewWorker(d.WorkerPool)
		worker.Start()
	}
	go d.dispatch()
}

func (d *Dispatcher) dispatch() {
	for {
		select {
		case token_range := <-TokenQueue:
			// we don't use a goroutine here since we want to block inserts when there is no available worker
			tokenChannel := <-d.WorkerPool
			tokenChannel <- token_range
		}
	}
}

func (w Worker) Start() {
	go func() {
		//log.Println("Starting a worker...")
		session, _ := CQL_CLUSTER.CreateSession()
		defer session.Close()
		query_stmt := session.Query(fmt.Sprintf("SELECT COUNT(*) FROM %v.%v WHERE token(%v) >= ? AND token(%v) < ?",
			*CQL_KEYSPACE, *CQL_COLUMNFAMILY,
			*CQL_PRIMARYKEYCOLUMN, *CQL_PRIMARYKEYCOLUMN))
		defer query_stmt.Release()
		defer WorkersWaitGroup.Done()
		for {
			w.WorkerPool <- w.TokenChannel
			select {
			case token_range := <-w.TokenChannel:
				if token_range.quit {
					return
				}
				token_range_count := GetCountsFromTokenRange(query_stmt, token_range)
				if token_range_count > 0 {
					TOTAL_COUNT_RLOCK.Lock()
					TOTAL_COUNT += token_range_count
					TOTAL_COUNT_RLOCK.Unlock()
				}
				FinishedTasks += 1
			}
		}
	}()
}

func GetCountsFromTokenRange(query_stmt *gocql.Query, token_range TOKEN_RANGE) int64 {
	for range_size := token_range.RANGE_SIZE; range_size > 0; range_size = range_size / 2 {
		range_count := int64(0)
		has_error := false
		for range_value := int64(0); range_value < token_range.RANGE_SIZE; range_value += range_size {
			start_range := int64(math.Min((float64(token_range.MIN_RANGE) + float64(range_value)), float64(token_range.MAX_RANGE)))
			end_range := int64(math.Min((float64(start_range) + float64(range_size)), float64(token_range.MAX_RANGE)))

			// deal with cases where the numerator of range_size/2 is an even number
			if start_range == end_range {
				continue
			}

			query_stmt.Bind(start_range, end_range)

			this_count := int64(0)

			if err := query_stmt.Scan(&this_count); err != nil {
				//log.Println("Running start_range: ", start_range, "end_range: ", end_range, " range_size: ", range_size, " range_value: ", range_value, " count: ", this_count, " query_stmt: ", query_stmt, " err: ", err, " runtime.NumGoroutine: ", runtime.NumGoroutine(), " FinishedTasks: ", FinishedTasks, " EnqueuedTasks: ", EnqueuedTasks)
				has_error = true
				break
			} else {
				//log.Println("Running start_range: ", start_range, "end_range: ", end_range, " range_size: ", range_size, " range_value: ", range_value, " count: ", this_count, " query_stmt: ", query_stmt, " runtime.NumGoroutine: ", runtime.NumGoroutine(), " FinishedTasks: ", FinishedTasks, " EnqueuedTasks: ", EnqueuedTasks)
				range_count += this_count
			}

		}
		if has_error {
			continue
		}
		return range_count
	}
	return int64(0)
}

func InitTermUI() {
	// initialize termui
	err := termui.Init()
	if err != nil {
		log.Fatal(err)
	}

	go func() {
		startTime := time.Now()

		ft := termui.NewPar("")
		ft.BorderLabel = "Finished Tasks"
		ft.Height = 3

		et := termui.NewPar("")
		et.BorderLabel = "Enqueued Tasks"
		et.Height = 3

		par := termui.NewPar("")
		par.BorderLabel = "Partitions"
		par.Height = 3

		elapsedTime := termui.NewPar("")
		elapsedTime.BorderLabel = "Elapsed Time"
		elapsedTime.Height = 3

		b := termui.NewGauge()
		b.Width = 100
		b.Height = 3
		b.BorderLabel = "Completed"
		b.BarColor = termui.ColorRed
		b.BorderFg = termui.ColorWhite

		cc := termui.NewPar("")
		cc.BorderLabel = "Current Counts"
		cc.Height = 3

		termui.Body.AddRows(
			termui.NewRow(
				termui.NewCol(3, 0, ft),
				termui.NewCol(3, 0, et),
				termui.NewCol(3, 0, par),
				termui.NewCol(3, 0, elapsedTime),
			),
			termui.NewRow(
				termui.NewCol(8, 0, b),
				termui.NewCol(8, 0, cc),
			),
		)

		draw := func() {
			ft.Text = fmt.Sprintf("%v", FinishedTasks)
			et.Text = fmt.Sprintf("%v", EnqueuedTasks)
			par.Text = fmt.Sprintf("%v", *NumPartitions)
			elapsedTime.Text = time.Since(startTime).String()

			if EnqueuedTasks != 0 {
				b.Percent = int(float64(FinishedTasks) / float64(*NumPartitions) * 100.0)
			} else {
				b.Percent = 0
			}
			cc.Text = fmt.Sprintf("%v", TOTAL_COUNT)

			// calculate layout
			termui.Body.Align()
			termui.Render(termui.Body)

		}

		termui.Handle("/sys/kbd/q", func(termui.Event) {
			termui.StopLoop()
		})

		termui.Handle("/sys/kbd/C-c", func(termui.Event) {
			termui.StopLoop()
		})

		termui.Handle("/timer/1s", func(e termui.Event) {
			draw()

			if int(FinishedTasks) >= *NumPartitions {
				termui.StopLoop()
			}
		})

		termui.Loop()
		termui.Close()

		// hack to retain the UI display
		// without termui.Close() then termui.Init(), the output is mangled
		termui.Init()
		draw()
		// sleep a short while for it to finish the last render
		time.Sleep(2000 * time.Millisecond)

		UIFinished <- true
	}()

}

func main() {
	// parse flags
	CassandraAddresses := flag.String("CassandraAddresses", "127.0.0.1,127.0.0.2,127.0.0.3", "Cassandra cluster addresses, multiple addresses seperated by comma")
	CQL_KEYSPACE = flag.String("Keyspace", "default", "Keyspace to use")
	CQL_COLUMNFAMILY = flag.String("ColumnFamily", "cf", "ColumnFamily")
	CQL_PRIMARYKEYCOLUMN = flag.String("PrimaryKey", "id", "Primary Key column")
	NumWorkers = flag.Int("NumWorkers", 10, "Number of Workers")
	NumPartitions = flag.Int("NumPartitions", 10240, "Number of Partitions")

	flag.Parse()

	// initialize variables from flags
	// parse address
	var USE_CassandraAddresses []string
	for _, _addr := range strings.Split(*CassandraAddresses, ",") {
		USE_CassandraAddresses = append(USE_CassandraAddresses, _addr)
	}

	num_partitions := int64(*NumPartitions)
	WorkersWaitGroup.Add(*NumWorkers)

	// connect to the cluster
	CQL_CLUSTER = gocql.NewCluster(USE_CassandraAddresses...)
	CQL_CLUSTER.ProtoVersion = 4
	CQL_CLUSTER.Keyspace = *CQL_KEYSPACE
	CQL_CLUSTER.Consistency = gocql.Quorum
	CQL_CLUSTER.NumConns = 10

	token_range_size := int64(math.Ceil((float64(TOKEN_MAX) - float64(TOKEN_MIN)) / float64(num_partitions)))

	//log.Println("TOKEN_MIN: ", TOKEN_MIN, " TOKEN_MAX: ", TOKEN_MAX)
	//log.Println("token_range_size: ", token_range_size)

	// initialize queue
	TokenQueue = make(chan TOKEN_RANGE, 100)
	UIFinished = make(chan bool, 1)

	// initialize dispatchers
	dispatcher := NewDispatcher()
	dispatcher.Run()

	// initialize variables
	FinishedTasks, EnqueuedTasks = int64(0), int64(0)

	InitTermUI()

	// add token_ranges to channel
	for i := float64(TOKEN_MIN); i < float64(TOKEN_MAX); i += float64(token_range_size) {
		//log.Println("start_range: ", i, " end_range: ", i+token_range_size)
		token_range := TOKEN_RANGE{MIN_RANGE: int64(i),
			MAX_RANGE:  int64(i + float64(token_range_size)),
			RANGE_SIZE: token_range_size,
			quit:       false}
		TokenQueue <- token_range
		EnqueuedTasks += 1
	}

	for i := 0; i < *NumWorkers; i++ {
		token_range := TOKEN_RANGE{MIN_RANGE: 0, MAX_RANGE: 0, RANGE_SIZE: 0, quit: true}
		TokenQueue <- token_range
	}

	WorkersWaitGroup.Wait()
	<-UIFinished

	log.Printf("Total number of rows in %v.%v: %v", *CQL_KEYSPACE, *CQL_COLUMNFAMILY, TOTAL_COUNT)
}
