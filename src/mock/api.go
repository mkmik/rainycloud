package main

import (
	"web"
	"fmt"
	"json"
	"time"
	"rand"
	"flag"
)

////////////


type JobStatus string

const (
	QUEUED JobStatus = "WAITING"
	RUNNING = "RUNNING"
	DONE = "DONE"
	FAILED = "FAILED"
)

type Job struct {
	Id string
	Status JobStatus
	Completion float64
	Metrics map[string]Metric
	NWorkers int
}

type Metric	interface {}

type LoadMetric struct {
	ResID string "resId"
	Value float32 "value"
}


const (
	MAX = 1650703652
)

//
func UpdateMetrics(m map[string]Metric, increment float64, workers int) {

	now := time.Nanoseconds()/1000000
	lastTp, ok := m["throughput"].([2]int64)
	if !ok {
		panic(fmt.Sprintf("cannot cast '%s' to throughput", m["throughput"]))
	}

	last := lastTp[0]
	delta := now - last

	absIncrement := MAX * int64(increment) / 100
	
	
	m["throughput"] = [...]int64{now, absIncrement/delta * 1000}

	loads := make([]LoadMetric, workers)
	for i, _ := range loads {
		loads[i] = LoadMetric{fmt.Sprintf("W%d", i), 80.0+rand.Float32()*10}
	}
	m["load"] = loads

}

func ResetThroughput(m map[string]Metric) {
	m["throughput"] = [...]int64{time.Nanoseconds()/1000000, 0}
}

func (this *Job) Run() {
	fmt.Printf("Running job %s\n", this.Id)
	this.Status = RUNNING

	ResetThroughput(this.Metrics)

	for {
		time.Sleep(1e9)

		maxPercent := *maxPercentParam * float64(this.NWorkers)
		if this.Completion >80 {
			maxPercent = 0
		} else if this.Completion < 10 {
			maxPercent = maxPercent / 2
		}

		increment := *basePercentParam + rand.Float64()*maxPercent

		comp := this.Completion + increment
		if comp > 100 {
			comp = 100
		}

		UpdateMetrics(this.Metrics, increment, this.NWorkers)
		
		this.Completion = comp

		fmt.Printf("Progressing %s: %d%%\n", this.Id, int(this.Completion))
		
		if comp >= 100 {
			this.Status = "DONE"
			ResetThroughput(this.Metrics)
			break
		}
	}
}

func Schedule(job *Job) {
	fmt.Printf("Scheduling job %s\n", job.Id)

	go func() {
		time.Sleep((10 + rand.Int63n(30)) * 1e8)
		go job.Run()
	}()
}

////////////

type JobRequest struct {
	NWorkers int "nWorkers"
	HspecTableName TableReference "hspecDestinationTableName"
}

type TableReference struct {
	JdbcUri string "jdbcUrl"
	TableName string "tableName"
}

func (self TableReference) String() string {
	return fmt.Sprintf("%s ! %s", self.JdbcUri, self.TableName)
}

type JobResponse struct {
	Id string "id"
	Error *SubmissionError "error"
}

type SubmissionError struct {
	Code int "code"
	Description string "description"
}

type JobStatusResponse struct {
	Id string "id"
	Status JobStatus "status"
	Completion float64 "completion"
	Metrics map[string]Metric "metrics"
}


func (self Handler) ApiSubmit(ctx *web.Context) string {
	var request JobRequest

	println("Got request: ", string(ctx.Request.ParamData))

	e := json.Unmarshal(ctx.Request.ParamData, &request)

	if e != nil {
		println("got unmarshal error: ", e.String())
	}

	fmt.Printf("request: %s\n", request.HspecTableName)

	id, e := self.registry.Submit(request)
	if e != nil {
		println("got submit error: ", e.String())
	}

	ctx.ContentType("application/json")
	job := &JobResponse{id, nil}
	m, _ := json.Marshal(job)
	return string(m)
}

func (self Handler) getStatus(id string) *JobStatusResponse {
	job, e := self.registry.Get(id)
	if e != nil {
		return &JobStatusResponse{id, "ERROR", 100, nil}
//		panic(e.String())
	}
	return &JobStatusResponse{job.Id, job.Status, job.Completion, job.Metrics}
}

func (self Handler) ApiStatus(ctx *web.Context, id string) string {
	ctx.ContentType("application/json")

	status := self.getStatus(id)

	m, _ := json.Marshal(status)
	return string(m)
}

func (self Handler) ApiList(ctx *web.Context) string {
	ctx.ContentType("application/json")
	
	res := make([]map[string]string, 0, 100)

	list, _ := self.registry.List()
	for _, j := range list {
		res = append(res, map[string] string{"id": j.Id, "status": string(j.Status)})
	}

	m, _ := json.Marshal(res)
	return string(m)	
}

type Handler struct {
	registry JobRegistry
}

func NewHandler() Handler {
	return Handler{*NewJobRegistry()}
}

func spawn(port int) {
	var s web.Server

	handler := NewHandler()

	s.Post("/api/submit", web.MethodHandler(handler, "ApiSubmit"))
	s.Get("/api/status/(.*)", web.MethodHandler(handler, "ApiStatus"))
	s.Get("/api/list", web.MethodHandler(handler, "ApiList"))
	go func() { s.Run(fmt.Sprintf("0.0.0.0:%d", port))}()
}

var maxPercentParam *float64
var basePercentParam *float64

func main() {
	maxPercentParam = flag.Float64("maxPercent", 0.5, "max speed in completion percent per second")
	basePercentParam = flag.Float64("basePercent", 0.1, "base speed in completion percent per second (never slower than that)")

	flag.Parse()

	spawn(5941)
	spawn(5942)
	spawn(5943)

	c:=make(chan int)
	<-c
}
