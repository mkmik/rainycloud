package main

import (
	"web"
	"fmt"
	"json"
	"time"
	"rand"
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
	Completion int
	Metrics map[string]Metric
}

type Metric	interface {}

type LoadMetric struct {
	ResID string "resId"
	Value float32 "value"
}


const (
	MAX = 1650703652
	WORKERS = 4
)

//
func UpdateMetrics(m map[string]Metric, increment int) {

	now := time.Nanoseconds()/1000000
	lastTp, ok := m["throughput"].([2]int64)
	if !ok {
		panic(fmt.Sprintf("cannot cast '%s' to throughput", m["throughput"]))
	}

	last := lastTp[0]
	delta := now - last

	absIncrement := MAX * int64(increment) / 100
	
	
	m["throughput"] = [...]int64{now, absIncrement/delta * 1000}
	m["load"] = [...]LoadMetric{LoadMetric{"W1", 51.5}, LoadMetric{"W2", 23.4}}

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

		maxPercent := 3		
		if this.Completion >80 {
			maxPercent = 0
		} else if this.Completion < 10 {
			maxPercent = 1
		}

		increment := 1 + rand.Intn(maxPercent)

		comp := this.Completion + increment
		if comp > 100 {
			comp = 100
		}

		UpdateMetrics(this.Metrics, increment)
		
		this.Completion = comp

		fmt.Printf("Progressing %s: %d%%\n", this.Id, this.Completion)
		
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
	Completion int "completion"
	Metrics map[string]Metric "metrics"
}


var registry JobRegistry = *NewJobRegistry()

func ApiSubmit(ctx *web.Context) string {
	var request JobRequest

	println("Got request: ", string(ctx.Request.ParamData))

	e := json.Unmarshal(ctx.Request.ParamData, &request)

	if e != nil {
		println("got unmarshal error: ", e.String())
	}

	fmt.Printf("request: %s\n", request.HspecTableName)

	id, e := registry.Submit(request)
	if e != nil {
		println("got submit error: ", e.String())
	}

	ctx.ContentType("application/json")
	job := &JobResponse{id, nil}
	m, _ := json.Marshal(job)
	return string(m)
}

func getStatus(id string) *JobStatusResponse {
	job, e := registry.Get(id)
	if e != nil {
		panic(e.String())
	}
	return &JobStatusResponse{job.Id, job.Status, job.Completion, job.Metrics}
}

func ApiStatus(ctx *web.Context, id string) string {
	ctx.ContentType("application/json")

	status := getStatus(id)

	m, _ := json.Marshal(status)
	return string(m)
}

func ApiList(ctx *web.Context) string {
	ctx.ContentType("application/json")
	
	res := make([]map[string]string, 0, 100)

	list, _ := registry.List()
	for _, j := range list {
		res = append(res, map[string] string{"id": j.Id, "status": string(j.Status)})
	}

	m, _ := json.Marshal(res)
	return string(m)	
}

func spawn(port int) {
	var s web.Server
	s.Post("/api/submit", ApiSubmit)
	s.Get("/api/status/(.*)", ApiStatus)
	s.Get("/api/list", ApiList)
	go func() { s.Run(fmt.Sprintf("0.0.0.0:%d", port))}()
}

func main() {
	spawn(5942)
	spawn(5943)

	c:=make(chan int)
	<-c
}
