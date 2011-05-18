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


//
func (this *Job) Run() {
	fmt.Printf("Running job %s\n", this.Id)
	this.Status = RUNNING

	for {
		time.Sleep(rand.Int63n(20) * 1e8)
		
		increment := rand.Intn(4)

		comp := this.Completion + increment
		if comp > 100 {
			comp = 100
		}
		
		this.Completion = comp

		fmt.Printf("Progressing %s: %d%%\n", this.Id, this.Completion)
		
		if comp >= 100 {
			this.Status = "DONE"
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
	Test int64 "test"
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

type Metric struct {
	Name string "name"
	
}

var registry JobRegistry = *NewJobRegistry()

func ApiSubmit(ctx *web.Context) string {
	var request JobRequest

	println("Got request: ", string(ctx.Request.ParamData))

	e := json.Unmarshal(ctx.Request.ParamData, &request)
	if e != nil {
		println("got unmarshal error: ", e.String())
	}

	println("request:", request.Test)

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
		println("got error", e.String())
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

func main() {
	web.Post("/api/submit", ApiSubmit)
	web.Get("/api/status/(.*)", ApiStatus)
	web.Get("/api/list", ApiList)
	web.Run("0.0.0.0:5941")
}
