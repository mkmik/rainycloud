package main

import (
	"web"
	"os"
	"fmt"
	"json"
)

func Uuid() string {
	f, _ := os.Open("/dev/urandom")
	defer f.Close()
	b := make([]byte, 16)
	f.Read(b)
	return fmt.Sprintf("%x-%x-%x-%x-%x", b[0:4], b[4:6], b[6:8], b[8:10], b[10:])
}

////////////

type JobRegistry struct {
	submission chan JobRegistrySubmitRequest
	listing chan JobRegistryListRequest
}

type JobRegistrySubmitRequest struct {
	request JobRequest
	response chan string
}

type JobRegistryListRequest struct {
	response chan []Job
}

type JobStatus string

const (
	RUNNING JobStatus = "RUNNING"
	DONE = "DONE"
	FAILED = "FAILED"
)

type Job struct {
	Id string
	Status JobStatus
}

func NewJobRegistry() (this *JobRegistry) {
	this = &JobRegistry{make(chan JobRegistrySubmitRequest, 10), make(chan JobRegistryListRequest, 10)}

	go func() {
		jobMap := make(map[string] Job)
		
		for {
			select {
			case sub := <- this.submission:
				job := Job{Uuid(), RUNNING}
				jobMap[job.Id] = job
				println("got request")
				sub.response <- job.Id
				
			case list := <- this.listing:
				
				res := make([]Job, 0, 100)
				for _, v := range jobMap {
					res = append(res, v)
				}
				list.response <- res
				
			}
		}
	}()

	return
}

func (this *JobRegistry) Submit(job JobRequest) (string, os.Error) {
	res := make(chan string, 1)
	req := JobRegistrySubmitRequest{job, res}
	this.submission <- req
	return <-res, nil
}

func (this *JobRegistry) List() ([]Job, os.Error) {
	res := make(chan []Job, 1)
	req := JobRegistryListRequest{res}
	this.listing <- req
	return <-res, nil
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
	Count int "count"
	Completed int "completed"
}

var registry JobRegistry = *NewJobRegistry()

func ApiSubmit(ctx *web.Context) string {
	var request JobRequest

	println("Got request: ", string(ctx.Request.ParamData))

	e := json.Unmarshal(ctx.Request.ParamData, &request)
	if e != nil {
		println("got error: ", e.String())
	}

	println("request:", request.Test)

	registry.Submit(request)

	ctx.ContentType("application/json")
	job := &JobResponse{Uuid(), nil}
	m, _ := json.Marshal(job)
	return string(m)
}

func getStatus(id string) *JobStatusResponse {
	return &JobStatusResponse{10, 4}
}

func ApiStatus(ctx *web.Context, id string) string {
	ctx.ContentType("application/json")

	status := getStatus(id)
	count := status.Count
	completed := status.Completed

	res := &JobStatusResponse{count, completed}
	m, _ := json.Marshal(res)
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
