package main

import (
	"os"
	"fmt"
	cgl "tideland-cgl.googlecode.com/hg"
)


type JobRegistry struct {
	submission chan JobRegistrySubmitRequest
	listing chan JobRegistryListRequest
	getting chan JobRegistryGetRequest
}

type JobRegistrySubmitRequest struct {
	request JobRequest
	response chan string
	error chan os.Error
}

type JobRegistryListRequest struct {
	response chan []Job
	error chan os.Error
}

type JobRegistryGetRequest struct {
	id string
	response chan Job
	error chan os.Error
}


func NewJobRegistry() (this *JobRegistry) {
	this = &JobRegistry{make(chan JobRegistrySubmitRequest, 10), make(chan JobRegistryListRequest, 10), make(chan JobRegistryGetRequest, 10)}

	go this.handler()

	return
}


func (this *JobRegistry) handler() {
	jobMap := make(map[string] *Job)
	
	for {
		select {
		case sub := <- this.submission:
			workers := sub.request.NWorkers
			if workers < 1 {
				workers = 1
			}
			
			job := Job{cgl.NewUUID().String(), QUEUED, 0, make(map[string]Metric), workers}
			jobMap[job.Id] = &job
			println("got submission")
			
			sub.response <- job.Id

			Schedule(&job)
			
		case list := <- this.listing:			
			res := make([]Job, 0, 100)
			for _, v := range jobMap {
				res = append(res, *v)
			}
			list.response <- res

		case get := <- this.getting:
			job, ok := jobMap[get.id]
			if !ok {
				get.error <- os.NewError("Cannot find job " + get.id)
			} else {
				get.response <- *job
			}

		}
	}
}


func (this *JobRegistry) Submit(job JobRequest) (string, os.Error) {
	res := make(chan string, 1)
	error := make(chan os.Error, 1)

	req := JobRegistrySubmitRequest{job, res, error}
	this.submission <- req
	return <-res, nil
}

func (this *JobRegistry) List() ([]Job, os.Error) {
	res := make(chan []Job, 1)
	error := make(chan os.Error, 1)

	req := JobRegistryListRequest{res, error}
	this.listing <- req
	return <-res, nil
}

func (this *JobRegistry) Get(id string) (*Job, os.Error) {
	res := make(chan Job, 1)
	error := make(chan os.Error, 1)

	req := JobRegistryGetRequest{id, res, error}
	this.getting <- req
	select {
	case job := <- res:
		fmt.Printf("GOT JOB %s\n", job.Id)
		return &job, nil
	case e := <- error:
		return nil, e
	}
	return nil, nil
}
