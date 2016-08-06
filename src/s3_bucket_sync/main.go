package main

import (
	"fmt"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"log"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"
)

var (
	respC       = make(chan []*s3.Object, 5)
	entryC      = make(chan *s3.Object, 4000)
	done        = make(chan bool, 1)
	wg          sync.WaitGroup
	source      string
	destination string
)

func broker(c chan []*s3.Object) {
	defer wg.Done()
	wg.Add(1)
	count := 0
	zeroResults := 0
	t := time.Tick(5 * time.Second)
	for {
		select {
		case <-t:
			log.Printf("Count of Records: %d\n", count)
			if count == 0 {
				zeroResults++
			}
			if zeroResults > 4 {
				break
			}
		case <-done:
			done <- true
			break
		case r := <-c:
			count = count + len(r)
			for i, e := range r {
				if i%5000 == 0 {
					log.Printf("Sample Record: struct{%s, %s}", *e.LastModified, *e.Key)
				}
				entryC <- e
			}
		}
	}
}
func CopyObject(src *s3.Object) error {
	svc := s3.New(session.New())

	copySrc := fmt.Sprintf("%s/%s", source, *src.Key)
	params := &s3.CopyObjectInput{
		Bucket:     aws.String(destination), // Required
		CopySource: aws.String(copySrc),     // Required
		Key:        aws.String(*src.Key),    // Required
		ACL:        aws.String("public-read"),
		CopySourceIfNoneMatch: aws.String(*src.ETag),
	}
	resp, err := svc.CopyObject(params)

	if err != nil {
		if strings.Contains(err.Error(), "PreconditionFailed") {
			// Copy PreconditionFailed, which is successful
			return nil
		}
		log.Printf("ERROR %+v", err)
		return err
	}

	fmt.Printf("SUCCESS COPY: %v", *resp)
	return nil
}

func copyWorker(c chan *s3.Object, i int) {
	defer wg.Done()
	wg.Add(1)

	log.Printf("Starting copyWorker # %d", i)
	for {
		select {
		case e := <-c:
			CopyObject(e)
		case <-done:
			done <- true
			break
		}
	}
}

func mustGetEnv(key string) string {
	s := os.Getenv(key)
	if s == "" {
		log.Fatalf("Missing ENV %s", key)
	}
	return s
}

func main() {
	source = mustGetEnv("S3_SOURCE")
	destination = mustGetEnv("S3_DESTINATION")

	// workerCount := 0
	const copyCount int = 50
	var a [copyCount]int
	var i int
	for i, _ = range a {
		go copyWorker(entryC, i)
	}

	go broker(respC)
	listWorkers := 0
	for i = 56; i < 58; i++ {
		listWorkers++
		go listWorker(strconv.Itoa(i))
	}

	wg.Wait()
	<-done
}

func listWorker(n string) {
	defer wg.Done()
	wg.Add(1)
	log.Printf("Starting listWorker # %s", n)
	svc := s3.New(session.New())

	params := &s3.ListObjectsV2Input{
		Bucket:     aws.String(source), // Required
		MaxKeys:    aws.Int64(1000),
		StartAfter: aws.String(fmt.Sprintf(n)),
	}

	for {
		resp, err := svc.ListObjectsV2(params)
		if err != nil {
			log.Println(err.Error())
			break
		}

		respC <- resp.Contents

		if *resp.IsTruncated == false {
			// Done with responses
			break
		}
		params.ContinuationToken = resp.NextContinuationToken
	}
}
