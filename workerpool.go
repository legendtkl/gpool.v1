package gpool

import (
	"log"
	"sync"
	"time"
)

// WorkerPool define a worker pool
type WorkerPool struct {
	maxWorkerNumber int           //the max worker number in the pool
	workerNumber    int           //the worker number now in the pool
	workers         []*Worker     //the available worker queue
	lock            sync.Mutex    //for queue thread-safe
	maxIdleTime     time.Duration //the recycle time. That means goroutine will be destroyed when it has not been used for maxIdleTime.
	stop            chan struct{} //stop
	stopFlag        bool          //trick
	objectPool      *sync.Pool    //gc-friendly
}

//Worker run as a goroutine
type Worker struct {
	fn           chan func()
	lastUsedTime int64
}

// NewLimt creates a worker pool
//
// maxWorkerNum define the max worker number in the pool. When the worker
// number exceeds maxWorkerNum, we will ignore the job.
// recycleTime(minute) define the time to recycle goroutine. When a goroutine has
// not been used for recycleTime, it will be recycled.
func NewLimit(maxWorkerNum, recycleTime int) (*WorkerPool, error) {
	wp := &WorkerPool{
		maxWorkerNumber: maxWorkerNum,
		maxIdleTime:     time.Duration(recycleTime) * time.Minute,
		objectPool: &sync.Pool{
			New: func() interface{} {
				return &Worker{
					fn: make(chan func()),
				}
			},
		},
	}
	wp.init()
	return wp, nil
}

// NewUnlimit creates a unlimited-number worker pool.
func NewUnlimit(recycleTime int) (*WorkerPool, error) {
	wp := &WorkerPool{
		maxWorkerNumber: -1,
		maxIdleTime:     time.Duration(recycleTime) * time.Minute,
	}

	wp.init()
	return wp, nil
}

// init initializes the workerpool.
//
// init func will be in charge of cleaning up goroutines and receiving the stop signal.
func (wp *WorkerPool) init() {
	go func() {
		tick := time.Tick(wp.maxIdleTime)
		for {
			select {
			case <-tick:
				wp.cleanup()
			case <-wp.stop:
				wp.stopPool()
				break
			}
		}
	}()
}

// Stop stop goroutine pool
func (wp *WorkerPool) Stop() {
	wp.stop <- struct{}{}
}

// cleanup cleans up the available worker queue.
func (wp *WorkerPool) cleanup() {
	i := 0
	now := time.Now().Unix()
	for i = 0; i < len(wp.workers); i++ {
		if time.Duration(now-wp.workers[i].lastUsedTime)*time.Second < wp.maxIdleTime {
			break
		} else {
			close(wp.workers[i].fn)
		}
	}

	wp.lock.Lock()
	wp.workers = wp.workers[i:]
	wp.lock.Unlock()
}

// stopPool stops the worker pool.
func (wp *WorkerPool) stopPool() {
	wp.stopFlag = true

	wp.lock.Lock()
	for _, w := range wp.workers {
		w.fn <- nil
	}
	wp.lock.Unlock()
}

// Queue assigns a worker for job (fn func(), with closure we can define every job in this form)
//
// If the worker pool is limited-number and the worker number has reached the limit, we prefer to discard the job.
func (wp *WorkerPool) Queue(fn func()) {
	worker := wp.getWorker()
	if worker == nil {
		log.Print("get worker Failed")
		return
	}
	worker.fn <- fn
}

// GetWorker select a worker.
//
// If the available worker queue is empty, we will new a worker.
// else we will select the last worker, in this case, the worker queue
// is like a FILO queue, and the select algorithm is kind of like LRU.
func (wp *WorkerPool) getWorker() *Worker {
	if len(wp.workers) == 0 {
		wp.workerNumber++
		if wp.maxWorkerNumber != -1 && wp.workerNumber > wp.maxWorkerNumber {
			//log
			log.Println("worker number excess max")
			return nil
		}
		worker := &Worker{
			fn: make(chan func()),
		}
		go wp.startWorker(worker)
		return worker
	}

	wp.lock.Lock()
	worker := wp.workers[len(wp.workers)-1]
	wp.workers = wp.workers[:len(wp.workers)-1]
	wp.lock.Unlock()
	return worker
}

// StartWorker starts a new goroutine.
func (wp *WorkerPool) startWorker(worker *Worker) {
	for f := range worker.fn {
		if f == nil {
			break
		}
		f()

		if wp.stopFlag == true {
			break
		}
		worker.lastUsedTime = time.Now().Unix()
		wp.lock.Lock()
		wp.workers = append(wp.workers, worker)
		wp.lock.Unlock()
	}
}
