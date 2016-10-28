package pool

import (
	"log"
	"sync"
	"time"
)

type WorkerPool struct {
	maxWorkerNumber int
	workerNumber    int
	workers         []*Worker
	lock            sync.Mutex
	maxIdleTime     time.Duration
	stop            chan struct{}
	stopFlag        bool
}

type Worker struct {
	fn           chan func()
	lastUsedTime int64
}

func NewLimit(num int) (*WorkerPool, error) {
	wp := &WorkerPool{
		maxWorkerNumber: num,
		maxIdleTime:     10 * time.Minute,
	}

	return wp, nil
}

func NewUnlimit() (*WorkerPool, error) {
	wp := &WorkerPool{
		maxWorkerNumber: -1,
		maxIdleTime:     10 * time.Minute,
	}

	return wp, nil
}

func (wp *WorkerPool) init() {
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
}

func (wp *WorkerPool) cleanup() {
	i := 0
	now := time.Now().Unix()
	for i = 0; i < len(wp.workers); i++ {
		if time.Duration(now-wp.workers[i].lastUsedTime) < wp.maxIdleTime {
			break
		}
	}

	wp.lock.Lock()
	wp.workers = wp.workers[i:]
	wp.lock.Unlock()
}

func (wp *WorkerPool) stopPool() {
	wp.stopFlag = true

	wp.lock.Lock()
	for _, w := range wp.workers {
		w.fn <- nil
	}
	wp.lock.Unlock()
}

func (wp *WorkerPool) Queue(fn func()) {
	worker := wp.GetWorker()
	if worker == nil {
		log.Print("get worker Failed")
		return
	}
	worker.fn <- fn
}

func (wp *WorkerPool) GetWorker() *Worker {
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
		go wp.StartWorker(worker)
		return worker
	}

	wp.lock.Lock()
	worker := wp.workers[len(wp.workers)-1]
	wp.workers = wp.workers[:len(wp.workers)-1]
	wp.lock.Unlock()
	return worker
}

func (wp *WorkerPool) StartWorker(worker *Worker) {
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
