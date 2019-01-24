package breaker

import (
  "time"
  "sync"
  "errors"
)

type BreakerState int

func (st BreakerState) String() string {
  switch(st){
    case ClosedState:
      return "Closed"
    case HalfOpenState:
      return "HalfOpen"
    case OpenState:
      return "Open"
    default:
      return ""
  }
}

const (
  ClosedState BreakerState = iota
  OpenState BreakerState = iota
  HalfOpenState BreakerState = iota
)

type BreakerCounters struct {
  failures int
  successes int
}

func (counters *BreakerCounters) addFailure() int{
  counters.failures++
  return counters.failures
}

func (counters *BreakerCounters) addSuccess() int{
  counters.successes++
  return counters.successes
}

func (counters *BreakerCounters) resetAll(){
  counters.successes = 0
  counters.failures = 0
}

func (counters *BreakerCounters) resetSuccesses(){
  counters.successes = 0
}

func (counters *BreakerCounters) resetFailures(){
  counters.failures = 0
}

type Breaker struct {
  BaseTimeout time.Duration
  MaxFailures int
  RequiredSuccesses int

  State BreakerState
  counters *BreakerCounters
  mainLock *sync.Mutex
}

func NewBreaker(timeout time.Duration, maxFailures int, requiredSuccesses int) *Breaker {
  return &Breaker{
    timeout,
    maxFailures,
    requiredSuccesses,
    ClosedState,
    &BreakerCounters{
      0,
      0,
    },
    &sync.Mutex{},
  }
}

// Function used to execute external calls. Return types support any interface and must include an error
func (breaker *Breaker) Run(call func() (interface{}, error)) (interface{}, error) {
  var result interface{}
  var err error

  if breaker.State == ClosedState || breaker.State == HalfOpenState {
    result, err = call()

    if(err != nil){
      breaker.addFailure(result)
    } else {
      breaker.addSuccess(result)
    }
  } else {
    err = errors.New("Error: Circuit Breaker is Open")
  }
  return result, err
}

func (breaker *Breaker) addFailure(result interface{}){
  switch breaker.State {
    case ClosedState:
      breaker.mainLock.Lock()
      failures := breaker.counters.addFailure()
      if failures >= breaker.MaxFailures {
        // Goroutine acquires the lock before the main one unlocks
        go breaker.changeState(OpenState)
      }
      breaker.mainLock.Unlock()
    case HalfOpenState:
      breaker.changeState(OpenState)
  }
}

func (breaker *Breaker) addSuccess(result interface{}){
  switch breaker.State {
    case HalfOpenState:
      breaker.mainLock.Lock()
      successes := breaker.counters.addSuccess()
      if successes >= breaker.RequiredSuccesses {
        // Goroutine acquires the lock before the main one unlocks
        go breaker.changeState(ClosedState)
      }
      breaker.mainLock.Unlock()
  }
}

func (breaker *Breaker) changeState(newState BreakerState) {
  breaker.mainLock.Lock()
  breaker.State = newState
  switch newState {
    case ClosedState:
      breaker.counters.resetAll()
    case HalfOpenState:
      breaker.counters.resetSuccesses()
    case OpenState:
      breaker.counters.resetSuccesses()
      // Goroutine needed in order to leave changeState
      go breaker.openForTimeout()
  }
  breaker.mainLock.Unlock()
}

func (breaker *Breaker) openForTimeout() {
  time.Sleep(breaker.BaseTimeout)
  breaker.changeState(HalfOpenState)
}

