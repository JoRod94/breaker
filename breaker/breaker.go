package breaker

import (
  "time"
  "sync"
  "errors"
)

type State int

func (st State) String() string {
  switch(st){
    case ClosedState:
      return "Closed"
    case HalfOpenState:
      return "HalfOpen"
    case OpenState:
      return "Open"
  }

  return ""
}

const (
  ClosedState State = iota
  OpenState State = iota
  HalfOpenState State = iota
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
  baseTimeout time.Duration
  maxFailures int
  requiredSuccesses int

  state State
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

func (breaker *Breaker) Run(call func() (interface{}, error)) (interface{}, error) {
  var result interface{}
  var err error

  if breaker.state == ClosedState || breaker.state == HalfOpenState {
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
  switch breaker.state {
    case ClosedState:
      breaker.mainLock.Lock()
      failures := breaker.counters.addFailure()
      if failures >= breaker.maxFailures {
        go breaker.changeState(OpenState)
      }
      breaker.mainLock.Unlock()
    case HalfOpenState:
      breaker.changeState(OpenState)
  }
}

func (breaker *Breaker) addSuccess(result interface{}){
  switch breaker.state {
    case HalfOpenState:
      breaker.mainLock.Lock()
      successes := breaker.counters.addSuccess()
      if successes >= breaker.requiredSuccesses {
        go breaker.changeState(ClosedState)
      }
      breaker.mainLock.Unlock()
  }
}

func (breaker *Breaker) changeState(newState State) {
  breaker.mainLock.Lock()
  breaker.state = newState
  switch newState {
    case ClosedState:
      breaker.counters.resetAll()
    case HalfOpenState:
      breaker.counters.resetSuccesses()
    case OpenState:
      breaker.counters.resetSuccesses()
      go breaker.openForTimeout()
  }
  breaker.mainLock.Unlock()
}

func (breaker *Breaker) openForTimeout() {
  time.Sleep(breaker.baseTimeout)
  breaker.changeState(HalfOpenState)
}

