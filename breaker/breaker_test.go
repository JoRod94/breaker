package breaker

import (
  "testing"
  "fmt"
  "time"
  "errors"
  "math/rand"
)

var OpenStateError = "Error: Circuit Breaker is Open"

func alwaysFailsFunc() (interface{}, error){
  return nil, errors.New("Failed")
}

func alwaysSucceedsFunc() (interface{}, error){
  return true, nil
}

func getFunc() func() (interface{}, error){
  if(rand.Intn(1) % 2 == 0){
    return alwaysSucceedsFunc
  }else{
    return alwaysFailsFunc
  }
}

func (breaker *Breaker) open() {
  for i := 0; i < breaker.MaxFailures; i++ {
    go breaker.Run(alwaysFailsFunc)
    // Needs at least some time in between requests, immediate execution is not representable
    time.Sleep(200 * time.Millisecond)
  }
}

func (breaker *Breaker) halfOpen() {
  for i := 0; i < breaker.MaxFailures; i++ {
    go breaker.Run(alwaysFailsFunc)
    // Needs at least some time in between requests, immediate execution is not representable
    time.Sleep(200 * time.Millisecond)
  }

  time.Sleep(breaker.BaseTimeout)
}

func evaluateCondition(t *testing.T, condition bool, testName string){
  if(!condition){
    t.Error(fmt.Sprintf(testName, " test failure"))
  }
  fmt.Println("Passed: ", testName)
}

func TestAlwaysFailsFunc(t *testing.T){
  breaker := NewBreaker(5 * time.Second, 2, 2)
  _, err := breaker.Run(alwaysFailsFunc)

  evaluateCondition(t, err != nil, "TestAlwaysFailsFunc")
}

func TestAlwaysSucceedsFunc(t *testing.T){
  breaker := NewBreaker(5 * time.Second, 2, 2)
  _, err := breaker.Run(alwaysSucceedsFunc)

  evaluateCondition(t, err == nil, "TestAlwaysSucceedsFunc")
}

// State is open after maxFailures
func TestOpenAfterFailures(t *testing.T){
  breaker := NewBreaker(5 * time.Second, 2, 2)

  breaker.open()

  evaluateCondition(t, breaker.State == OpenState, "TestOpenAfterFailures")
}

// A correct call fails when state is Open, with correct error
func TestFailWithOpenState(t *testing.T){
  breaker := NewBreaker(5 * time.Second, 2, 2)

  breaker.open()

  _, err := breaker.Run(alwaysSucceedsFunc)

  evaluateCondition(t, err != nil && err.Error() == OpenStateError, "TestFailWithOpenState")
}

// An open breaker becomes half open after the timeout
func TestHalfOpenAfterTimeout(t *testing.T){
  breaker := NewBreaker(2 * time.Second, 2, 2)

  breaker.halfOpen()

  evaluateCondition(t, breaker.State == HalfOpenState, "TestHalfOpenAfterTimeout")
}

// A correct call succeeds after a state is opened and then moved to half-open after the timeout
func TestSucceedsAfterTimeout(t *testing.T){
  breaker := NewBreaker(2 * time.Second, 2, 2)

  breaker.halfOpen()

  _, err := breaker.Run(alwaysSucceedsFunc)

  evaluateCondition(t, err == nil, "TestSucceedsAfterTimeout")
}

// State becomes open again if a half-open breaker makes a call that fails
func TestOpensAfterHalfOpenFailure(t *testing.T){
  breaker := NewBreaker(2 * time.Second, 2, 2)

  breaker.halfOpen()
  breaker.Run(alwaysFailsFunc)

  evaluateCondition(t, breaker.State == OpenState, "TestOpensAfterHalfOpenFailure")
}

// State becomes open again if a half-open breaker makes a call that fails
func TestClosesAfterRequiredSuccesses(t *testing.T){
  breaker := NewBreaker(2 * time.Second, 2, 2)

  breaker.halfOpen()
  breaker.Run(alwaysSucceedsFunc)
  breaker.Run(alwaysSucceedsFunc)

  // Required since this recovery is not immediate (due to concurrency in correct locking and unlocking)
  time.Sleep(200 * time.Millisecond)

  evaluateCondition(t, breaker.State == ClosedState, "TestClosesAfterRequiredSuccesses")
}