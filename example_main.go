package main

import (
  "time"
  "fmt"
  "errors"

  "./breaker"
)

func getFunc(i int) (func() (interface{}, error)){
  return func() (interface{}, error){
    time.Sleep(400 * time.Millisecond)
    if i % 3 == 0 && i != 0 {
      return "Successful Call!", nil
    }

    return nil, errors.New("Error: Call failure")
  }
}

func main() {
  b := breaker.NewBreaker(3 * time.Second, 4, 1)
  done := make(chan bool)

  for i := 0; i < 31; i++ {
    funcToRun := getFunc(i)
    go func() {
      result, err := b.Run(funcToRun)
      if(err != nil){
        fmt.Println(err)
      }else{
        fmt.Println(result)
      }
      done <- true
    }()
    time.Sleep(1000 * time.Millisecond)
  }

  for i := 0; i < 31; i++ {
    <-done
  }
}