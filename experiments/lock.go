package main

import (
	"fmt"
	"sync"
)

func miau() {
	fmt.Println("miau")
}

func main() {
	counter := 0
	var mu sync.Mutex
	var wg sync.WaitGroup

	for i := 0; i < 1000; i++ {
		wg.Add(1)
		go func(){
			defer wg.Done()
			
			mu.Lock()
			defer mu.Unlock()
			counter++

			miau()
		}()
	}

	mu.Lock()
	end := counter
	mu.Unlock()
	
	wg.Wait()
	fmt.Println(end, counter)
}