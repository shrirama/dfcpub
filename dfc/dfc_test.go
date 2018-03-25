package dfc_test

import (
	"fmt"
	"math/rand"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/NVIDIA/dfcpub/dfc"
	"github.com/NVIDIA/dfcpub/pkg/client"
)

func TestHRWDistribution(t *testing.T) {
	targets := dfc.NewSmap()
	rnd := rand.New(rand.NewSource(time.Now().UnixNano()))

	for i := 0; i < 10; i++ {
		targets.SmapAdd(strconv.Itoa(8080 + i))
	}

	distMap := make(map[int]int)
	for i := 0; i < 1000000; i++ {
		n := client.FastRandomFilename(rnd, 32)
		idStr, err := dfc.HRWTarget(n, &targets)
		if err != nil {
			t.Fatal(err)
		}

		id, err := strconv.Atoi(idStr)
		if err != nil {
			t.Fatal(err)
		}
		distMap[id]++
	}

	fmt.Println(distMap)
}

func BenchmarkHRWGet(b *testing.B) {
	targets := dfc.NewSmap()

	for i := 0; i < 10; i++ {
		targets.SmapAdd(strconv.Itoa(8080 + i))
	}

	b.RunParallel(func(pb *testing.PB) {
		rnd := rand.New(rand.NewSource(time.Now().UnixNano()))
		distMap := make(map[string]int)
		for pb.Next() {
			n := client.FastRandomFilename(rnd, 32)
			id, err := dfc.HRWTarget(n, &targets)
			if err != nil {
				b.Fatal(err)
			}
			distMap[id]++
		}
	})
}

func TestProxy(t *testing.T) {
	go dfc.StartTestProxyServer()

	// Give sometime for the go routines including http server to start
	time.Sleep(time.Second)

	for i := 0; i < 10; i++ {
		err := dfc.AddTarget("test taregt " + strconv.Itoa(i))
		if err != nil {
			t.Fatal(err)
		}
	}

	var wg sync.WaitGroup

	wg.Add(1)
	go func() {
		for i := 0; i < 1000; i++ {
			dfc.ListTargets()
		}
		wg.Done()
	}()

	for i := 10; i < 20; i++ {
		err := dfc.AddTarget("test taregt " + strconv.Itoa(i))
		if err != nil {
			t.Fatal(err)
		}
	}

	wg.Wait()
	dfc.StopTestProxyServer()
}
