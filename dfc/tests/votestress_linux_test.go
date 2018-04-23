package dfc_test

import (
	"fmt"
	"math/rand"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/NVIDIA/dfcpub/pkg/client"
	"github.com/NVIDIA/dfcpub/pkg/client/readers"
)

const (
	multiproxydir = "multipleproxy"
	multiproxybkt = "multipleproxytmp"
)

func rwdloop(seed int64, stopch <-chan struct{}, proxyurlch <-chan string, errch chan error, wg *sync.WaitGroup) {
	defer wg.Done()
	// Each iteration of the loop puts, then gets, then deletes. This way, failovers will theoretically happen in each step of the process.

	random := rand.New(rand.NewSource(seed))
	missedDeletes := make(chan string, 10)
loop:
	for {
		select {
		case <-stopch:
			close(errch)
			break loop
		default:
		}
		select {
		case proxyurl = <-proxyurlch:
			// Any deletes that were missed can be executed here
			done := false
			for !done {
				select {
				case keyname := <-missedDeletes:
					err := client.Del(proxyurl, multiproxybkt, keyname, nil, errch, true)
					if err != nil {
						missedDeletes <- keyname
					}
				default:
					done = true
				}
			}
		default:
		}

		reader, err := readers.NewRandReader(fileSize, true /* withHash */)
		if err != nil {
			if errch != nil {
				errch <- err
			}
		}

		fname := client.FastRandomFilename(random, fnlen)
		keyname := fmt.Sprintf("%s/%s", multiproxydir, fname)

		err = client.Put(proxyurl, reader, multiproxybkt, keyname, true /* silent */)
		if err != nil {
			errch <- err
			// Skip the get/delete state
			time.Sleep(time.Duration(keepaliveseconds) * time.Second)
			continue
		}
		time.Sleep(1 * time.Second)

		client.Get(proxyurl, multiproxybkt, keyname, nil, errch, true, false)
		time.Sleep(1 * time.Second)

		err = client.Del(proxyurl, multiproxybkt, keyname, nil, errch, true)
		if err != nil {
			missedDeletes <- keyname
		}
		time.Sleep(5 * time.Second)

	}
}

func killLoop(t *testing.T, seed int64, stopch <-chan struct{}, proxyurlchs []chan string, errch chan error, wg *sync.WaitGroup) {
	defer wg.Done()
	random := rand.New(rand.NewSource(seed))

loop:
	for {
		select {
		case <-stopch:
			close(errch)
			for _, ch := range proxyurlchs {
				close(ch)
			}
			break loop
		default:
		}

		smap := getClusterMap(httpclient, t)
		delete(smap.Pmap, smap.ProxySI.DaemonID)
		_, nextProxyURL, err := hrwProxy(&smap)
		if err != nil {
			errch <- fmt.Errorf("Error performing HRW: %v", err)
		}

		primaryProxyURL := smap.ProxySI.DirectURL
		cmd, args, err := kill(httpclient, primaryProxyURL, smap.ProxySI.DaemonPort)
		if err != nil {
			errch <- fmt.Errorf("Error killing Primary Proxy: %v", err)
		}

		time.Sleep(time.Duration(2*keepaliveseconds) * time.Second)
		for _, ch := range proxyurlchs {
			ch <- nextProxyURL
		}

		var idx int
		found := false
		for i, arg := range args {
			if strings.Contains(arg, "-proxyurl") {
				idx = i
				found = true
			}
		}
		if found {
			args = append(args[:idx], args[idx+1:]...)
		}

		proxyurl = nextProxyURL
		args = append(args, "-proxyurl="+nextProxyURL)
		err = restore(httpclient, primaryProxyURL, cmd, args, false)
		if err != nil {
			errch <- fmt.Errorf("Error restoring proxy: %v", err)
		}

		durationmillis := (random.NormFloat64()) * 30           // [0, 30]
		sleepdir := time.Duration(durationmillis) * time.Second // [0, 30s)
		time.Sleep(sleepdir)
	}
}

func Test_votestress(t *testing.T) {
	originalproxyid := canRunMultipleProxyTests(t)
	originalproxyurl := proxyurl

	client.CreateLocalBucket(proxyurl, multiproxybkt)

	bs := int64(baseseed)
	errchs := make([]chan error, numworkers+1)
	stopchs := make([]chan struct{}, numworkers+1)
	proxyurlchs := make([]chan string, numworkers)
	wg := &sync.WaitGroup{}
	for i := 0; i < numworkers; i++ {
		errchs[i] = make(chan error, 10)
		stopchs[i] = make(chan struct{}, 10)
		proxyurlchs[i] = make(chan string, 10)
		wg.Add(1)
		go rwdloop(bs, stopchs[i], proxyurlchs[i], errchs[i], wg)
		bs += 1
		time.Sleep(50 * time.Millisecond) // stagger
	}

	errchs[numworkers] = make(chan error, 10)
	stopchs[numworkers] = make(chan struct{}, 10)
	wg.Add(1)
	go killLoop(t, bs, stopchs[numworkers], proxyurlchs, errchs[numworkers], wg)

	timer := time.After(multiProxyTestDuration)
	var errs uint64 = 0
loop:
	for {
		select {
		case <-timer:
			break loop
		default:
		}

		for _, ch := range errchs {
			select {
			case <-ch:
				// This test is likely to cause a lot of errors, but the real goal is for the cluster to not panic ever.
				errs++
			default:
			}
		}
	}

	for _, stopch := range stopchs {
		var v struct{}
		stopch <- v
		close(stopch)
	}

	wg.Wait()

	client.DestroyLocalBucket(proxyurl, multiproxybkt)
	resetPrimaryProxy(originalproxyid, t)
	proxyurl = originalproxyurl
}
