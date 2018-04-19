package dfc_test

import (
	"testing"
	"time"
)

var (
	failbackTests = []Test{
		Test{"Basic", failback_basic},
		Test{"Multiple", failback_multiple_failures},
		Test{"Fast Restore", failback_fast_restore},
	}
)

func Test_failback(t *testing.T) {
	originalproxyid := canRunMultipleProxyTests(t)
	originalproxyurl := proxyurl

	for _, test := range failbackTests {
		t.Run(test.name, test.method)
		if t.Failed() && abortonerr {
			t.FailNow()
		}
	}

	resetPrimaryProxy(originalproxyid, t)
	proxyurl = originalproxyurl
}

//==========
//
// Subtests
//
//==========

func failback_basic(t *testing.T) {
	// Get Smap
	smap := getClusterMap(httpclient, t)

	// hrwProxy to find next proxy
	delete(smap.Pmap, smap.ProxySI.DaemonID)
	nextProxyID, nextProxyURL, err := hrwProxy(&smap)
	if err != nil {
		t.Errorf("Error performing HRW: %v", err)
	}

	// Kill original primary proxy
	primaryProxyURL := smap.ProxySI.DirectURL
	primaryProxyID := smap.ProxySI.DaemonID
	cmd, args, err := kill(httpclient, primaryProxyURL, smap.ProxySI.DaemonPort)
	if err != nil {
		t.Errorf("Error killing Primary Proxy: %v", err)
	}
	// Wait the maxmimum time it should take to switch.
	waitProgressBar("Primary Proxy Changing: ", time.Duration(2*keepaliveseconds)*time.Second)

	err = restore(httpclient, primaryProxyURL, cmd, args, true)
	if err != nil {
		t.Errorf("Error restoring proxy: %v", err)
	}

	waitProgressBar("Proxy Suspect Time: ", time.Duration(startupsuspectseconds+5)*time.Second)

	// Check if the previous primary proxy correctly rejoined the cluster
	proxyurl = nextProxyURL
	smap = getClusterMap(httpclient, t)
	if smap.ProxySI.DaemonID != nextProxyID {
		t.Errorf("Incorrect Primary Proxy: %v, should be: %v", smap.ProxySI.DaemonID, nextProxyID)
	}
	if _, ok := smap.Pmap[primaryProxyID]; !ok {
		t.Errorf("Previous Primary Proxy did not rejoin the cluster.")
	}
}

func failback_fast_restore(t *testing.T) {
	// FIXME: won't the smap be out of sync if this happens (in terms of version number)?

	// Get Smap
	smap := getClusterMap(httpclient, t)

	// Kill original primary proxy
	primaryProxyURL := smap.ProxySI.DirectURL
	primaryProxyID := smap.ProxySI.DaemonID
	cmd, args, err := kill(httpclient, primaryProxyURL, smap.ProxySI.DaemonPort)
	if err != nil {
		t.Errorf("Error killing Primary Proxy: %v", err)
	}
	time.Sleep(2 * time.Second)
	err = restore(httpclient, primaryProxyURL, cmd, args, true)
	if err != nil {
		t.Errorf("Error restoring proxy: %v", err)
	}

	waitProgressBar("Proxy Suspect Time: ", time.Duration(startupsuspectseconds+5)*time.Second)

	// Check if the previous primary proxy correctly remained primary
	smap = getClusterMap(httpclient, t)
	if smap.ProxySI.DaemonID != primaryProxyID {
		t.Errorf("Incorrect Primary Proxy: %v, should be: %v", smap.ProxySI.DaemonID, primaryProxyID)
	}
}
func failback_multiple_failures(t *testing.T) {
	// Get Smap
	smap := getClusterMap(httpclient, t)
	if len(smap.Pmap) <= 2 {
		t.Errorf("Canot run Failback_multiple_failures with %d proxies; must be at least %d", len(smap.Pmap), 2)
	}

	// hrwProxy to find next proxy
	delete(smap.Pmap, smap.ProxySI.DaemonID)
	nextProxyID, nextProxyURL, err := hrwProxy(&smap)
	if err != nil {
		t.Errorf("Error performing HRW: %v", err)
	}

	// Kill original primary proxy, and the next proxy in the list that isn't the primary.
	primaryProxyURL, primaryProxyID := smap.ProxySI.DirectURL, smap.ProxySI.DaemonID
	pcmd, pargs, err := kill(httpclient, primaryProxyURL, smap.ProxySI.DaemonPort)
	if err != nil {
		t.Errorf("Error killing Primary Proxy: %v", err)
	}

	secondProxyURL, secondProxyPort, secondProxyID := "", "", ""
	// Select a "random" proxy
	// FIXME: map iteration is apparently deterministic for small numbers of elements
	for pid, pxyinfo := range smap.Pmap {
		if pid == nextProxyID || pid == primaryProxyID {
			continue
		}
		secondProxyURL = pxyinfo.DirectURL
		secondProxyPort = pxyinfo.DaemonPort
		secondProxyID = pxyinfo.DaemonID
		break
	}
	spcmd, spargs, err := kill(httpclient, secondProxyURL, secondProxyPort)
	if err != nil {
		t.Errorf("Error killing Target: %v", err)
	}

	// Wait the maxmimum time it should take to switch.
	waitProgressBar("Primary Proxy Changing: ", time.Duration(2*keepaliveseconds)*time.Second)

	// Restore the killed proxies
	err = restore(httpclient, secondProxyURL, spcmd, spargs, true)
	if err != nil {
		t.Errorf("Error restoring target: %v", err)
	}
	err = restore(httpclient, primaryProxyURL, pcmd, pargs, true)
	if err != nil {
		t.Errorf("Error restoring proxy: %v", err)
	}

	waitProgressBar("Startup Suspect Time: ", time.Duration(startupsuspectseconds+5)*time.Second)

	// Check if the killed proxies successfully rejoin the cluster.
	proxyurl = nextProxyURL
	smap = getClusterMap(httpclient, t)
	if smap.ProxySI.DaemonID != nextProxyID {
		t.Errorf("Incorrect Primary Proxy: %v, should be: %v", smap.ProxySI.DaemonID, nextProxyID)
	}
	if _, ok := smap.Pmap[primaryProxyID]; !ok {
		t.Errorf("Previous Primary Proxy did not rejoin the cluster.")
	}
	if _, ok := smap.Pmap[secondProxyID]; !ok {
		t.Errorf("Previous Primary Proxy did not rejoin the cluster.")
	}
}
