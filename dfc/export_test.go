package dfc

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"
	"time"
)

func (s *Smap) SmapAdd(id string) {
	s.add(&daemonInfo{DaemonID: id})
}

func NewSmap() Smap {
	return Smap{Smap: make(map[string]*daemonInfo, 8)}
}

func HRWTarget(name string, smap *Smap) (string, error) {
	t, errstr := hrwTarget(name, smap)
	if errstr != "" {
		return "", fmt.Errorf("Failed to get target's id")
	}

	return t.DaemonID, nil
}

func StartTestProxyServer() {
	ctx.rg = &rungroup{
		runarr: make([]runner, 0, 4),
		runmap: make(map[string]runner),
	}

	ctx.smap = &Smap{Smap: make(map[string]*daemonInfo, 8)}
	ctx.config.StatsTime = time.Duration(100000 * time.Hour)
	ctx.config.KeepAliveTime = time.Duration(100000 * time.Hour)
	ctx.config.Listen.Port = "8081"
	p := &proxyrunner{
		confdir: "/tmp",
	}
	p.si = &daemonInfo{
		NodeIPAddr: "localhost",
		DaemonPort: "8081",
		DaemonID:   "test proxy",
		DirectURL:  "http://localhost:8081",
	}

	ctx.rg.add(p, xproxy)
	ctx.rg.add(&proxystatsrunner{}, xproxystats)
	ctx.rg.add(newproxykalive(p), xproxykalive)

	ctx.rg.run()
}

func StopTestProxyServer() {
	ctx.rg.stop()
}

func doHTTP(method string, msg []byte) error {
	url := "http://localhost:8081/v1/cluster"
	req, err := http.NewRequest(method, url, bytes.NewBuffer(msg))
	req.Header.Set("Content-Type", "application/json")
	client := &http.Client{}
	resp, err := client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	ioutil.ReadAll(resp.Body)
	if resp.StatusCode != 200 {
		return fmt.Errorf("http error %s", resp.Status)
	}
	return nil
}

func AddTarget(t string) error {
	msg, err := json.Marshal(daemonInfo{
		NodeIPAddr: "127.0.0.1",
		DaemonPort: "8081",
		DaemonID:   t,
		DirectURL:  "http://127.0.0.1:8081",
	})
	if err != nil {
		return err
	}

	return doHTTP(http.MethodPost, msg)
}

func ListTargets() error {
	msg, err := json.Marshal(GetMsg{
		GetWhat: "smap",
	})
	if err != nil {
		return err
	}

	return doHTTP(http.MethodGet, msg)
}
