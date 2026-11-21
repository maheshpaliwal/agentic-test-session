package svc

import (
	"context"
	"fmt"
	"net/http"
	"reflect"
	"testing"
	"time"
)

func TestEnvHelpers(t *testing.T) {
	t.Setenv("T_STR", "x")
	t.Setenv("T_INT", "-3")
	t.Setenv("T_BAD", "nope")
	t.Setenv("T_DUR", "-2s")
	if Env("T_STR", "d") != "x" || Env("T_UNSET", "d") != "d" {
		panic("Env")
	}
	if EnvInt("T_INT", 7) != -3 || EnvInt("T_BAD", 7) != 7 || EnvPositiveInt("T_INT", 7) != 7 {
		panic("EnvInt")
	}
	if EnvDur("T_DUR", time.Second) != -2*time.Second || EnvPositiveDur("T_DUR", time.Second) != time.Second || EnvDur("T_BAD", time.Minute) != time.Minute {
		panic("EnvDur")
	}
}

func TestSplitCSV(t *testing.T) {
	if got := SplitCSV(" a, ,b ,"); !reflect.DeepEqual(got, []string{"a", "b"}) {
		panic(fmt.Sprintf("got %v", got))
	}
	if SplitCSV("") != nil {
		panic("empty should be nil")
	}
}

func TestAddrChecks(t *testing.T) {
	for _, a := range []string{":80", "0.0.0.0:80", "[::]:80", "bogus"} {
		if !IsWildcardAddr(a) {
			t.Errorf("%q should be wildcard", a)
		}
	}
	if IsWildcardAddr("127.0.0.1:80") || !IsLoopbackHost("localhost:1") || IsLoopbackHost("10.0.0.1:1") || IsLoopbackHost("x") {
		panic("addr checks")
	}
}

func TestServeShutsDownOnCancel(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	srv := &http.Server{Addr: "127.0.0.1:0", Handler: http.NotFoundHandler()}
	ran := make(chan struct{})
	errc := make(chan error, 1)
	go func() { errc <- Serve(ctx, srv, time.Second, func() { close(ran) }) }()
	time.Sleep(50 * time.Millisecond)
	cancel()
	select {
	case err := <-errc:
		if err != nil {
			panic(err)
		}
	case <-time.After(3 * time.Second):
		panic("Serve did not return")
	}
	select {
	case <-ran:
	default:
		panic("beforeShutdown not called")
	}
}
