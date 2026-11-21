package svc

import (
	"fmt"
	"net"
	"strings"
	"testing"
)

func TestListenNamesTheSettingWhenThePortIsTaken(t *testing.T) {
	held, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		panic(err)
	}
	defer held.Close()
	_, err = Listen(held.Addr().String(), "--addr or TSESSION_CP_ADDR")
	if err == nil || !strings.Contains(err.Error(), "address already in use") || !strings.Contains(err.Error(), "--addr or TSESSION_CP_ADDR") {
		panic(fmt.Sprintf("got %v", err))
	}
	ln, err := Listen("127.0.0.1:0", "x")
	if err != nil {
		panic(err)
	}
	ln.Close()
}
