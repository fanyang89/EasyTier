// Two isolated IP stacks serve the same address, each on a separate test NIC.
// Each response identifies which NIC received the request.
package main

import (
	"bufio"
	"fmt"
	"log"
	"net"
	"net/netip"
	"os"

	"golang.zx2c4.com/wireguard/tun"
	"golang.zx2c4.com/wireguard/tun/netstack"
)

func forward(dst, src tun.Device) {
	buf, size := make([]byte, 65535), make([]int, 1)
	for {
		_, err := src.Read([][]byte{buf}, size, 0)
		if err != nil {
			log.Fatal(err)
		}
		if _, err = dst.Write([][]byte{buf[:size[0]]}, 0); err != nil {
			log.Fatal(err)
		}
	}
}

func echo(c net.Conn, nic string) {
	defer c.Close()
	scanner := bufio.NewScanner(c)
	for scanner.Scan() {
		if _, err := fmt.Fprintln(c, nic, scanner.Text()); err != nil {
			return
		}
	}
}

func serve(name string) {
	target := netip.MustParseAddrPort("198.18.247.42:24443")
	dev, err := tun.CreateTUN(name, 1400)
	if err != nil {
		log.Fatal(err)
	}
	vdev, stack, err := netstack.CreateNetTUN([]netip.Addr{target.Addr()}, nil, 1400)
	if err != nil {
		log.Fatal(err)
	}
	ln, err := stack.ListenTCPAddrPort(target)
	if err != nil {
		log.Fatal(err)
	}
	go forward(vdev, dev)
	go forward(dev, vdev)
	fmt.Println("ready", name)
	for {
		c, err := ln.Accept()
		if err != nil {
			log.Fatal(err)
		}
		go echo(c, name)
	}
}

func main() {
	if len(os.Args) != 2 {
		log.Fatal("provide the test NIC name")
	}
	serve(os.Args[1])
}
