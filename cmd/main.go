package main

import (
	"fmt"
	"github.com/smartwalle/pool4go"
)

func main() {
	var p = pool4go.New(func() (pool4go.Conn, error) {
		var c = &Conn{}
		fmt.Println("new")
		return c, nil
	}, pool4go.WithMaxOpen(5), pool4go.WithMaxIdle(2))

	for i := 0; i < 3; i++ {
		go func() {
			for {

				c, err := p.Get()
				if err != nil {
					continue
				}

				if c == nil {
					continue
				}

				//time.Sleep(time.Second * 1)
				//fmt.Println("do...")

				p.Put(c)
			}
		}()
	}

	select {}
}

type Conn struct {
}

func (this *Conn) Close() error {
	fmt.Println("close")
	return nil
}
