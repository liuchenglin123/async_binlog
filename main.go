package main

import (
	"fmt"
	"log"
	"os"
	"os/signal"
	"time"

	"mysqlbinlog/boot"
	"mysqlbinlog/repo"
	"mysqlbinlog/utils"
)

func main() {
	err := utils.InitConf()
	if err != nil {
		log.Panic(err)
		return
	}
	defer utils.SqlFile.Close()

	over := make(chan struct{})
	go func() {
		defer func() {
			if err := recover(); err != nil {
				defer utils.SqlFile.Close()
			}
			over <- struct{}{}
			close(over)
			os.Exit(1)
		}()
		boot.Run(repo.NewRow(), over)
	}()
	quit := make(chan os.Signal)
	signal.Notify(quit, os.Interrupt)
	<-quit
	over <- struct{}{}
	close(over)
	fmt.Println("Shutdown Server ...")
	time.Sleep(time.Second)
}
