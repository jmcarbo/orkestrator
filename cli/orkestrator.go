package main

import (
	"fmt"
	log "github.com/Sirupsen/logrus"
	"github.com/codegangsta/cli"
	"github.com/jmcarbo/orkestrator"
	"github.com/nu7hatch/gouuid"
	"os"
	"time"
)

func main() {
	app := cli.NewApp()
	app.Name = "orkestrator"
	app.Usage = "orchestrate consul cluster!"
	app.Version = "0.0.1"
	app.Flags = []cli.Flag{
		cli.StringFlag{
			Name:  "scheduler",
			Value: "main_scheduler",
			Usage: "scheduler name",
		},
		cli.StringFlag{
			Name:  "node",
			Value: "",
			Usage: "node name",
		},
	}
	app.Commands = []cli.Command{
		{
			Name:      "version",
			ShortName: "v",
			Usage:     "orchestrator version",
			Action: func(c *cli.Context) {
				fmt.Println(app.Version)
			},
		},
		{
			Name:      "start",
			ShortName: "s",
			Usage:     "start scheduler",
			Action: func(c *cli.Context) {
				client := orkestrator.Connect()
				sche := orkestrator.NewScheduler(c.GlobalString("scheduler"), client, c.GlobalString("node"))
				if sche != nil {
					log.Printf("Starting orkestrator scheduler %s with node %s ...\n", c.GlobalString("scheduler"), c.GlobalString("node"))
					ch := sche.Start()
					<-ch
				}
			},
		},
		{
			Name:      "stop",
			ShortName: "t",
			Usage:     "stop scheduler",
			Action: func(c *cli.Context) {
				client := orkestrator.Connect()
				sche := orkestrator.NewScheduler(c.GlobalString("scheduler"), client, c.GlobalString("node"))
				if sche != nil {
					log.Printf("Stoping orkestrator scheduler %s with node %s ...\n", c.GlobalString("scheduler"), c.GlobalString("node"))
					sche.Stop()
				}
			},
		},
		{
			Name:      "addjob",
			ShortName: "a",
			Usage:     "add job",
			Flags: []cli.Flag{
				cli.StringFlag{
					Name:  "id",
					Value: func() string { s, _ := uuid.NewV4(); return s.String() }(),
					Usage: "job id",
				},
				cli.StringFlag{
					Name:  "command",
					Value: "echo hello world",
					Usage: "job command",
				},
				cli.StringFlag{
					Name:  "check",
					Value: "",
					Usage: "check command",
				},
				cli.StringFlag{
					Name:  "check_interval",
					Value: "30s",
					Usage: "check interval (ex: 2s)",
				},
				cli.BoolFlag{
					Name:  "log",
					Usage: "Log job output",
				},
				cli.StringFlag{
					Name:  "timeout",
					Value: "30s",
					Usage: "job timeout (ex: 30s)",
				},
			},
			Action: func(c *cli.Context) {
				client := orkestrator.Connect()
				sche := orkestrator.NewScheduler(c.GlobalString("scheduler"), client, c.GlobalString("node"))
				if sche != nil {
					log.Printf("Adding job to orkestrator scheduler %s...\n", c.GlobalString("scheduler"))
					var job orkestrator.Job
					job.ID = c.String("id")
					job.Name = c.String("id")
					job.Command = c.String("command")
					job.LogOutput = c.Bool("log")
					job.CheckCommand = c.String("check")
					job.CheckInterval = c.String("check_interval")
					dur, err := time.ParseDuration(c.String("timeout"))
					if err != nil {
						log.Fatal("invalid timeout")
					}
					job.Timeout = dur
					err = sche.AddJob(&job)
					if err != nil {
						log.Println(err)
					}
				}
			},
		},
	}
	app.Run(os.Args)
}

/* SCRATCH CODE TO DELETE

client, err := consulapi.NewClient(consulapi.DefaultConfig())
if err != nil {
	panic(err)
}

kv := client.KV()
session := client.Session()
agent := client.Agent()

err = agent.CheckRegister(&consulapi.AgentCheckRegistration{"blac", "blac", "", consulapi.AgentServiceCheck{TTL: "10s"}})
if err != nil {
	panic(err)
}
ses, _, err := session.Create(&consulapi.SessionEntry{Checks: []string{"blac"}}, nil)
if err != nil {
	panic(err)
}

ses2, _, err := session.CreateNoChecks(nil, nil)
if err != nil {
	panic(err)
}

k := &consulapi.KVPair{Key: "bla1", Value: []byte("Hola que tal"), Session: ses}
res, _, err := kv.Acquire(k, nil)
log.Printf("%v\n", res)
k.Session = ses2

res, _, err = kv.Acquire(k, nil)
log.Printf("%v\n", res)

k2, _, err := kv.Get("a/b/c", nil)
if k2 == nil {
	k2 = &consulapi.KVPair{Key: "a/b/c", Value: []byte("a taylor is rich")}
}
res, _, err = kv.CAS(k2, nil)
if err != nil {
	panic(err)
}
log.Printf("%v\n", res)
res, _, err = kv.CAS(k2, nil)
if err != nil {
	panic(err)
}
log.Printf("%v\n", res)
*/
