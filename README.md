# ha-go
use multicast achieving master-backup auto switch when the master node is down

## Usage

```golang
// create a node
node, err := ha.NewNode(context.Background(), 100, "224.0.0.18", "2345", "master", 1*time.Second, 5*time.Second, 5*time.Second)

// start listen
go func() {
    err = node.Listen(nil)
    if err != nil {
        fmt.Println(err.Error())
        node.Stop()
    }
}()
// start watch and run
go func() {
	//WatchAndRun will auto switch mode between master, candidate and backup
    err = node.WatchAndRun(job, false, 0)
    if err != nil {
        fmt.Println(err.Error())
        node.Stop()
    }
}()
```
