package main

import (
	_ "github.com/aaronland/go-pool-boltdb"
)

import (
	"context"
	"flag"
	"fmt"
	"github.com/aaronland/go-artisanal-integers-proxy"
	"github.com/aaronland/go-pool"
	"github.com/whosonfirst/go-whosonfirst-log"
	golog "log"
	"os"
	"time"
)

func main() {

	dsn := flag.String("dsn", "integers.db", "...")
	min_count := flag.Int("min-count", 100, "...")
	workers := flag.Int("workers", 10, "...")

	flag.Parse()

	ctx := context.Background()

	logger := log.NewWOFLogger("prefill")
	logger.AddLogger(os.Stdout, "warning")

	proxy_dsn := fmt.Sprintf("boltdb://integers?dsn=%s", *dsn)
	proxy_pool, err := pool.NewPool(ctx, proxy_dsn)

	if err != nil {
		golog.Fatal(err)
	}

	proxy_args := proxy.ProxyServiceArgs{
		BrooklynIntegers: true,
		MinCount:         *min_count,
		// 		Logger:           logger,
		Workers: *workers,
	}

	_, err = proxy.NewProxyServiceWithPool(proxy_pool, proxy_args)

	if err != nil {
		golog.Fatal(err)
	}

	t1 := time.Now()
	done_ch := make(chan bool)

	go func() {

		ticker := time.Tick(5 * time.Second)

		for range ticker {

			select {
			case <-done_ch:
				break
			default:
				// pass
			}

			count := proxy_pool.Length()
			golog.Printf("Pre-cache %d integers: %v\n", count, time.Since(t1))
		}

	}()

	ticker := time.Tick(1 * time.Second)

	for range ticker {

		count := proxy_pool.Length()

		if count >= int64(proxy_args.MinCount) {
			done_ch <- true
			break
		}
	}

	golog.Printf("Time to pre-cache %d integers: %v\n", proxy_pool.Length(), time.Since(t1))

}
