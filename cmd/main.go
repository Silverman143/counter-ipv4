package main

import (
	"counter-ip/counter"
	"flag"
	"fmt"
	"log/slog"
	"os"
	"time"
)

func main() {
	var (
		mode         = flag.String("mode", "full", "processing mode: full | chunked")
		windowGB     = flag.Int("window-gb", 1, "chunked mode: mmap window size in GB (e.g. 1..4)")
		parsers      = flag.Int("parsers", 0, "number of parser goroutines (0 = NumCPU)")
		shards       = flag.Int("shards", 256, "number of bitset shards (power of two is best)")
		batch        = flag.Int("batch", 4*1024, "per-parser batch size (IPs)")
		chanBuf      = flag.Int("chanbuf", 64, "per-shard channel buffer")
		progressEvery = flag.Int64("progress-every", 100_000_000, "log every N lines (0=off)")
		sequential   = flag.Bool("sequential", false, "madvise: true=SEQUENTIAL, false=RANDOM")
	)
	flag.Usage = func() {
		fmt.Fprintf(os.Stderr, "Usage: %s [flags] <path-to-file>\n", os.Args[0])
		flag.PrintDefaults()
	}
	flag.Parse()

	if flag.NArg() < 1 {
		flag.Usage()
		os.Exit(2)
	}
	path := flag.Arg(0)

	f, err := os.Open(path)
	if err != nil {
		slog.Error("failed to open file", "path", path, "err", err)
		os.Exit(1)
	}
	defer f.Close()

	c := counter.New(*parsers, *shards, *batch, *chanBuf, *progressEvery)
	c.AccessSequential = *sequential

	start := time.Now()

	var uniq uint64
	switch *mode {
	case "full":
		uniq, err = c.CountUnique(f)
	case "chunked":
		if *windowGB <= 0 {
			slog.Error("window-gb must be > 0")
			os.Exit(2)
		}
		windowBytes := *windowGB * (1 << 30) // GB -> bytes
		uniq, err = c.CountUniqueChunked(f, windowBytes)
	default:
		slog.Error("unknown mode", "mode", *mode)
		os.Exit(2)
	}

	if err != nil {
		slog.Error("count failed", "err", err)
		os.Exit(1)
	}

	slog.Info("Complete",
		"mode", *mode,
		"unique_ips", uniq,
		"elapsed", time.Since(start),
	)
}
