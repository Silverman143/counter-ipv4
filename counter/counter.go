package counter

import (
	"fmt"
	"math/bits"
	"os"
	"os/exec"
	"runtime"
	"runtime/debug"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"golang.org/x/sys/unix"
)

const (
	bitmapWords   = 1 << 26
	wordCacheSize = 4096
	adviseWindow  = 64 << 20
)

type shardItem struct {
	buf []uint32
}

type IPv4Counter struct {
	Parsers          int
	Shards           int
	Batch            int
	ChanBuf          int
	LogInterval      int64
	AccessSequential bool
}

func New(parsers, shards, batch, chanBuf int, logInterval int64) *IPv4Counter {
	if parsers <= 0 {
		parsers = runtime.NumCPU()
	}
	if shards <= 0 {
		shards = 8
	}
	if batch < 128*1024 {
		batch = 128 * 1024
	}
	if chanBuf < 2048 {
		chanBuf = 2048
	}
	return &IPv4Counter{
		Parsers:          parsers,
		Shards:           shards,
		Batch:            batch,
		ChanBuf:          chanBuf,
		LogInterval:      logInterval,
		AccessSequential: false,
	}
}

type agg struct {
	total   int64
	valid   int64
	invalid int64
}

func (a *agg) add(dt, dv, di int64) {
	if dt != 0 {
		atomic.AddInt64(&a.total, dt)
	}
	if dv != 0 {
		atomic.AddInt64(&a.valid, dv)
	}
	if di != 0 {
		atomic.AddInt64(&a.invalid, di)
	}
}
func (a *agg) snap() (t, v, i int64) {
	return atomic.LoadInt64(&a.total), atomic.LoadInt64(&a.valid), atomic.LoadInt64(&a.invalid)
}

func (c *IPv4Counter) CountUnique(f *os.File) (uint64, error) {
	fi, err := f.Stat()
	if err != nil {
		return 0, err
	}
	size := int(fi.Size())
	if size == 0 {
		return 0, nil
	}

	data, err := unix.Mmap(int(f.Fd()), 0, size, unix.PROT_READ, unix.MAP_SHARED)
	if err != nil {
		return 0, err
	}
	defer unix.Munmap(data)

	if c.AccessSequential {
		_ = unix.Madvise(data, unix.MADV_SEQUENTIAL)
	} else {
		_ = unix.Madvise(data, unix.MADV_RANDOM)
	}

	old := debug.SetGCPercent(200)
	defer debug.SetGCPercent(old)
	runtime.GOMAXPROCS(runtime.NumCPU())

	parsers := max(1, c.Parsers)
	shards := max(1, c.Shards)
	wordsPerShard := bitmapWords / shards
	localMask := wordsPerShard - 1
	wordShift := bits.TrailingZeros(uint(wordsPerShard))
	batch := max(c.Batch, 4096)
	chanBuf := max(c.ChanBuf, 64)

	shardWords := make([][]uint64, shards)
	for s := 0; s < shards; s++ {
		shardWords[s] = make([]uint64, wordsPerShard)
	}
	perShardCap := max(batch/shards, 1024)
	bufPool := &sync.Pool{New: func() any { return make([]uint32, 0, perShardCap) }}
	shardCh := make([]chan shardItem, shards)
	for s := 0; s < shards; s++ {
		shardCh[s] = make(chan shardItem, chanBuf)
	}

	var shardWG sync.WaitGroup
	shardWG.Add(shards)
	for s := 0; s < shards; s++ {
		go shardWorker(shardWords[s], shardCh[s], &shardWG, localMask, bufPool)
	}

	type seg struct{ s, e int }
	segs := make([]seg, 0, parsers)
	chunk := size / parsers
	cur := 0
	for p := 0; p < parsers; p++ {
		s := cur
		e := s + chunk
		if p == parsers-1 {
			e = size
		}
		if p > 0 {
			s = alignToNextNewline(data, s, size)
		}
		if e < size {
			e = alignToNextNewline(data, e, size)
		}
		if s > e {
			s = e
		}
		segs = append(segs, seg{s, e})
		cur = e
	}

	var totals agg
	start, done := startLoggers(c.LogInterval, &totals, uint64(bitmapWords*8), uint64(size))

	var parserWG sync.WaitGroup
	parserWG.Add(parsers)
	for i := 0; i < parsers; i++ {
		sg := segs[i]
		go parserWorkerFull(data, sg.s, sg.e, shards, wordShift, shardCh, batch, &parserWG, bufPool, &totals)
	}

	parserWG.Wait()
	for s := 0; s < shards; s++ {
		close(shardCh[s])
	}
	shardWG.Wait()
	close(done)

	uniq := popcountAll(shardWords)

	elapsed := time.Since(start)
	t, v, inv := totals.snap()
	fmt.Println()
	fmt.Println("═══════════════════════════════════════")
	fmt.Println("          PROCESSING RESULTS")
	fmt.Println("═══════════════════════════════════════")
	fmt.Printf("Duration:         %v\n", elapsed.Truncate(time.Millisecond))
	fmt.Printf("Total lines:      %s\n", formatNumber(t))
	fmt.Printf("Valid IPs:        %s\n", formatNumber(v))
	fmt.Printf("Invalid lines:    %s\n", formatNumber(inv))
	fmt.Printf("Unique IPs:       %s\n", formatNumber(int64(uniq)))
	if elapsed > 0 {
		fmt.Printf("Processing rate:  %.0f lines/sec\n", float64(t)/elapsed.Seconds())
	}
	return uniq, nil
}

func (c *IPv4Counter) CountUniqueChunked(f *os.File, windowBytes int) (uint64, error) {
	fi, err := f.Stat()
	if err != nil {
		return 0, err
	}
	fileSize := fi.Size()
	if fileSize == 0 {
		return 0, nil
	}

	parsers := max(1, c.Parsers)
	shards := max(1, c.Shards)
	wordsPerShard := bitmapWords / shards
	localMask := wordsPerShard - 1
	wordShift := bits.TrailingZeros(uint(wordsPerShard))
	batch := max(c.Batch, 128*1024)
	chanBuf := max(c.ChanBuf, 2048)

	shardWords := make([][]uint64, shards)
	for s := 0; s < shards; s++ {
		shardWords[s] = make([]uint64, wordsPerShard)
	}
	perShardCap := max(batch/shards, 1024)
	bufPool := &sync.Pool{New: func() any { return make([]uint32, 0, perShardCap) }}
	shardCh := make([]chan shardItem, shards)
	for s := 0; s < shards; s++ {
		shardCh[s] = make(chan shardItem, chanBuf)
	}
	var shardWG sync.WaitGroup
	shardWG.Add(shards)
	for s := 0; s < shards; s++ {
		go shardWorker(shardWords[s], shardCh[s], &shardWG, localMask, bufPool)
	}

	old := debug.SetGCPercent(200)
	defer debug.SetGCPercent(old)
	runtime.GOMAXPROCS(runtime.NumCPU())

	var totals agg
	start, done := startLoggers(c.LogInterval, &totals, uint64(bitmapWords*8), uint64(fileSize))

	page := unix.Getpagesize()
	if windowBytes < page {
		windowBytes = page
	}
	carry := make([]byte, 0, 64)

	var offset int64
	for offset < fileSize {
		mapOff := (offset / int64(page)) * int64(page)
		startInMap := int(offset - mapOff)
		want := startInMap + windowBytes
		if mapOff+int64(want) > fileSize {
			want = int(fileSize - mapOff)
		}
		if want <= 0 {
			break
		}
		data, err := unix.Mmap(int(f.Fd()), mapOff, want, unix.PROT_READ, unix.MAP_SHARED)
		if err != nil {
			for s := 0; s < shards; s++ {
				close(shardCh[s])
			}
			shardWG.Wait()
			close(done)
			return 0, err
		}
		if c.AccessSequential {
			_ = unix.Madvise(data, unix.MADV_SEQUENTIAL)
		} else {
			_ = unix.Madvise(data, unix.MADV_RANDOM)
		}

		view := data[startInMap:]

		if len(carry) > 0 {
			idx := -1
			for i := 0; i < len(view); i++ {
				if view[i] == '\n' {
					idx = i
					break
				}
			}
			if idx == -1 {
				carry = append(carry, view...)
				offset += int64(len(view))
				_ = unix.Munmap(data)
				continue
			}
			tmp := make([]byte, 0, len(carry)+idx)
			tmp = append(tmp, carry...)
			tmp = append(tmp, view[:idx]...)
			if ip, ok := parseIPv4FastStrict(tmp, 0, len(tmp)); ok {
				word := int(ip >> 6)
				shardID := word >> wordShift
				b := bufPool.Get().([]uint32)[:0]
				b = append(b, ip)
				shardCh[shardID] <- shardItem{buf: b}
				totals.add(1, 1, 0)
			} else {
				totals.add(1, 0, 1)
			}
			carry = carry[:0]
			view = view[idx+1:]
			offset += int64(idx + 1)
		}

		if len(view) == 0 {
			_ = unix.Munmap(data)
			continue
		}

		lastNL := -1
		for i := len(view) - 1; i >= 0; i-- {
			if view[i] == '\n' {
				lastNL = i
				break
			}
		}
		var parseEnd int
		if lastNL == -1 {
			carry = append(carry, view...)
			offset += int64(len(view))
			_ = unix.Munmap(data)
			continue
		} else {
			parseEnd = lastNL + 1
		}

		chunk := parseEnd / parsers
		type seg struct{ s, e int }
		segs := make([]seg, 0, parsers)
		cur := 0
		for p := 0; p < parsers; p++ {
			s := cur
			e := s + chunk
			if p == parsers-1 {
				e = parseEnd
			} else {
				for e < parseEnd && view[e] != '\n' {
					e++
				}
			}
			if s > e {
				s = e
			}
			segs = append(segs, seg{s, e})
			cur = e
		}

		var wg sync.WaitGroup
		wg.Add(parsers)
		for i := 0; i < parsers; i++ {
			sg := segs[i]
			go parserWorkerChunked(view, sg.s, sg.e, wordShift, shardCh, batch, &wg, bufPool, &totals)
		}
		wg.Wait()

		if parseEnd < len(view) {
			carry = append(carry, view[parseEnd:]...)
			offset += int64(len(view))
		} else {
			offset += int64(parseEnd)
		}
		_ = unix.Munmap(data)
	}

	for s := 0; s < shards; s++ {
		close(shardCh[s])
	}
	shardWG.Wait()
	close(done)

	uniq := popcountAll(shardWords)
	elapsed := time.Since(start)
	t, v, inv := totals.snap()
	fmt.Println()
	fmt.Println("═══════════════════════════════════════")
	fmt.Println("          PROCESSING RESULTS")
	fmt.Println("═══════════════════════════════════════")
	fmt.Printf("Duration:         %v\n", elapsed.Truncate(time.Millisecond))
	fmt.Printf("Total lines:      %s\n", formatNumber(t))
	fmt.Printf("Valid IPs:        %s\n", formatNumber(v))
	fmt.Printf("Invalid lines:    %s\n", formatNumber(inv))
	fmt.Printf("Unique IPs:       %s\n", formatNumber(int64(uniq)))
	if elapsed > 0 {
		fmt.Printf("Processing rate:  %.0f lines/sec\n", float64(t)/elapsed.Seconds())
	}
	return uniq, nil
}

func parserWorkerFull(
	data []byte,
	start, end int,
	shards int,
	wordShift int,
	shardCh []chan shardItem,
	batch int,
	wg *sync.WaitGroup,
	pool *sync.Pool,
	total *agg,
) {
	defer wg.Done()

	perShardCap := max(batch/shards, 1024)
	batches := make([][]uint32, shards)
	for i := 0; i < shards; i++ {
		b := pool.Get().([]uint32)[:0]
		if cap(b) < perShardCap {
			b = make([]uint32, 0, perShardCap)
		}
		batches[i] = b
	}

	page := unix.Getpagesize()
	alignDown := func(x int) int { return x - (x % page) }
	advCursor := start

	var lt, lv, li, lastFlush int64
	lineStart := start
	flushEvery := 1 << 20

	for i := start; i < end; i++ {
		if data[i] != '\n' {
			continue
		}
		lt++
		if lineStart < i {
			if ip, ok := parseIPv4FastStrict(data, lineStart, i); ok {
				lv++
				word := int(ip >> 6)
				shardID := word >> wordShift
				b := batches[shardID]
				b = append(b, ip)
				if len(b) >= cap(b) {
					shardCh[shardID] <- shardItem{buf: b}
					nb := pool.Get().([]uint32)[:0]
					if cap(nb) < perShardCap {
						nb = make([]uint32, 0, perShardCap)
					}
					batches[shardID] = nb
				} else {
					batches[shardID] = b
				}
			} else {
				li++
			}
		} else {
			li++
		}
		lineStart = i + 1

		if lt-lastFlush >= int64(flushEvery) {
			total.add(lt-lastFlush, lv, li)
			lastFlush = lt
			lv, li = 0, 0
		}

		processedEnd := i + 1
		if processedEnd-advCursor >= adviseWindow {
			cut := alignDown(processedEnd)
			if cut > advCursor {
				_ = unix.Madvise(data[advCursor:cut], unix.MADV_DONTNEED)
				advCursor = cut
			}
		}
	}
	if lineStart < end {
		lt++
		if ip, ok := parseIPv4FastStrict(data, lineStart, end); ok {
			lv++
			word := int(ip >> 6)
			shardID := word >> wordShift
			batches[shardID] = append(batches[shardID], ip)
		} else {
			li++
		}
	}
	total.add(lt-lastFlush, lv, li)

	for s := range batches {
		if len(batches[s]) > 0 {
			shardCh[s] <- shardItem{buf: batches[s]}
		} else {
			b := batches[s][:0]
			pool.Put(b)
		}
	}
	if advCursor < end {
		cut := alignDown(end)
		if cut > advCursor {
			_ = unix.Madvise(data[advCursor:cut], unix.MADV_DONTNEED)
		}
	}
}

func parserWorkerChunked(
	data []byte,
	start, end int,
	wordShift int,
	shardCh []chan shardItem,
	batch int,
	wg *sync.WaitGroup,
	pool *sync.Pool,
	total *agg,
) {
	defer wg.Done()

	shards := len(shardCh)
	perShardCap := max(batch/shards, 1024)
	batches := make([][]uint32, shards)
	for i := 0; i < shards; i++ {
		b := pool.Get().([]uint32)[:0]
		if cap(b) < perShardCap {
			b = make([]uint32, 0, perShardCap)
		}
		batches[i] = b
	}

	var lt, lv, li, lastFlush int64
	lineStart := start
	flushEvery := 1 << 20

	for i := start; i < end; i++ {
		if data[i] != '\n' {
			continue
		}
		lt++
		if lineStart < i {
			if ip, ok := parseIPv4FastStrict(data, lineStart, i); ok {
				lv++
				word := int(ip >> 6)
				shardID := word >> wordShift
				b := batches[shardID]
				b = append(b, ip)
				if len(b) >= cap(b) {
					shardCh[shardID] <- shardItem{buf: b}
					nb := pool.Get().([]uint32)[:0]
					if cap(nb) < perShardCap {
						nb = make([]uint32, 0, perShardCap)
					}
					batches[shardID] = nb
				} else {
					batches[shardID] = b
				}
			} else {
				li++
			}
		} else {
			li++
		}
		lineStart = i + 1

		if lt-lastFlush >= int64(flushEvery) {
			total.add(lt-lastFlush, lv, li)
			lastFlush = lt
			lv, li = 0, 0
		}
	}
	total.add(lt-lastFlush, lv, li)

	for s := range batches {
		if len(batches[s]) > 0 {
			shardCh[s] <- shardItem{buf: batches[s]}
		} else {
			b := batches[s][:0]
			pool.Put(b)
		}
	}
}

func shardWorker(words []uint64, in <-chan shardItem, wg *sync.WaitGroup, localMask int, pool *sync.Pool) {
	defer wg.Done()

	var keys [wordCacheSize]int
	var vals [wordCacheSize]uint64
	var touched [wordCacheSize]int
	touchedN := 0
	for i := range keys {
		keys[i] = -1
	}

	flush := func() {
		for i := 0; i < touchedN; i++ {
			idx := touched[i]
			if keys[idx] >= 0 {
				words[keys[idx]] |= vals[idx]
				keys[idx] = -1
				vals[idx] = 0
			}
		}
		touchedN = 0
	}

	for item := range in {
		b := item.buf
		for _, ip := range b {
			word := int(ip >> 6)
			local := word & localMask
			mask := uint64(1) << (ip & 63)
			i := local & (wordCacheSize - 1)

			if keys[i] == local {
				vals[i] |= mask
				continue
			}
			if keys[i] == -1 {
				keys[i] = local
				vals[i] = mask
				touched[touchedN] = i
				touchedN++
				continue
			}
			words[keys[i]] |= vals[i]
			keys[i] = local
			vals[i] = mask
		}
		flush()

		b = b[:0]
		pool.Put(b)
	}
}

func parseIPv4FastStrict(data []byte, start, end int) (uint32, bool) {
	n := end - start
	if n < 7 || n > 15 {
		return 0, false
	}
	d1, d2, d3 := -1, -1, -1
	for i := start; i < end; i++ {
		if data[i] == '.' {
			if d1 == -1 {
				d1 = i
			} else if d2 == -1 {
				d2 = i
			} else if d3 == -1 {
				d3 = i
			} else {
				return 0, false
			}
		}
	}
	if d3 == -1 {
		return 0, false
	}
	a, ok := parseOctet3(data, start, d1)
	if !ok {
		return 0, false
	}
	b, ok := parseOctet3(data, d1+1, d2)
	if !ok {
		return 0, false
	}
	c, ok := parseOctet3(data, d2+1, d3)
	if !ok {
		return 0, false
	}
	d, ok := parseOctet3(data, d3+1, end)
	if !ok {
		return 0, false
	}
	return (a << 24) | (b << 16) | (c << 8) | d, true
}

func parseOctet3(data []byte, s, e int) (uint32, bool) {
	l := e - s
	if l < 1 || l > 3 {
		return 0, false
	}
	if l == 1 {
		b := data[s]
		if b < '0' || b > '9' {
			return 0, false
		}
		return uint32(b - '0'), true
	}
	if l == 2 {
		b0, b1 := data[s], data[s+1]
		if b0 < '0' || b0 > '9' || b1 < '0' || b1 > '9' {
			return 0, false
		}
		return uint32(b0-'0')*10 + uint32(b1-'0'), true
	}
	b0, b1, b2 := data[s], data[s+1], data[s+2]
	if b0 < '0' || b0 > '9' || b1 < '0' || b1 > '9' || b2 < '0' || b2 > '9' {
		return 0, false
	}
	if b0 > '2' {
		return 0, false
	}
	if b0 == '2' && (b1 > '5' || (b1 == '5' && b2 > '5')) {
		return 0, false
	}
	a := uint32(b0 - '0')
	v := (a<<3 + a<<1) + uint32(b1-'0')
	v = (v<<3 + v<<1) + uint32(b2-'0')
	return v, true
}

func alignToNextNewline(data []byte, pos, limit int) int {
	if pos <= 0 {
		return 0
	}
	if pos >= limit {
		return limit
	}
	for pos < limit {
		if data[pos-1] == '\n' {
			break
		}
		pos++
	}
	return pos
}

func popcountAll(shardWords [][]uint64) uint64 {
	var uniq uint64
	var wg sync.WaitGroup
	results := make(chan uint64, len(shardWords))
	wg.Add(len(shardWords))
	for s := 0; s < len(shardWords); s++ {
		go func(words []uint64) {
			defer wg.Done()
			var local uint64
			for i := range words {
				local += uint64(bits.OnesCount64(words[i]))
			}
			results <- local
		}(shardWords[s])
	}
	wg.Wait()
	close(results)
	for v := range results {
		uniq += v
	}
	return uniq
}

func startLoggers(logEvery int64, totals *agg, bitsetBytes, mappedBytes uint64) (time.Time, chan struct{}) {
	start := time.Now()
	done := make(chan struct{})
	if logEvery > 0 {
		go func() {
			next := logEvery
			t := time.NewTicker(200 * time.Millisecond)
			defer t.Stop()
			for {
				select {
				case <-done:
					return
				case <-t.C:
					tl, vl, il := totals.snap()
					if tl >= next {
						el := time.Since(start).Seconds()
						if el < 1 {
							el = 1
						}
						fmt.Printf("%s lines | %s valid | %s invalid | %.0f lines/sec | %.0fs elapsed\n",
							formatNumber(tl), formatNumber(vl), formatNumber(il),
							float64(tl)/el, el)
						next += logEvery
					}
				}
			}
		}()
	}
	go func() {
		t := time.NewTicker(2 * time.Second)
		defer t.Stop()
		for {
			select {
			case <-done:
				return
			case <-t.C:
				alloc, sys, numGC := heapSnapshot()
				rss := rssBytes()
				fmt.Printf("mem: heap_alloc=%s heap_sys=%s gc=%d | rss=%s | bitset=%s | mmap=%s\n",
					humanBytes(alloc), humanBytes(sys), numGC,
					humanBytes(rss),
					humanBytes(bitsetBytes), humanBytes(mappedBytes),
				)
			}
		}
	}()
	return start, done
}

func formatNumber(n int64) string {
	s := fmt.Sprintf("%d", n)
	if len(s) <= 3 {
		return s
	}
	var out strings.Builder
	rem := len(s) % 3
	if rem == 0 {
		rem = 3
	}
	out.WriteString(s[:rem])
	for i := rem; i < len(s); i += 3 {
		out.WriteByte(',')
		out.WriteString(s[i : i+3])
	}
	return out.String()
}

func max(a, b int) int {
	if a > b {
		return a
	}
	return b
}

func humanBytes(b uint64) string {
	const unit = 1024
	if b < unit {
		return fmt.Sprintf("%d B", b)
	}
	div, exp := uint64(unit), 0
	for n := b / unit; n >= unit; n /= unit {
		div *= unit
		exp++
	}
	return fmt.Sprintf("%.1f %cB", float64(b)/float64(div), "KMGTPE"[exp])
}

func heapSnapshot() (alloc, sys uint64, numGC uint32) {
	var m runtime.MemStats
	runtime.ReadMemStats(&m)
	return m.Alloc, m.Sys, m.NumGC
}

func rssBytes() uint64 {
	pid := os.Getpid()
	out, err := exec.Command("ps", "-o", "rss=", "-p", strconv.Itoa(pid)).Output()
	if err != nil {
		return 0
	}
	s := strings.TrimSpace(string(out))
	kb, err := strconv.ParseUint(s, 10, 64)
	if err != nil {
		return 0
	}
	return kb * 1024
}
