/*
 * Warp (C) 2019-2020 MinIO, Inc.
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */

package cli

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"strings"
	"sync"
	"time"

	tea "github.com/charmbracelet/bubbletea"
	"github.com/klauspost/compress/zstd"
	"github.com/minio/cli"
	"github.com/minio/madmin-go/v4"
	"github.com/minio/mc/pkg/probe"
	"github.com/minio/pkg/v3/console"
	"github.com/minio/warp/api"
	"github.com/minio/warp/pkg/aggregate"
	"github.com/minio/warp/pkg/bench"
	"github.com/minio/warp/pkg/generator"
)

// SerializableObject represents an object that can be saved to/loaded from JSON
type SerializableObject struct {
	Name        string `json:"name"`
	ContentType string `json:"content_type"`
	Prefix      string `json:"prefix"`
	VersionID   string `json:"version_id"`
	Size        int64  `json:"size"`
}

// SerializableObjects represents a collection of serializable objects
type SerializableObjects struct {
	Objects map[string]SerializableObject `json:"objects"`
}

// saveObjectsToFile saves objects to a JSON file
func saveObjectsToFile(objects generator.Objects, filename string) error {
	serializable := SerializableObjects{
		Objects: make(map[string]SerializableObject),
	}

	for _, obj := range objects {
		serializable.Objects[obj.Name] = SerializableObject{
			Name:        obj.Name,
			ContentType: obj.ContentType,
			Prefix:      obj.Prefix,
			VersionID:   obj.VersionID,
			Size:        obj.Size,
		}
	}

	file, err := os.Create(filename)
	if err != nil {
		return err
	}
	defer file.Close()

	encoder := json.NewEncoder(file)
	encoder.SetIndent("", "  ")
	return encoder.Encode(serializable)
}

// saveObjectsMapToFile saves objects map to a JSON file (for MixedDistribution)
func saveObjectsMapToFile(objectsMap map[string]generator.Object, filename string) error {
	serializable := SerializableObjects{
		Objects: make(map[string]SerializableObject),
	}

	for _, obj := range objectsMap {
		serializable.Objects[obj.Name] = SerializableObject{
			Name:        obj.Name,
			ContentType: obj.ContentType,
			Prefix:      obj.Prefix,
			VersionID:   obj.VersionID,
			Size:        obj.Size,
		}
	}

	file, err := os.Create(filename)
	if err != nil {
		return err
	}
	defer file.Close()

	encoder := json.NewEncoder(file)
	encoder.SetIndent("", "  ")
	return encoder.Encode(serializable)
}

// loadObjectsFromFile loads objects from a JSON file into a MixedDistribution
func loadObjectsFromFile(filename string, dist *bench.MixedDistribution) error {
	file, err := os.Open(filename)
	if err != nil {
		return err
	}
	defer file.Close()

	var serializable SerializableObjects
	decoder := json.NewDecoder(file)
	err = decoder.Decode(&serializable)
	if err != nil {
		return err
	}

	// Create new objects map
	objectsMap := make(map[string]generator.Object)

	// Convert serializable objects back to generator.Objects
	for _, sObj := range serializable.Objects {
		obj := generator.Object{
			Name:        sObj.Name,
			ContentType: sObj.ContentType,
			Prefix:      sObj.Prefix,
			VersionID:   sObj.VersionID,
			Size:        sObj.Size,
			Reader:      nil, // Reader is always nil after uploads
		}
		objectsMap[obj.Name] = obj
	}

	// Use the new LoadObjectsMap method
	dist.LoadObjectsMap(objectsMap)

	// CRITICAL: Regenerate the operations distribution after loading objects
	// The RegenerateOps method creates the ops slice that drives the benchmark
	// without overwriting the objects we just loaded
	err = dist.RegenerateOps()
	if err != nil {
		return fmt.Errorf("failed to regenerate operations: %w", err)
	}

	return nil
}

var benchFlags = []cli.Flag{
	cli.StringFlag{
		Name:  "benchdata",
		Value: "",
		Usage: "Output benchmark+profile data to this file. By default unique filename is generated.",
	},
	cli.StringFlag{
		Name:  "serverprof",
		Usage: "Run MinIO server profiling during benchmark; possible values are 'cpu', 'mem', 'block', 'mutex' and 'trace'.",
		Value: "",
	},
	cli.DurationFlag{
		Name:  "duration",
		Usage: "Duration to run the benchmark. Use 's' and 'm' to specify seconds and minutes.",
		Value: 5 * time.Minute,
	},
	cli.BoolFlag{
		Name:  "autoterm",
		Usage: "Auto terminate when benchmark is considered stable.",
	},
	cli.DurationFlag{
		Name:  "autoterm.dur",
		Usage: "Minimum duration where output must have been stable to allow automatic termination.",
		Value: 15 * time.Second,
	},
	cli.Float64Flag{
		Name:  "autoterm.pct",
		Usage: "The percentage the last 6/25 time blocks must be within current speed to auto terminate.",
		Value: 7.5,
	},
	cli.BoolFlag{
		Name:  "noclear",
		Usage: "Do not clear bucket before or after running benchmarks. Use when running multiple clients.",
	},
	cli.BoolFlag{
		Name:   "keep-data",
		Usage:  "Leave benchmark data. Do not run cleanup after benchmark. Bucket will still be cleaned prior to benchmark",
		Hidden: true,
	},
	cli.StringFlag{
		Name:  "syncstart",
		Usage: "Specify a benchmark start time. Time format is 'hh:mm' where hours are specified in 24h format, server TZ.",
		Value: "",
	},
	cli.StringFlag{
		Name:   "warp-client",
		Usage:  "Connect to warp clients and run benchmarks there.",
		EnvVar: "",
		Value:  "",
	},
	cli.StringSliceFlag{
		Name:   "add-metadata",
		Usage:  "Add user metadata to all objects using the format <key>=<value>. Random value can be set with 'rand:%length'. Can be used multiple times. Example: --add-metadata foo=bar --add-metadata randomValue=rand:1024.",
		Hidden: true,
	},
	cli.StringSliceFlag{
		Name:   "tag",
		Usage:  "Add user tag to all objects using the format <key>=<value>. Random value can be set with 'rand:%length'. Can be used multiple times. Example: --tag foo=bar --tag randomValue=rand:1024.",
		Hidden: true,
	},
	cli.StringFlag{
		Name:  "stage",
		Usage: "Only run specific stage: prepare, benchmark, or cleanup",
		Value: "",
	},
	cli.StringFlag{
		Name:  "objects-file",
		Usage: "File to save/load object metadata when using --stage. Enables running prepare and benchmark as separate commands.",
		Value: "",
	},
}

// runBench will run the supplied benchmark and save/print the analysis.
func runBench(ctx *cli.Context, b bench.Benchmark) error {
	defer globalWG.Wait()
	activeBenchmarkMu.Lock()
	ab := activeBenchmark
	activeBenchmarkMu.Unlock()
	c := b.GetCommon()
	c.Error = printError
	if ab != nil {
		c.ClientIdx = ab.clientIdx
		return runClientBenchmark(ctx, b, ab)
	}
	if done, err := runServerBenchmark(ctx, b); done || err != nil {
		// Close all extra output channels so the benchmark will terminate
		for _, out := range c.ExtraOut {
			close(out)
		}
		fatalIf(probe.NewError(err), "Error running remote benchmark")
		return nil
	}

	// Check for stage filtering
	onlyStage := ctx.String("stage")
	var runPrepare, runBenchmark, runCleanup bool

	if onlyStage == "" {
		// No stage specified, run all stages
		runPrepare = true
		runBenchmark = true
		runCleanup = true
	} else {
		// Only run the specified stage
		runPrepare = (onlyStage == stagePrepare.String())
		runBenchmark = (onlyStage == stageBenchmark.String())
		runCleanup = (onlyStage == stageCleanup.String())

		// Validate stage name
		if !runPrepare && !runBenchmark && !runCleanup {
			fatalIf(probe.NewError(fmt.Errorf("invalid stage: %s. Valid stages are: prepare, benchmark, cleanup", onlyStage)), "Invalid stage")
		}
	}

	var ui ui
	if !globalQuiet && !globalJSON {
		go ui.Run()
	}

	retrieveOps, updates := addCollector(ctx, b)
	c.UpdateStatus = ui.SetSubText

	monitor := api.NewBenchmarkMonitor(ctx.String(serverFlagName), updates)
	monitor.SetLnLoggers(func(data ...interface{}) {
		ui.SetSubText(strings.TrimRight(fmt.Sprintln(data...), "\r\n."))
	}, printError)
	defer monitor.Done()

	// PREPARE STAGE
	if runPrepare {
		monitor.InfoLn("Preparing server")
		c.Clear = !ctx.Bool("noclear")
		if ctx.Bool("autoterm") {
			c.AutoTermDur = ctx.Duration("autoterm.dur")
			c.AutoTermScale = ctx.Float64("autoterm.pct") / 100
		}
		c.PrepareProgress = make(chan float64, 1)
		ui.StartPrepare("Preparing", c.PrepareProgress, updates)

		err := b.Prepare(context.Background())
		fatalIf(probe.NewError(err), "Error preparing server")
		if c.PrepareProgress != nil {
			close(c.PrepareProgress)
		}

		if ap, ok := b.(AfterPreparer); ok {
			err := ap.AfterPrepare(context.Background())
			fatalIf(probe.NewError(err), "Error preparing server")
		}

		if onlyStage != "" {
			// Save objects to file for Mixed benchmarks if objects-file is specified
			if objFile := ctx.String("objects-file"); objFile != "" && ctx.Command.Name == "mixed" {
				if mixedBench, ok := b.(*bench.Mixed); ok {
					monitor.InfoLn("Saving object metadata to", objFile)
					err := saveObjectsMapToFile(mixedBench.Dist.GetObjectsMap(), objFile)
					if err != nil {
						monitor.Errorln("Failed to save objects:", err)
					} else {
						monitor.InfoLn("Objects saved successfully")
					}
				}
			}
			monitor.InfoLn("Prepare stage completed.")
			ui.Wait()
			return nil
		}
	}

	// BENCHMARK STAGE
	if runBenchmark {
		// Load objects from file for Mixed benchmarks if objects-file is specified and running benchmark-only
		if onlyStage == stageBenchmark.String() && ctx.String("objects-file") != "" && ctx.Command.Name == "mixed" {
			if mixedBench, ok := b.(*bench.Mixed); ok {
				objFile := ctx.String("objects-file")
				monitor.InfoLn("Loading object metadata from", objFile)
				err := loadObjectsFromFile(objFile, mixedBench.Dist)
				if err != nil {
					fatalIf(probe.NewError(err), "Failed to load objects from file")
				} else {
					monitor.InfoLn("Objects loaded successfully")
				}
			}
		}

		// Check if we're running benchmark-only for commands that need prepared objects
		if onlyStage == stageBenchmark.String() {
			cmdName := ctx.Command.Name
			needsObjects := []string{"mixed", "get", "stat", "delete", "versioned"}
			for _, needsObj := range needsObjects {
				if cmdName == needsObj {
					// Only show warning if objects-file is not specified for mixed benchmark
					if cmdName == "mixed" && ctx.String("objects-file") != "" {
						// Skip warning since we're loading objects from file
						continue
					}
					monitor.InfoLn(fmt.Sprintf("Warning: Running '%s --stage benchmark' requires objects from the prepare stage.", cmdName))
					monitor.InfoLn("Either run prepare stage first, or run without --stage to include all stages.")
					monitor.InfoLn("If objects already exist, use --noclear flag to preserve them.")
					if cmdName == "mixed" {
						monitor.InfoLn("Alternatively, use --objects-file to save/load objects between prepare and benchmark stages.")
					}
				}
			}
		}

		// Start after waiting a second or until we reached the start time.
		tStart := time.Now().Add(time.Second * 3)
		if st := ctx.String("syncstart"); st != "" {
			startTime := parseLocalTime(st)
			now := time.Now()
			if startTime.Before(now) {
				monitor.Errorln("Did not manage to prepare before syncstart")
				tStart = time.Now()
			} else {
				tStart = startTime
			}
		}
		if u := ui.updates.Load(); u != nil {
			*u <- aggregate.UpdateReq{Reset: true}
		}
		benchDur := ctx.Duration("duration")
		ui.StartBenchmark("Benchmarking", tStart, tStart.Add(benchDur), updates)
		ctx2, cancel := context.WithDeadline(context.Background(), tStart.Add(benchDur))
		defer cancel()
		ui.cancelFn.Store(&cancel)
		start := make(chan struct{})
		go func() {
			monitor.InfoLn("Pausing before benchmark")
			<-time.After(time.Until(tStart))
			monitor.InfoLn("Press 'q' to abort benchmark and print partial results")
			close(start)
		}()

		fileName := ctx.String("benchdata")
		cID := pRandASCII(4)
		if fileName == "" {
			fileName = fmt.Sprintf("%s-%s-%s-%s", appName, ctx.Command.Name, time.Now().Format("2006-01-02[150405]"), cID)
		}

		prof, err := startProfiling(ctx2, ctx)
		fatalIf(probe.NewError(err), "Unable to start profile.")
		monitor.InfoLn("Starting benchmark in", time.Until(tStart).Round(time.Second))
		b.Start(ctx2, start)
		c.Collector.Close()
		cancel()

		ctx2 = context.Background()
		prof.stop(ctx2, ctx, fileName+".profiles.zip")

		// Previous context is canceled, create a new...
		monitor.InfoLn("Saving benchmark data")
		if ops := retrieveOps(); len(ops) > 0 {
			ops.SortByStartTime()
			ops.SetClientID(cID)

			if len(ops) > 0 {
				f, err := os.Create(fileName + ".csv.zst")
				if err != nil {
					monitor.Errorln("Unable to write benchmark data:", err)
				} else {
					func() {
						defer f.Close()
						enc, err := zstd.NewWriter(f, zstd.WithEncoderLevel(zstd.SpeedBetterCompression))
						fatalIf(probe.NewError(err), "Unable to compress benchmark output")

						defer enc.Close()
						err = ops.CSV(enc, commandLine(ctx))
						fatalIf(probe.NewError(err), "Unable to write benchmark output")

						monitor.InfoLn(fmt.Sprintf("\nBenchmark data written to %q\n", fileName+".csv.zst"))
					}()
				}
			}
			monitor.OperationsReady(ops, fileName, commandLine(ctx))
			var buf bytes.Buffer
			printAnalysis(ctx, &buf, ops)
			ui.Update(tea.Quit())
			ui.Wait()
			fmt.Println(buf.String())
		} else if updates != nil {
			finalCh := make(chan *aggregate.Realtime, 1)
			updates <- aggregate.UpdateReq{Final: true, C: finalCh}
			final := <-finalCh
			final.Commandline = commandLine(ctx)
			final.WarpVersion = GlobalVersion
			final.WarpDate = GlobalDate
			final.WarpCommit = GlobalCommit
			f, err := os.Create(fileName + ".json.zst")
			if err != nil {
				monitor.Errorln("Unable to write benchmark data:", err)
			} else {
				func() {
					defer f.Close()
					enc, err := zstd.NewWriter(f, zstd.WithEncoderLevel(zstd.SpeedBetterCompression))
					if err != nil {
						monitor.Errorln("Unable to compress benchmark data:", err)
					}

					defer enc.Close()
					js := json.NewEncoder(enc)
					js.SetIndent("", "  ")
					err = js.Encode(final)
					if err != nil {
						monitor.Errorln("Unable to write benchmark data:", err)
					}

					monitor.InfoLn(fmt.Sprintf("\nBenchmark data written to %q\n\n", fileName+".json.zst"))
				}()
			}
			var rep *bytes.Buffer
			if globalJSON {
				rep = &bytes.Buffer{}
				enc := json.NewEncoder(rep)
				enc.SetIndent("", "  ")
				_ = enc.Encode(final)
			} else {
				rep = final.Report(aggregate.ReportOptions{
					Details: ctx.Bool("analyze.v"),
					Color:   !globalNoColor,
					OnlyOps: getAnalyzeOPS(ctx),
				})
			}

			monitor.UpdateAggregate(final, fileName)
			ui.Update(tea.Quit())
			ui.Wait()
			fmt.Println("")
			fmt.Println(rep)
		}

		if onlyStage != "" {
			monitor.InfoLn("Benchmark stage completed.")
			ui.Wait()
			return nil
		}
	}

	// CLEANUP STAGE
	if runCleanup {
		if !ctx.Bool("keep-data") && !ctx.Bool("noclear") {
			ui.SetPhase("Cleanup")
			monitor.InfoLn("Starting cleanup...")
			b.Cleanup(context.Background())
		}
		monitor.InfoLn("Cleanup Done.")
	}

	ui.Wait()
	return nil
}

var (
	activeBenchmarkMu sync.Mutex
	activeBenchmark   *clientBenchmark
)

type clientBenchmark struct {
	ctx       context.Context
	err       error
	cancel    context.CancelFunc
	info      map[benchmarkStage]stageInfo
	stage     benchmarkStage
	results   bench.Operations
	updates   chan<- aggregate.UpdateReq
	clientIdx int
	sync.Mutex
}

type stageInfo struct {
	start          chan struct{}
	done           chan struct{}
	custom         map[string]string
	stageCtx       context.Context
	cancelFn       context.CancelFunc
	startRequested bool
}

func (c *clientBenchmark) init(ctx context.Context) {
	c.results = nil
	c.err = nil
	c.stage = stageNotStarted
	c.info = make(map[benchmarkStage]stageInfo, len(benchmarkStages))
	c.ctx, c.cancel = context.WithCancel(ctx)
	for _, stage := range benchmarkStages {
		sCtx, sCancel := context.WithCancel(ctx)
		c.info[stage] = stageInfo{
			start:    make(chan struct{}),
			done:     make(chan struct{}),
			stageCtx: sCtx,
			cancelFn: sCancel,
		}
	}
}

// waitForStage waits for the stage to be ready and updates the stage when it is
func (c *clientBenchmark) waitForStage(s benchmarkStage) error {
	c.Lock()
	info, ok := c.info[s]
	ctx := c.ctx
	c.Unlock()
	if !ok {
		return errors.New("waitForStage: unknown stage")
	}
	select {
	case <-info.start:
		c.setStage(s)
	case <-ctx.Done():
		return ctx.Err()
	}
	return nil
}

// waitForStage waits for the stage to be ready and updates the stage when it is
func (c *clientBenchmark) stageDone(s benchmarkStage, err error, custom map[string]string) {
	console.Infoln("Stage", s, "done...")
	if err != nil {
		console.Errorln(err.Error())
	}
	c.Lock()
	info := c.info[s]
	info.custom = custom
	if err != nil && c.err == nil {
		c.err = err
	}
	if info.done != nil {
		close(info.done)
	}
	if info.cancelFn != nil {
		info.cancelFn()
	}
	c.info[s] = info
	c.Unlock()
}

func (c *clientBenchmark) setStage(s benchmarkStage) {
	c.Lock()
	c.stage = s
	c.Unlock()
}

type benchmarkStage string

const (
	stagePrepare    benchmarkStage = "prepare"
	stageBenchmark  benchmarkStage = "benchmark"
	stageCleanup    benchmarkStage = "cleanup"
	stageDone       benchmarkStage = "done"
	stageNotStarted benchmarkStage = ""
)

// String returns the string representation of the benchmark stage
func (bs benchmarkStage) String() string {
	return string(bs)
}

var benchmarkStages = []benchmarkStage{
	stagePrepare, stageBenchmark, stageCleanup,
}

func runClientBenchmark(ctx *cli.Context, b bench.Benchmark, cb *clientBenchmark) error {
	// Load objects from file FIRST for Mixed benchmarks if objects-file is specified (client-server mode)
	// This must happen before stage coordination begins
	if objFile := ctx.String("objects-file"); objFile != "" && ctx.Command.Name == "mixed" {
		if mixedBench, ok := b.(*bench.Mixed); ok {
			console.Infoln("Loading object metadata from", objFile)
			err := loadObjectsFromFile(objFile, mixedBench.Dist)
			if err != nil {
				console.Errorln("Failed to load objects:", err)
				// Continue anyway - maybe objects were created locally during prepare
			} else {
				console.Infoln("Objects loaded successfully")
			}
		}
	}

	// Check if we're running benchmark-only stage
	onlyStage := ctx.String("stage")
	skipPrepare := (onlyStage == stageBenchmark.String())

	if !skipPrepare {
		err := cb.waitForStage(stagePrepare)
		if err != nil {
			return err
		}
	}

	retrieveOps, updates := addCollector(ctx, b)
	common := b.GetCommon()
	common.UpdateStatus = func(s string) {
		console.Infoln(s)
	}
	defer common.Collector.Close()

	cb.Lock()
	benchStage := cb.info[stageBenchmark]
	start := benchStage.start
	cb.updates = updates
	cb.Unlock()

	if !skipPrepare {
		err := b.Prepare(cb.info[stagePrepare].stageCtx)
		cb.stageDone(stagePrepare, err, common.Custom)
		if err != nil {
			return err
		}

		// Save objects to file for Mixed benchmarks if objects-file is specified (client-server mode)
		if objFile := ctx.String("objects-file"); objFile != "" && ctx.Command.Name == "mixed" {
			if mixedBench, ok := b.(*bench.Mixed); ok {
				console.Infoln("Saving object metadata to", objFile)
				err := saveObjectsMapToFile(mixedBench.Dist.GetObjectsMap(), objFile)
				if err != nil {
					console.Errorln("Failed to save objects:", err)
				} else {
					console.Infoln("Objects saved successfully")
				}
			}
		}
	}

	ctx2, cancel := benchStage.stageCtx, benchStage.cancelFn
	defer cancel()

	// Start after waiting a second or until we reached the start time.
	benchDur := ctx.Duration("duration")
	go func() {
		console.Infoln("Waiting")
		// Wait for start signal
		select {
		case <-ctx2.Done():
			console.Infoln("Aborted")
			return
		case <-start:
		}
		console.Infoln("Starting")
		// Finish after duration
		select {
		case <-ctx2.Done():
			console.Infoln("Aborted")
			return
		case <-time.After(benchDur):
		}
		console.Infoln("Stopping")
		// Stop the benchmark
		cancel()
	}()

	fileName := ctx.String("benchdata")
	cID := pRandASCII(6)
	if fileName == "" {
		fileName = fmt.Sprintf("%s-%s-%s-%s", appName, ctx.Command.Name, time.Now().Format("2006-01-02[150405]"), cID)
	}

	err := b.Start(ctx2, start)
	ops := retrieveOps()
	cb.Lock()
	cb.results = ops
	cb.Unlock()
	cb.stageDone(stageBenchmark, err, common.Custom)
	if err != nil {
		return err
	}
	ops.SetClientID(cID)
	ops.SortByStartTime()
	common.Collector.Close()

	if len(ops) > 0 {
		f, err := os.Create(fileName + ".csv.zst")
		if err != nil {
			console.Error("Unable to write benchmark data:", err)
		} else {
			func() {
				defer f.Close()
				enc, err := zstd.NewWriter(f, zstd.WithEncoderLevel(zstd.SpeedBetterCompression))
				fatalIf(probe.NewError(err), "Unable to compress benchmark output")

				defer enc.Close()
				err = ops.CSV(enc, commandLine(ctx))
				fatalIf(probe.NewError(err), "Unable to write benchmark output")

				console.Infof("Benchmark data written to %q\n", fileName+".csv.zst")
			}()
		}
	} else if updates != nil {
		finalCh := make(chan *aggregate.Realtime, 1)
		updates <- aggregate.UpdateReq{Final: true, C: finalCh}
		final := <-finalCh
		final.Commandline = commandLine(ctx)
		final.WarpVersion = GlobalVersion
		final.WarpDate = GlobalDate
		final.WarpCommit = GlobalCommit
		f, err := os.Create(fileName + ".json.zst")
		if err != nil {
			console.Errorln("Unable to write benchmark data:", err)
		} else {
			func() {
				defer f.Close()
				enc, err := zstd.NewWriter(f, zstd.WithEncoderLevel(zstd.SpeedBetterCompression))
				if err != nil {
					console.Errorln("Unable to compress benchmark data:", err)
				}

				defer enc.Close()
				js := json.NewEncoder(enc)
				js.SetIndent("", "  ")
				err = js.Encode(final)
				if err != nil {
					console.Errorln("Unable to write benchmark data:", err)
				}
				console.Infoln(fmt.Sprintf("\nBenchmark data written to %q\n\n", fileName+".json.zst"))
			}()
		}
	}

	// Skip cleanup stage coordination if running benchmark-only
	skipCleanup := (onlyStage == stageBenchmark.String())

	if !skipCleanup {
		err := cb.waitForStage(stageCleanup)
		if err != nil {
			return err
		}
		if !ctx.Bool("keep-data") && !ctx.Bool("noclear") {
			console.Infoln("Starting cleanup...")
			b.Cleanup(cb.info[stageCleanup].stageCtx)
		}
		cb.stageDone(stageCleanup, nil, common.Custom)
	}

	return nil
}

func addCollector(ctx *cli.Context, b bench.Benchmark) (bench.OpsCollector, chan<- aggregate.UpdateReq) {
	// Add collectors
	common := b.GetCommon()

	if !ctx.Bool("full") {
		updates := make(chan aggregate.UpdateReq, 1000)
		c := aggregate.LiveCollector(context.Background(), updates, pRandASCII(4))
		c.AddOutput(common.ExtraOut...)
		common.Collector = c
		return bench.EmptyOpsCollector, updates
	}
	if common.DiscardOutput {
		common.Collector = bench.NewNullCollector()
		common.Collector.AddOutput(common.ExtraOut...)
		return bench.EmptyOpsCollector, nil
	}
	var retrieveOps bench.OpsCollector
	common.Collector, retrieveOps = bench.NewOpsCollector()
	common.Collector.AddOutput(common.ExtraOut...)
	return retrieveOps, nil
}

type runningProfiles struct {
	client *madmin.AdminClient
}

func startProfiling(ctx2 context.Context, ctx *cli.Context) (*runningProfiles, error) {
	prof := ctx.String("serverprof")
	if len(prof) == 0 {
		return nil, nil
	}
	var r runningProfiles
	r.client = newAdminClient(ctx)

	// Start profile
	//nolint:staticcheck
	_, cmdErr := r.client.StartProfiling(ctx2, madmin.ProfilerType(prof))
	if cmdErr != nil {
		return nil, cmdErr
	}
	console.Infoln("Server profiling successfully started.")
	return &r, nil
}

func (rp *runningProfiles) stop(ctx2 context.Context, _ *cli.Context, fileName string) {
	if rp == nil || rp.client == nil {
		return
	}

	// Ask for profile data, which will come compressed with zip format
	//nolint:staticcheck
	zippedData, adminErr := rp.client.DownloadProfilingData(ctx2)
	fatalIf(probe.NewError(adminErr), "Unable to download profile data.")
	defer zippedData.Close()

	f, err := os.Create(fileName)
	if err != nil {
		console.Error("Unable to write profile data:", err)
		return
	}
	defer f.Close()

	// Copy zip content to target download file
	_, err = io.Copy(f, zippedData)
	if err != nil {
		console.Error("Unable to download profile data:", err)
		return
	}

	console.Infof("Profile data successfully downloaded as %s\n", fileName)
}

func checkBenchmark(ctx *cli.Context) {
	profilerTypes := []madmin.ProfilerType{
		madmin.ProfilerCPU,
		madmin.ProfilerMEM,
		madmin.ProfilerBlock,
		madmin.ProfilerMutex,
		madmin.ProfilerTrace,
		madmin.ProfilerCPUIO,
		madmin.ProfilerThreads,
	}

	_, err := parseInfluxURL(ctx)
	fatalIf(probe.NewError(err), "invalid influx config")

	profs := strings.Split(ctx.String("serverprof"), ",")
	for _, profilerType := range profs {
		if len(profilerType) == 0 {
			continue
		}
		// Check if the provided profiler type is known and supported
		supportedProfiler := false
		for _, profiler := range profilerTypes {
			if profilerType == string(profiler) {
				supportedProfiler = true
				break
			}
		}
		if !supportedProfiler {
			fatalIf(errDummy(), "Profiler type %s unrecognized. Possible values are: %v.", profilerType, profilerTypes)
		}
	}
	if st := ctx.String("syncstart"); st != "" {
		t := parseLocalTime(st)
		if t.Before(time.Now()) {
			fatalIf(errDummy(), "syncstart is in the past: %v", t)
		}
	}
	if ctx.Bool("autoterm") {
		// TODO: autoterm cannot be used when in client/server mode
		if ctx.Duration("autoterm.dur") <= 0 {
			fatalIf(errDummy(), "autoterm.dur cannot be zero or negative")
		}
		if ctx.Float64("autoterm.pct") <= 0 {
			fatalIf(errDummy(), "autoterm.pct cannot be zero or negative")
		}
	}

	// Validate stage flag
	if stage := ctx.String("stage"); stage != "" {
		validStages := []string{stagePrepare.String(), stageBenchmark.String(), stageCleanup.String()}
		isValid := false
		for _, validStage := range validStages {
			if stage == validStage {
				isValid = true
				break
			}
		}
		if !isValid {
			fatalIf(errDummy(), "Invalid stage '%s'. Valid stages are: %v", stage, validStages)
		}
	}
}

// time format for start time.
const timeLayout = "15:04"

func parseLocalTime(s string) time.Time {
	t, err := time.ParseInLocation(timeLayout, s, time.Local)
	fatalIf(probe.NewError(err), "Unable to parse time: %s", s)
	now := time.Now()
	y, m, d := now.Date()
	t = t.AddDate(y, int(m)-1, d-1)
	if t.Before(time.Now()) {
		t = t.Add(24 * time.Hour)
	}
	return t
}

// pRandASCII return pseudorandom ASCII string with length n.
// Should never be considered for true random data generation.
func pRandASCII(n int) string {
	const asciiLetters = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ1234567890"
	// Use a single seed.
	dst := make([]byte, n)
	var seed [8]byte

	// Get something random
	_, _ = rand.Read(seed[:])
	rnd := binary.LittleEndian.Uint32(seed[0:4])
	rnd2 := binary.LittleEndian.Uint32(seed[4:8])
	for i := range dst {
		dst[i] = asciiLetters[int(rnd>>16)%len(asciiLetters)]
		rnd ^= rnd2
		rnd *= 2654435761
	}
	return string(dst)
}
