package main

import (
	"bufio"
	"context"
	"encoding/csv"
	"flag"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/pelletier/go-toml/v2"

	"github.com/apache/arrow/go/v14/arrow"
	"github.com/apache/arrow/go/v14/arrow/array"
	"github.com/apache/arrow/go/v14/arrow/memory"
	"github.com/apache/arrow/go/v14/parquet"
	"github.com/apache/arrow/go/v14/parquet/compress"
	"github.com/apache/arrow/go/v14/parquet/file"
	"github.com/apache/arrow/go/v14/parquet/pqarrow"
)

//
// ---------------- CONFIG ----------------
//

type DataItem struct {
	Label string `toml:"label"`
	Field string `toml:"field"`
	Type  string `toml:"type"` // string, int32, int64, float32, float64, uint8, uint16
}

type TShark struct {
	Path       string   `toml:"path"`
	Parameters []string `toml:"parameters"`
}

type Config struct {
	Tshark    TShark                `toml:"tshark"`
	Datagroup map[string][]DataItem `toml:"datagroup"`
	Frame     []DataItem            `toml:"frame"` // campos a colocar no início
}

func loadConfig(path string) (Config, error) {
	var cfg Config
	data, err := os.ReadFile(path)
	if err != nil {
		return cfg, err
	}
	err = toml.Unmarshal(data, &cfg)
	return cfg, err
}

//
// ---------------- ARROW / PARQUET ----------------
//

type ParquetWriter struct {
	schema   *arrow.Schema
	writer   *pqarrow.FileWriter
	builders []array.Builder
	mem      memory.Allocator
	rows     int
	batch    int
}

func (f DataItem) ArrowType() arrow.DataType {
	switch f.Type {
	case "int32":
		return arrow.PrimitiveTypes.Int32
	case "int64":
		return arrow.PrimitiveTypes.Int64
	case "float32":
		return arrow.PrimitiveTypes.Float32
	case "float64":
		return arrow.PrimitiveTypes.Float64
	case "uint8":
		return arrow.PrimitiveTypes.Uint8
	case "uint16":
		return arrow.PrimitiveTypes.Uint16
	default:
		return arrow.BinaryTypes.String
	}
}

func newParquetWriter(path string, fields []DataItem) (*ParquetWriter, error) {
	mem := memory.NewGoAllocator()

	arrowFields := make([]arrow.Field, len(fields))
	builders := make([]array.Builder, len(fields))

	for i, f := range fields {
		arrowFields[i] = arrow.Field{
			Name:     f.Label,
			Type:     f.ArrowType(),
			Nullable: true,
		}
		builders[i] = array.NewBuilder(mem, f.ArrowType())
	}

	schema := arrow.NewSchema(arrowFields, nil)

	file, err := os.Create(path)
	if err != nil {
		return nil, err
	}

	props := parquet.NewWriterProperties(
		parquet.WithCompression(compress.Codecs.Snappy),
	)

	writer, err := pqarrow.NewFileWriter(
		schema,
		file,
		props,
		pqarrow.DefaultWriterProps(),
	)
	if err != nil {
		return nil, err
	}

	return &ParquetWriter{
		schema:   schema,
		writer:   writer,
		builders: builders,
		mem:      mem,
		batch:    1024,
	}, nil
}

func appendValue(b array.Builder, v string) {
	if v == "" {
		b.AppendNull()
		return
	}

	switch bb := b.(type) {
	case *array.Int32Builder:
		var x int32
		fmt.Sscan(v, &x)
		bb.Append(x)
	case *array.Int64Builder:
		var x int64
		fmt.Sscan(v, &x)
		bb.Append(x)
	case *array.Float32Builder:
		var x float32
		fmt.Sscan(v, &x)
		bb.Append(x)
	case *array.Float64Builder:
		var x float64
		fmt.Sscan(v, &x)
		bb.Append(x)
	case *array.StringBuilder:
		bb.Append(v)
	case *array.Uint8Builder:
		var x uint8
		fmt.Sscan(v, &x)
		bb.Append(x)
	case *array.Uint16Builder:
		var x uint16
		fmt.Sscan(v, &x)
		bb.Append(x)
	default:
		b.AppendNull()
	}
}

func (p *ParquetWriter) WriteRow(values []string) error {
	for i, v := range values {
		appendValue(p.builders[i], v)
	}
	p.rows++
	if p.rows >= p.batch {
		return p.flush()
	}
	return nil
}

func (p *ParquetWriter) flush() error {
	arrays := make([]arrow.Array, len(p.builders))
	for i, b := range p.builders {
		arrays[i] = b.NewArray()
	}

	record := array.NewRecord(p.schema, arrays, int64(p.rows))
	if err := p.writer.Write(record); err != nil {
		return err
	}

	record.Release()

	// recria os builders
	for i, f := range p.schema.Fields() {
		p.builders[i].Release()
		p.builders[i] = array.NewBuilder(p.mem, f.Type)
	}

	p.rows = 0
	return nil
}

func (p *ParquetWriter) Close() error {
	if p.rows > 0 {
		if err := p.flush(); err != nil {
			return err
		}
	}
	return p.writer.Close()
}

//
// ---------------- APP ----------------
//

type Job struct {
	File string
}

type App struct {
	cfg       Config
	datalist  string
	timestamp bool
	genCSV    bool
	jobs      chan Job
	wg        sync.WaitGroup
}

func (a *App) worker() {
	for job := range a.jobs {
		a.processFile(job.File)
		a.wg.Done()
	}
}

func (a *App) processFile(filename string) {
	start := time.Now()

	// 🔹 campos de frame primeiro, depois categoria
	fields := append(a.cfg.Frame, a.cfg.Datagroup[a.datalist]...)
	if len(fields) == 0 {
		fmt.Printf("❌ CAT %s não encontrada\n", a.datalist)
		return
	}

	outfile := strings.TrimSuffix(filepath.Base(filename), filepath.Ext(filename)) + ".parquet"

	args := []string{"-r", filename}
	args = append(args, a.cfg.Tshark.Parameters...)
	//args = append(args, "-Y", "asterix.category=="+a.category)

	// adiciona campos de frame primeiro
	for _, f := range a.cfg.Frame {
		args = append(args, "-e", f.Field)
	}

	// depois os campos da categoria
	for _, f := range a.cfg.Datagroup[a.datalist] {
		args = append(args, "-e", f.Field)
	}

	cmd := exec.Command(a.cfg.Tshark.Path, args...)
	stdout, _ := cmd.StdoutPipe()
	stderr, _ := cmd.StderrPipe()

	go func() {
		scanner := bufio.NewScanner(stderr)
		for scanner.Scan() {
			fmt.Println("[tshark]", scanner.Text())
		}
	}()

	if err := cmd.Start(); err != nil {
		fmt.Println("❌ falha ao iniciar tshark:", err)
		return
	}

	pw, err := newParquetWriter(outfile, fields)
	if err != nil {
		fmt.Println("❌ parquet:", err)
		return
	}

	scanner := bufio.NewScanner(stdout)
	scanner.Buffer(make([]byte, 64*1024), 10*1024*1024)

	lineCount := 0

	for scanner.Scan() {
		raw := strings.Split(scanner.Text(), ";")

		// separa cada campo que pode ter múltiplos valores
		split := make([][]string, len(raw))
		max := 1
		for i, f := range raw {
			parts := strings.Split(f, ",")
			for j := range parts {
				parts[j] = strings.TrimSpace(parts[j])
			}
			split[i] = parts
			if len(parts) > max {
				max = len(parts)
			}
		}

		// 🔹 salva os valores de frame (timestamp, etc)
		frameValues := make([]string, len(a.cfg.Frame))
		for i := range a.cfg.Frame {
			if len(split) > i && split[i][0] != "" {
				frameValues[i] = split[i][0]
			}
		}

		// para cada “linha” da mensagem ASTERIX
		for i := 0; i < max; i++ {
			row := make([]string, len(fields))
			// frame primeiro
			copy(row[:len(frameValues)], frameValues)

			// categoria ASTERIX
			offset := len(frameValues)
			for j := 0; j < len(a.cfg.Datagroup[a.datalist]); j++ {
				idx := offset + j
				if idx < len(split) && i < len(split[idx]) {
					row[offset+j] = split[idx][i]
				} else {
					row[offset+j] = ""
				}
			}

			_ = pw.WriteRow(row)

		}

		lineCount++
		if lineCount%5000 == 0 {
			fmt.Printf("\r%s: %d pacotes processados.   ", filepath.Base(filename), lineCount)
		}
	}
	fmt.Println()

	if err := cmd.Wait(); err != nil {
		fmt.Println("❌ erro tshark:", err)
	}

	if err := pw.Close(); err != nil {
		fmt.Println("❌ parquet close:", err)
		return
	}

	if a.genCSV {
		if err := parquetToCSV(outfile, a.cfg); err != nil {
			fmt.Println("❌ CSV:", err)
		}
	}

	fmt.Printf("✔ %s → %s (%.2fs)\n", filepath.Base(filename), outfile, time.Since(start).Seconds())
}

func getCSVFieldOrder(cfg Config, schema *arrow.Schema) []int {
	order := []int{}
	frameFields := map[string]bool{}
	for _, f := range cfg.Frame {
		frameFields[f.Label] = true
	}

	// Primeiro índices dos campos frame
	for i, f := range schema.Fields() {
		if frameFields[f.Name] {
			order = append(order, i)
		}
	}

	// Depois os outros campos
	for i, f := range schema.Fields() {
		if !frameFields[f.Name] {
			order = append(order, i)
		}
	}
	return order
}

func parquetToCSV(parquetFile string, cfg Config) error {
	csvFile := strings.TrimSuffix(parquetFile, ".parquet") + ".csv"

	f, err := os.Open(parquetFile)
	if err != nil {
		return err
	}
	defer f.Close()

	pqReader, err := file.NewParquetReader(f)
	if err != nil {
		return err
	}
	defer pqReader.Close()

	mem := memory.NewGoAllocator()
	arrowReader, err := pqarrow.NewFileReader(pqReader, pqarrow.ArrowReadProperties{}, mem)
	if err != nil {
		return err
	}

	table, err := arrowReader.ReadTable(context.Background())
	if err != nil {
		return err
	}
	defer table.Release()

	out, err := os.Create(csvFile)
	if err != nil {
		return err
	}
	defer out.Close()

	w := csv.NewWriter(out)
	w.Comma = ';'

	schema := table.Schema()
	order := getCSVFieldOrder(cfg, schema)

	// header
	headers := make([]string, len(schema.Fields()))
	for i, idx := range order {
		headers[i] = schema.Field(idx).Name
	}
	if err := w.Write(headers); err != nil {
		return err
	}

	// records
	recReader := array.NewTableReader(table, 1024)
	defer recReader.Release()

	for recReader.Next() {
		rec := recReader.Record()
		rows := int(rec.NumRows())
		cols := int(rec.NumCols())

		for r := 0; r < rows; r++ {
			line := make([]string, cols)
			for i, idx := range order {
				line[i] = valueAt(rec.Column(idx), r)
			}
			if err := w.Write(line); err != nil {
				return err
			}
		}
		rec.Release()
	}

	w.Flush()
	return w.Error()
}

func valueAt(arr arrow.Array, row int) string {
	if arr.IsNull(row) {
		return ""
	}

	switch a := arr.(type) {
	case *array.Int32:
		return strconv.FormatInt(int64(a.Value(row)), 10)
	case *array.Int64:
		return strconv.FormatInt(a.Value(row), 10)
	case *array.Float32:
		return strconv.FormatFloat(float64(a.Value(row)), 'f', -1, 32)
	case *array.Float64:
		return strconv.FormatFloat(a.Value(row), 'f', -1, 64)
	case *array.String:
		return a.Value(row)
	case *array.Uint8:
		return strconv.FormatUint(uint64(a.Value(row)), 10)
	case *array.Uint16:
		return strconv.FormatUint(uint64(a.Value(row)), 10)
	default:
		return ""
	}
}

//
// ---------------- MAIN ----------------
//

func main() {
	file := flag.String("f", "", "PCAP file")
	dir := flag.String("d", "", "PCAP directory")
	datagroup := flag.String("g", "", "PCAP datagroup")
	cfgPath := flag.String("cfg", "config.toml", "Config file")
	workers := flag.Int("j", runtime.NumCPU(), "Workers")
	csvFile := flag.Bool("csv", false, "Gerar CSV a partir do Parquet")

	flag.Parse()

	if *datagroup == "" {
		fmt.Println("❌ Use -g <datagroup>")
		return
	}

	cfg, err := loadConfig(*cfgPath)
	if err != nil {
		fmt.Println("❌ config:", err)
		return
	}

	files := []string{}
	if *file != "" {
		files = append(files, *file)
	} else if *dir != "" {
		matches, _ := filepath.Glob(filepath.Join(*dir, "*.pcap"))
		files = append(files, matches...)
	}

	if len(files) == 0 {
		fmt.Println("❌ Nenhum arquivo PCAP encontrado")
		return
	}

	app := App{
		cfg:      cfg,
		datalist: *datagroup,
		jobs:     make(chan Job),
		genCSV:   *csvFile,
	}

	for i := 0; i < *workers; i++ {
		go app.worker()
	}

	for _, f := range files {
		app.wg.Add(1)
		app.jobs <- Job{File: f}
	}

	close(app.jobs)
	app.wg.Wait()
}
