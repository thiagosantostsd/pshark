package main

import (
	"bufio"
	"encoding/csv"
	"flag"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"time"

	"github.com/pelletier/go-toml/v2"
)

type Config struct {
	Tshark struct {
		Path       string   `toml:"path"`
		Parameters []string `toml:"parameters"`
	} `toml:"tshark"`

	Categories map[string][]struct {
		Key   string `toml:"Key"`
		Value string `toml:"Value"`
	} `toml:"categories"`
}

type Job struct {
	File string
}

type App struct {
	cfg       Config
	category  string
	timestamp bool
	jobs      chan Job
	wg        sync.WaitGroup
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

func main() {
	dir := flag.String("d", "", "Diretório PCAP")
	category := flag.String("c", "", "Categoria ASTERIX")
	timestamp := flag.Bool("ts", false, "Adicionar timestamp")
	workers := flag.Int("j", runtime.NumCPU(), "Jobs paralelos")
	flag.Parse()

	if *dir == "" || *category == "" {
		fmt.Println("❌ Use -d <dir> -c <cat>")
		os.Exit(1)
	}

	cfg, err := loadConfig("config.toml")
	if err != nil {
		fmt.Println("❌ Erro config:", err)
		os.Exit(1)
	}

	files, _ := filepath.Glob(filepath.Join(*dir, "*"))
	if len(files) == 0 {
		fmt.Println("⚠️ Nenhum PCAP encontrado")
		return
	}

	app := App{
		cfg:       cfg,
		category:  *category,
		timestamp: *timestamp,
		jobs:      make(chan Job),
	}

	start := time.Now()

	// Workers
	for i := 0; i < *workers; i++ {
		go app.worker()
	}

	// Envia jobs
	for _, f := range files {
		app.wg.Add(1)
		app.jobs <- Job{File: f}
	}

	close(app.jobs)
	app.wg.Wait()

	fmt.Printf("\n✔ Processamento finalizado em %.2fs\n",
		time.Since(start).Seconds(),
	)
}

func (a *App) worker() {
	for job := range a.jobs {
		a.processFile(job.File)
		a.wg.Done()
	}
}

func (a *App) processFile(filename string) {
	start := time.Now()

	fields := a.cfg.Categories[a.category]
	if len(fields) == 0 {
		fmt.Printf("❌ CAT %s não encontrada\n", a.category)
		return
	}

	outfile := strings.TrimSuffix(filepath.Base(filename), filepath.Ext(filename)) + ".csv"

	var headers []string
	var args []string

	args = append(args, "-r", filename)
	args = append(args, a.cfg.Tshark.Parameters...)
	args = append(args, "-Y", "asterix.category=="+a.category)

	if a.timestamp {
		headers = append(headers, "TIMESTAMP")
		args = append(args, "-e", "frame.time_epoch")
	}

	for _, f := range fields {
		headers = append(headers, f.Key)
		args = append(args, "-e", f.Value)
	}

	cmd := exec.Command(a.cfg.Tshark.Path, args...)
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		fmt.Println("❌ stdout:", err)
		return
	}

	if err := cmd.Start(); err != nil {
		fmt.Println("❌ TShark:", err)
		return
	}

	file, err := os.Create(outfile)
	if err != nil {
		fmt.Println("❌ CSV:", err)
		return
	}
	defer file.Close()

	writer := csv.NewWriter(file)
	writer.Comma = ';'
	writer.Write(headers)

	scanner := bufio.NewScanner(stdout)
	buffer := make([][]string, 0, 10000)

	for scanner.Scan() {
		buffer = append(buffer, strings.Split(scanner.Text(), ";"))

		if len(buffer) == cap(buffer) {
			writer.WriteAll(buffer)
			buffer = buffer[:0]
		}
	}

	if len(buffer) > 0 {
		writer.WriteAll(buffer)
	}

	writer.Flush()
	cmd.Wait()

	fmt.Printf("✔ %s → %s (%.2fs)\n",
		filepath.Base(filename),
		outfile,
		time.Since(start).Seconds(),
	)
}
