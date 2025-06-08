package main

import (
	"AT/timeseries"
	"encoding/csv"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"log"
	"os"
	"strconv"
	"time"
)

func main() {
	// Конфигурация флагов командной строки
	inputFile := flag.String("input", "", "Path to input CSV file with timestamps")
	outputFile := flag.String("output", "", "Path to output JSON file (default: stdout)")
	minPeriod := flag.Float64("min-period", 0.1, "Minimum period in hours")
	maxPeriod := flag.Float64("max-period", 8760, "Maximum period in hours")
	numPeriods := flag.Int("num-periods", 5, "Number of periods to return")
	samplesPerPeak := flag.Int("samples-per-peak", 5, "Samples per peak for periodogram")
	flag.Parse()

	// Валидация параметров
	if *inputFile == "" {
		log.Fatal("Input file is required. Use -input flag to specify CSV file")
	}
	if *minPeriod <= 0 || *maxPeriod <= 0 {
		log.Fatal("Periods must be positive values")
	}
	if *minPeriod >= *maxPeriod {
		log.Fatal("min-period must be less than max-period")
	}
	if *numPeriods <= 0 {
		log.Fatal("num-periods must be at least 1")
	}

	// Загрузка временных меток из CSV
	timestamps, err := loadTimestampsFromCSV(*inputFile)
	if err != nil {
		log.Fatalf("Failed to load timestamps: %v", err)
	}
	log.Printf("Loaded %d timestamps from %s", len(timestamps), *inputFile)

	// Конфигурация анализа
	config := timeseries.PeriodConfig{
		MinPeriod:      *minPeriod,
		MaxPeriod:      *maxPeriod,
		NumPeriods:     *numPeriods,
		SamplesPerPeak: *samplesPerPeak,
	}

	// Выполнение анализа
	startTime := time.Now()
	result, err := timeseries.AnalyzeTimestamps(timestamps, config)
	if err != nil {
		log.Fatalf("Analysis failed: %v", err)
	}
	duration := time.Since(startTime)
	log.Printf("Analysis completed in %s", duration)

	// Форматирование и вывод результатов
	output, err := json.MarshalIndent(result, "", "  ")
	if err != nil {
		log.Fatalf("Failed to marshal results: %v", err)
	}

	// Вывод в файл или stdout
	if *outputFile != "" {
		if err := os.WriteFile(*outputFile, output, 0644); err != nil {
			log.Fatalf("Failed to write output file: %v", err)
		}
		log.Printf("Results saved to %s", *outputFile)
	} else {
		fmt.Println(string(output))
	}
}

// loadTimestampsFromCSV загружает временные метки из CSV файла
func loadTimestampsFromCSV(filename string) ([]int64, error) {
	file, err := os.Open(filename)
	if err != nil {
		return nil, err
	}
	defer file.Close()

	reader := csv.NewReader(file)
	var timestamps []int64

	for {
		record, err := reader.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			return nil, err
		}

		for _, value := range record {
			if value == "" {
				continue
			}

			ts, err := strconv.ParseInt(value, 10, 64)
			if err != nil {
				return nil, fmt.Errorf("invalid timestamp %s: %v", value, err)
			}

			timestamps = append(timestamps, ts)
		}
	}

	return timestamps, nil
}
