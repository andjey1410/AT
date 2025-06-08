package timeseries

import (
	"errors"
	"fmt"
	"math"
	"sort"
	"time"
)

// PeriodConfig содержит параметры для спектрального анализа
type PeriodConfig struct {
	MinPeriod      float64 // Минимальный период в часах (по умолчанию 0.1)
	MaxPeriod      float64 // Максимальный период в часах (по умолчанию 8760)
	NumPeriods     int     // Количество возвращаемых периодов (по умолчанию 5)
	SamplesPerPeak int     // Количество сэмплов на пик (по умолчанию 5)
}

// PeriodResult представляет результат обнаружения периода
type PeriodResult struct {
	Period       float64 `json:"period"`       // Период в часах
	Power        float64 `json:"power"`        // Мощность сигнала
	Significance float64 `json:"significance"` // Значимость в процентах
}

// PeriodResults содержит результаты спектрального анализа
type PeriodResults struct {
	Daily     []PeriodResult            `json:"daily"`
	Weekly    []PeriodResult            `json:"weekly"`
	AllTime   []PeriodResult            `json:"allTime"`
	Quarterly map[string][]PeriodResult `json:"quarterly"` // Ключ: "2023-Q1"
}

// DayRecord представляет агрегированные данные за день
type DayRecord struct {
	Date  time.Time `json:"date"`
	Count int       `json:"count"`
}

// WeekRecord представляет агрегированные данные за неделю
type WeekRecord struct {
	Week  time.Time `json:"week"` // Начало недели (понедельник)
	Count int       `json:"count"`
}

// MonthRecord представляет агрегированные данные за месяц
type MonthRecord struct {
	Month time.Time `json:"month"` // Первый день месяца
	Count int       `json:"count"`
}

// ContinuousResult содержит результаты анализа непрерывных периодов
type ContinuousResult struct {
	AllData           PeriodResults `json:"allData"`
	LongestContinuous PeriodResults `json:"longestContinuous"`
	Start             time.Time     `json:"start"`
	End               time.Time     `json:"end"`
	RecordCount       int           `json:"recordCount"`
}

// AnalysisResult содержит полные результаты анализа
type AnalysisResult struct {
	TotalRecords int              `json:"totalRecords"`
	StartDate    time.Time        `json:"startDate"`
	EndDate      time.Time        `json:"endDate"`
	Days         []DayRecord      `json:"days"`
	Weeks        []WeekRecord     `json:"weeks"`
	Months       []MonthRecord    `json:"months"`
	Periods      PeriodResults    `json:"periods"`
	Continuous   ContinuousResult `json:"continuous"`
}

// DefaultPeriodConfig возвращает конфигурацию по умолчанию
func DefaultPeriodConfig() PeriodConfig {
	return PeriodConfig{
		MinPeriod:      0.1,  // 6 минут
		MaxPeriod:      8760, // 1 год
		NumPeriods:     5,
		SamplesPerPeak: 5,
	}
}

// AnalyzeTimestamps - основная точка входа для анализа
func AnalyzeTimestamps(timestamps []int64, config PeriodConfig) (*AnalysisResult, error) {
	if len(timestamps) == 0 {
		return nil, errors.New("no timestamps provided")
	}

	// Валидация конфигурации
	if config.MinPeriod <= 0 {
		return nil, errors.New("minPeriod must be positive")
	}
	if config.MaxPeriod <= 0 {
		return nil, errors.New("maxPeriod must be positive")
	}
	if config.MinPeriod >= config.MaxPeriod {
		return nil, errors.New("minPeriod must be less than maxPeriod")
	}
	if config.NumPeriods <= 0 {
		return nil, errors.New("numPeriods must be at least 1")
	}

	// Конвертация временных меток в time.Time
	times := make([]time.Time, len(timestamps))
	for i, ts := range timestamps {
		times[i] = time.Unix(ts/1000, (ts%1000)*int64(time.Millisecond))
	}

	// Определение временного диапазона
	startDate, endDate := findDateRange(times)

	// Агрегация данных
	days := aggregateByDay(times)
	weeks := aggregateByWeek(times)
	months := aggregateByMonth(times)

	// Инициализация детектора периодов
	detector := newPeriodDetector(config)

	// Спектральный анализ
	periods := PeriodResults{
		Daily:     detector.detect(filterByTimeRange(times, endDate, 72*time.Hour)),
		Weekly:    detector.detect(filterByTimeRange(times, endDate, 336*time.Hour)),
		AllTime:   detector.detect(times),
		Quarterly: detectQuarterlyPeriods(times, detector),
	}

	// Анализ непрерывных периодов
	continuous := analyzeContinuousPeriods(times, detector)

	// Формирование результата
	result := &AnalysisResult{
		TotalRecords: len(times),
		StartDate:    startDate,
		EndDate:      endDate,
		Days:         days,
		Weeks:        weeks,
		Months:       months,
		Periods:      periods,
		Continuous:   continuous,
	}

	return result, nil
}

// periodDetector реализует алгоритм Ломба-Скаргла
type periodDetector struct {
	config PeriodConfig
}

func newPeriodDetector(config PeriodConfig) *periodDetector {
	return &periodDetector{config: config}
}

// detect выполняет обнаружение периодов для набора временных меток
func (pd *periodDetector) detect(times []time.Time) []PeriodResult {
	if len(times) < 4 {
		return nil
	}

	// Конвертация в часы относительно минимального времени
	timesHours := convertToHours(times)

	// Вычисление периодограммы
	freqs, powers := pd.computePeriodogram(timesHours)

	// Поиск значимых пиков
	return pd.findSignificantPeaks(freqs, powers)
}

// computePeriodogram вычисляет периодограмму Ломба-Скаргла
func (pd *periodDetector) computePeriodogram(times []float64) ([]float64, []float64) {
	minFreq := 1 / pd.config.MaxPeriod
	maxFreq := 1 / pd.config.MinPeriod

	// Рассчитываем количество частот
	T := times[len(times)-1] - times[0]
	if T <= 0 {
		return nil, nil
	}

	nFreqs := int(float64(pd.config.SamplesPerPeak) * T * (maxFreq - minFreq))
	if nFreqs < 100 {
		nFreqs = 100
	} else if nFreqs > 10000 {
		nFreqs = 10000
	}

	freqs := make([]float64, nFreqs)
	powers := make([]float64, nFreqs)

	// Шаг по частоте
	df := (maxFreq - minFreq) / float64(nFreqs-1)

	// Вычисляем мощность для каждой частоты
	for i := 0; i < nFreqs; i++ {
		f := minFreq + float64(i)*df
		freqs[i] = f
		powers[i] = pd.computePower(times, f)
	}

	return freqs, powers
}

// computePower вычисляет мощность для заданной частоты
func (pd *periodDetector) computePower(times []float64, freq float64) float64 {
	omega := 2 * math.Pi * freq
	N := float64(len(times))

	var sumCos, sumSin float64
	for _, t := range times {
		sumCos += math.Cos(omega * t)
		sumSin += math.Sin(omega * t)
	}

	return (sumCos*sumCos + sumSin*sumSin) / N
}

func (pd *periodDetector) findSignificantPeaks(freqs, powers []float64) []PeriodResult {
	// Находим все локальные максимумы
	peaks := findLocalPeaks(powers)
	if len(peaks) == 0 {
		return nil
	}

	// Сортируем пики по мощности (по убыванию)
	sortPeaksByPower(peaks, powers)

	// Ограничиваем количество возвращаемых периодов
	if len(peaks) > pd.config.NumPeriods {
		peaks = peaks[:pd.config.NumPeriods]
	}

	// Вычисляем общую мощность для нормализации
	totalPower := 0.0
	for _, p := range powers {
		totalPower += p
	}
	if totalPower < 1e-10 {
		totalPower = 1e-10
	}

	// Формируем результаты
	results := make([]PeriodResult, len(peaks))
	for i, idx := range peaks {
		period := 1 / freqs[idx]
		power := powers[idx]
		significance := power / totalPower * 100

		results[i] = PeriodResult{
			Period:       period,
			Power:        power,
			Significance: significance,
		}
	}

	return results
}

// findLocalPeaks находит локальные максимумы
func findLocalPeaks(data []float64) []int {
	var peaks []int
	for i := 1; i < len(data)-1; i++ {
		if data[i] > data[i-1] && data[i] > data[i+1] {
			peaks = append(peaks, i)
		}
	}
	return peaks
}

// sortPeaksByPower сортирует пики по мощности (по убыванию)
func sortPeaksByPower(peaks []int, powers []float64) {
	sort.Slice(peaks, func(i, j int) bool {
		return powers[peaks[i]] > powers[peaks[j]]
	})
}

// findMaxPower находит максимальное значение мощности
func findMaxPower(powers []float64) float64 {
	max := 0.0
	for _, p := range powers {
		if p > max {
			max = p
		}
	}
	return max
}

// findDateRange определяет временной диапазон
func findDateRange(times []time.Time) (start, end time.Time) {
	if len(times) == 0 {
		return
	}

	start = times[0]
	end = times[0]

	for _, t := range times {
		if t.Before(start) {
			start = t
		}
		if t.After(end) {
			end = t
		}
	}

	return start, end
}

// convertToHours конвертирует временные метки в часы относительно минимального времени
func convertToHours(times []time.Time) []float64 {
	if len(times) == 0 {
		return nil
	}

	// Находим минимальное время
	minTime := times[0]
	for _, t := range times {
		if t.Before(minTime) {
			minTime = t
		}
	}

	// Конвертируем в часы
	result := make([]float64, len(times))
	for i, t := range times {
		duration := t.Sub(minTime)
		result[i] = duration.Hours()
	}

	return result
}

// filterByTimeRange фильтрует временные метки по диапазону
func filterByTimeRange(times []time.Time, end time.Time, duration time.Duration) []time.Time {
	startTime := end.Add(-duration)
	var result []time.Time

	for _, t := range times {
		if t.After(startTime) && t.Before(end.Add(24*time.Hour)) {
			result = append(result, t)
		}
	}

	return result
}

// detectQuarterlyPeriods выполняет анализ по кварталам
func detectQuarterlyPeriods(times []time.Time, detector *periodDetector) map[string][]PeriodResult {
	quarters := groupByQuarter(times)
	results := make(map[string][]PeriodResult)

	for quarter, times := range quarters {
		results[quarter] = detector.detect(times)
	}

	return results
}

// groupByQuarter группирует временные метки по кварталам
func groupByQuarter(times []time.Time) map[string][]time.Time {
	quarters := make(map[string][]time.Time)

	for _, t := range times {
		quarter := getQuarter(t)
		quarters[quarter] = append(quarters[quarter], t)
	}

	return quarters
}

// getQuarter возвращает квартал в формате "2023-Q1"
func getQuarter(t time.Time) string {
	year := t.Year()
	month := t.Month()

	var quarter int
	switch {
	case month >= time.January && month <= time.March:
		quarter = 1
	case month >= time.April && month <= time.June:
		quarter = 2
	case month >= time.July && month <= time.September:
		quarter = 3
	case month >= time.October && month <= time.December:
		quarter = 4
	}

	return fmt.Sprintf("%d-Q%d", year, quarter)
}

// analyzeContinuousPeriods анализирует непрерывные периоды
func analyzeContinuousPeriods(times []time.Time, detector *periodDetector) ContinuousResult {
	result := ContinuousResult{}
	if len(times) == 0 {
		return result
	}

	// Анализ всех данных
	result.AllData.Daily = detector.detect(times)
	result.AllData.Weekly = detector.detect(times)
	result.AllData.AllTime = detector.detect(times)
	result.RecordCount = len(times)

	// Поиск самого длинного непрерывного периода
	start, end, continuous := findLongestContinuousPeriod(times)
	if len(continuous) > 0 {
		result.Start = start
		result.End = end
		result.LongestContinuous.Daily = detector.detect(continuous)
		result.LongestContinuous.Weekly = detector.detect(continuous)
		result.LongestContinuous.AllTime = detector.detect(continuous)
	}

	return result
}

// findLongestContinuousPeriod находит самый длинный непрерывный период
func findLongestContinuousPeriod(times []time.Time) (start, end time.Time, continuous []time.Time) {
	if len(times) < 2 {
		return time.Time{}, time.Time{}, times
	}

	// Сортируем временные метки
	sort.Slice(times, func(i, j int) bool {
		return times[i].Before(times[j])
	})

	// Собираем уникальные дни
	daySet := make(map[time.Time]struct{})
	for _, t := range times {
		day := t.Truncate(24 * time.Hour)
		daySet[day] = struct{}{}
	}

	// Преобразуем в отсортированный список дней
	days := make([]time.Time, 0, len(daySet))
	for day := range daySet {
		days = append(days, day)
	}
	sort.Slice(days, func(i, j int) bool {
		return days[i].Before(days[j])
	})

	// Ищем самый длинный непрерывный период
	var currentStart, currentEnd, maxStart, maxEnd time.Time
	maxLength := 0
	currentLength := 0

	for i := 0; i < len(days); i++ {
		if i == 0 {
			currentStart = days[i]
			currentEnd = days[i]
			currentLength = 1
			continue
		}

		gap := days[i].Sub(days[i-1]).Hours() / 24
		if gap <= 2 { // Разрыв не более 2 дней
			currentEnd = days[i]
			currentLength++
		} else {
			if currentLength > maxLength {
				maxStart = currentStart
				maxEnd = currentEnd
				maxLength = currentLength
			}
			currentStart = days[i]
			currentEnd = days[i]
			currentLength = 1
		}
	}

	// Проверяем последний период
	if currentLength > maxLength {
		maxStart = currentStart
		maxEnd = currentEnd
		maxLength = currentLength
	}

	// Если не нашли подходящий период, возвращаем весь диапазон
	if maxLength == 0 {
		return times[0], times[len(times)-1], times
	}

	// Собираем записи, входящие в непрерывный период
	startLimit := maxStart
	endLimit := maxEnd.Add(24 * time.Hour) // Включаем весь последний день
	for _, t := range times {
		if !t.Before(startLimit) && t.Before(endLimit) {
			continuous = append(continuous, t)
		}
	}

	return maxStart, maxEnd, continuous
}

// aggregateByDay агрегирует данные по дням
func aggregateByDay(times []time.Time) []DayRecord {
	dateMap := make(map[time.Time]int)
	for _, t := range times {
		date := t.Truncate(24 * time.Hour)
		dateMap[date]++
	}

	if len(dateMap) == 0 {
		return nil
	}

	// Определяем временной диапазон
	var minDate, maxDate time.Time
	for date := range dateMap {
		if minDate.IsZero() || date.Before(minDate) {
			minDate = date
		}
		if maxDate.IsZero() || date.After(maxDate) {
			maxDate = date
		}
	}

	// Генерируем полный ряд
	var result []DayRecord
	current := minDate
	for !current.After(maxDate) {
		count := dateMap[current]
		result = append(result, DayRecord{
			Date:  current,
			Count: count,
		})
		current = current.AddDate(0, 0, 1)
	}

	return result
}

// aggregateByWeek агрегирует данные по неделям
func aggregateByWeek(times []time.Time) []WeekRecord {
	weekMap := make(map[string]int)
	weekStarts := make(map[string]time.Time)

	for _, t := range times {
		year, week := t.ISOWeek()
		key := fmt.Sprintf("%d-W%02d", year, week)

		// Вычисляем начало недели (понедельник)
		weekday := int(t.Weekday())
		if weekday == 0 {
			weekday = 7 // Воскресенье -> 7
		}
		daysToMonday := weekday - 1
		weekStart := t.AddDate(0, 0, -daysToMonday).Truncate(24 * time.Hour)

		if _, exists := weekStarts[key]; !exists {
			weekStarts[key] = weekStart
		}
		weekMap[key]++
	}

	// Собираем уникальные начала недель
	var weeks []time.Time
	for _, start := range weekStarts {
		weeks = append(weeks, start)
	}

	// Сортируем недели
	sort.Slice(weeks, func(i, j int) bool {
		return weeks[i].Before(weeks[j])
	})

	// Формируем результат
	var result []WeekRecord
	for _, weekStart := range weeks {
		year, week := weekStart.ISOWeek()
		key := fmt.Sprintf("%d-W%02d", year, week)
		result = append(result, WeekRecord{
			Week:  weekStart,
			Count: weekMap[key],
		})
	}

	return result
}

// aggregateByMonth агрегирует данные по месяцам
func aggregateByMonth(times []time.Time) []MonthRecord {
	monthMap := make(map[time.Time]int)
	for _, t := range times {
		month := time.Date(t.Year(), t.Month(), 1, 0, 0, 0, 0, t.Location())
		monthMap[month]++
	}

	if len(monthMap) == 0 {
		return nil
	}

	// Определяем временной диапазон
	var minMonth, maxMonth time.Time
	for month := range monthMap {
		if minMonth.IsZero() || month.Before(minMonth) {
			minMonth = month
		}
		if maxMonth.IsZero() || month.After(maxMonth) {
			maxMonth = month
		}
	}

	// Генерируем полный ряд
	var result []MonthRecord
	current := minMonth
	for !current.After(maxMonth) {
		count := monthMap[current]
		result = append(result, MonthRecord{
			Month: current,
			Count: count,
		})
		current = current.AddDate(0, 1, 0)
	}

	return result
}

// firstDayOfISOWeek возвращает первый день недели по ISO стандарту
func firstDayOfISOWeek(year, week int, loc *time.Location) time.Time {
	// Создаем дату для 4 января, которое всегда находится в 1 неделе
	date := time.Date(year, time.January, 4, 0, 0, 0, 0, loc)

	// Находим понедельник недели, содержащей 4 января
	offset := int(time.Monday - date.Weekday())
	if offset > 0 {
		offset = -6
	}
	date = date.AddDate(0, 0, offset)

	// Переходим к нужной неделе
	return date.AddDate(0, 0, (week-1)*7)
}
