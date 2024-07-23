package metric

import (
	"strings"
	"time"
)

type Metric struct {
	InSrcCnt                      int
	InDstCnt                      int
	InSrcAndDstByKeyCnt           int
	InSrcAndDstByIdButNotByKeyCnt int
	JustInSrcByKeyAndIdCnt        int
	JustInDstByKeyAndIdCnt        int

	ToInsertCnt int
	ToUpdateCnt int
	ToUpsertCnt int
	ToDeleteCnt int

	Strategy        uint8
	IndexTime       time.Duration
	CategorizeTime  time.Duration
	FetchTime       time.Duration
	CompareDataTime time.Duration
	TotalTime       time.Duration

	Insert Summary
	Upsert Summary
	Update Summary
	Delete Summary
	Total  Total
	Err    error

	Table string
}

type Summary struct {
	Transient Operation
	Main      Operation
	Total     Operation
}

type Operation struct {
	Affected int
	Time     time.Duration
}

type Total struct {
	TransientAffected int
	MainAffected      int
	TransientTime     time.Duration
	MainTime          time.Duration
	TotalAffected     int
	TotalTime         time.Duration
	Report            []string
}

func (m *Metric) Summary() {
	m.Insert.Total.Time = m.Insert.Transient.Time + m.Insert.Transient.Time
	m.Insert.Total.Affected = m.Insert.Transient.Affected + m.Insert.Transient.Affected

	operations := []*Summary{&m.Insert, &m.Update, &m.Upsert, &m.Delete}
	for _, o := range operations {
		o.Total.Time = o.Transient.Time + o.Main.Time
		o.Total.Affected = o.Transient.Affected + o.Main.Affected

		m.Total.MainAffected += o.Main.Affected
		m.Total.TransientAffected += o.Transient.Affected
		m.Total.MainTime += o.Main.Time
		m.Total.TransientTime += o.Transient.Time
	}

	m.Total.TotalAffected = m.Total.TransientAffected + m.Total.MainAffected
	m.Total.TotalTime = m.Total.TransientTime + m.Total.MainTime
}

// RowsAffected returns the number of rows affected by an insert, upsert, update or delete.
func (m *Metric) RowsAffected() int {
	if m.Err != nil {
		return 0
	}
	return m.ToInsertCnt + m.ToUpdateCnt + m.ToUpsertCnt + m.ToDeleteCnt
}

// InsRowsAffected returns the number of rows affected by an insert.
func (m *Metric) InsRowsAffected() int {
	if m.Err != nil {
		return 0
	}
	return m.ToInsertCnt
}

// UpsRowsAffected returns the number of rows affected by an upsert.
func (m *Metric) UpsRowsAffected() int {
	if m.Err != nil {
		return 0
	}
	return m.ToUpsertCnt
}

// UpdRowsAffected returns the number of rows affected by an update.
func (m *Metric) UpdRowsAffected() int {
	if m.Err != nil {
		return 0
	}
	return m.ToUpdateCnt
}

// DelRowsAffected returns the number of rows affected by an delete.
func (m *Metric) DelRowsAffected() int {
	if m.Err != nil {
		return 0
	}
	return m.ToDeleteCnt
}

// Report returns []string with merge report.
func (m *Metric) Report() string {
	sb := strings.Builder{}
	for _, s := range m.Total.Report {
		sb.WriteString(s)
	}
	return sb.String()
}

// InsertingTime returns inserting duration.
func (m *Metric) InsertingTime() time.Duration {
	if m.Err != nil {
		return 0
	}
	return m.Insert.Total.Time
}

// UpsertingTime returns upserting duration.
func (m *Metric) UpsertingTime() time.Duration {
	if m.Err != nil {
		return 0
	}
	return m.Upsert.Total.Time
}

// UpdatingTime returns updating duration.
func (m *Metric) UpdatingTime() time.Duration {
	if m.Err != nil {
		return 0
	}
	return m.Update.Total.Time
}

// DeletingTime returns deleting duration.
func (m *Metric) DeletingTime() time.Duration {
	if m.Err != nil {
		return 0
	}
	return m.Delete.Total.Time
}

// MergingTime returns total merging duration.
func (m *Metric) MergingTime() time.Duration {
	if m.Err != nil {
		return 0
	}
	return m.TotalTime
}

func (m *Metric) MergedTable() string {
	if m.Err != nil {
		return ""
	}
	return m.Table
}
