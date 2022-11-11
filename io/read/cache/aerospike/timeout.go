package aerospike

type TimeoutConfig struct {
	MaxRetries            int
	TotalTimeoutMs        int
	SleepBetweenRetriesMs int
}

func (t *TimeoutConfig) Init() {
	if t.MaxRetries == 0 {
		t.MaxRetries = 3
	}
	if t.TotalTimeoutMs == 0 {
		t.TotalTimeoutMs = 500
	}
	if t.SleepBetweenRetriesMs == 0 {
		t.SleepBetweenRetriesMs = 100
	}

}
