package scheduler

// Priority selects which of a Scheduler's two external queues a
// submitted task lands on. It only affects tasks that reach the shared
// queues (external Submit calls, and local-deque overflow); a task
// spawned from within an already-running task on the same Scheduler
// always goes straight to that worker's own local deque regardless of
// Priority, since there is no queue contention to arbitrate there.
type Priority int

const (
	// PriorityStandard is served ahead of PriorityBackground whenever
	// both queues are non-empty.
	PriorityStandard Priority = iota
	// PriorityBackground is served after PriorityStandard, except that
	// any PriorityBackground task waiting past the Scheduler's aging
	// threshold is served immediately, bounding its worst-case wait
	// regardless of how much PriorityStandard work keeps arriving.
	PriorityBackground
)
