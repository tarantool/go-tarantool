package queue

const (
	READY   = "r"
	TAKEN   = "t"
	DONE    = "-"
	BURIED  = "!"
	DELAYED = "~"
)

type queueType string

const (
	FIFO      queueType = "fifo"
	FIFO_TTL  queueType = "fifottl"
	UTUBE     queueType = "utube"
	UTUBE_TTL queueType = "utubettl"
)

type State int

const (
	UnknownState State = iota
	InitState
	StartupState
	RunningState
	EndingState
	WaitingState
)

var strToState = map[string]State{
	"INIT":    InitState,
	"STARTUP": StartupState,
	"RUNNING": RunningState,
	"ENDING":  EndingState,
	"WAITING": WaitingState,
}
