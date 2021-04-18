package liteRaft

import (
	"sync"
	"time"
)

type CMState int

const (
	Follower CMState = iota
	Candidate
	Leader
	Dead
)

func (s CMState) String() string {
	switch s {
	case Follower:
		return "Follower"
	case Candidate:
		return "Candidate"
	case Leader:
		return "Leader"
	case Dead:
		return "Dead"
	default:
		return "Unreachable"
	}
}

type LogEntry struct {
	Command interface{}
	Term    int
}

type ConsensusModule struct {
	mu sync.Mutex

	// the id of this consensus module
	id int

	// the ids of all our peer servers in the cluster
	peerIds []int

	// the server containing this consensus module. will be used to issue RPCs
	server *Server

	// persistens raft state on all servers
	currentTerm int
	votedFor    int
	log         []LogEntry

	// volatile raft state on all servers
	state              CMState
	electionResetEvent time.Time
}
