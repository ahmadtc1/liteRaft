package liteRaft

import (
	"fmt"
	"log"
	"math/rand"
	"os"
	"sync"
	"sync/atomic"
	"time"
)

const debugEnable = 1

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

func NewConsensusModule(id int, peerIds []int, server *Server, ready <-chan interface{}) *ConsensusModule {
	cm := new(ConsensusModule)
	cm.id = id
	cm.peerIds = peerIds
	cm.server = server
	cm.state = Follower
	cm.votedFor = -1

	go func() {
		// cm is quet until ready signalled, then start a countdown for leader election
		<-ready
		cm.mu.Lock()
		cm.electionResetEvent = time.Now()
		cm.mu.Unlock()
		cm.runElectionTimer()
	}()
	return cm
}

func (cm *ConsensusModule) debugLog(format string, args ...interface{}) {
	if debugEnable > 0 {
		format = fmt.Sprintf("[%d] ", cm.id) + format
		log.Printf(format, args...)
	}
}

func (cm *ConsensusModule) Report() (id int, term int, isLeader bool) {
	cm.mu.Lock()
	defer cm.mu.Unlock()
	return cm.id, cm.currentTerm, cm.state == Leader
}

func (cm *ConsensusModule) Stop() {
	cm.mu.Lock()
	defer cm.mu.Unlock()
	cm.state = Dead
	cm.debugLog("id: %d becomes dead", cm.id)
}

type RequestVoteArgs struct {
	Term         int
	CandidateId  int
	LastLogIndex int
	LastLogTerm  int
}

type RequestVoteReply struct {
	Term        int
	VoteGranted bool
}

type AppendEntriesArgs struct {
	Term     int
	LeaderId int

	PrevLogIndex int
	PrevLogTerm  int
	Entries      []LogEntry
	LeaderCommit int
}

type AppendEntriesReply struct {
	Term    int
	Success bool
}

func (cm *ConsensusModule) electionTimeout() time.Duration {
	// if RAFT_FORCE_MORE_REFLECTION is set, stress test delibterately
	// generating a hard-coded number very often to create collisions
	// between different servers and force re-elections
	if len(os.Getenv("RAFT_FORCE_MORE_REELECTION")) > 0 && rand.Intn(3) == 0 {
		return time.Duration(150) * time.Millisecond
	} else {
		return time.Duration(150+rand.Intn(150)) * time.Millisecond
	}
}

func (cm *ConsensusModule) runElectionTimer() {
	timeoutDuration := cm.electionTimeout()
	cm.mu.Lock()
	termStarted := cm.currentTerm
	cm.mu.Unlock()
	cm.debugLog("Election timer started (%v), term = %d", timeoutDuration, termStarted)

	// loops until either
	// we discover election timer isn't needed anymore
	// or election timer expires and this cm becomes a candidate
	// in follower, typically keeps running
	ticker := time.NewTicker(10 * time.Millisecond)
	defer ticker.Stop()

	// loop indefinitely and keep checking conditions to see if we need to start election
	for {
		<-ticker.C

		cm.mu.Lock()
		// nor a candidate nor a follower
		if cm.state != Candidate && cm.state != Follower {
			cm.debugLog("in election timer, state=%s, bailing out", cm.state)
			cm.mu.Unlock()
			return
		}

		// term has been updated since start of function
		if termStarted != cm.currentTerm {
			cm.debugLog("In election timer. term changed from %d to %d, bailing out", termStarted, cm.currentTerm)
			cm.mu.Unlock()
			return
		}

		// if we haven't heard from a leader or haven't voted for someone for duration of
		// timeout, start election
		if elapsed := time.Since(cm.electionResetEvent); elapsed > timeoutDuration {
			cm.startElection()
			cm.mu.Unlock()
			return
		}
		cm.mu.Unlock()
	}
}

func (cm *ConsensusModule) startElection() {
	cm.state = Candidate
	cm.currentTerm += 1
	savedCurrentTerm := cm.currentTerm
	cm.electionResetEvent = time.Now()
	cm.votedFor = cm.id
	cm.debugLog("Becomes Candidate (currentTerm=%d); log=%v", savedCurrentTerm, cm.log)

	var votesReceived int32 = 1

	// concurrently send RequestVote RPCs to to all servers concurrently
	for _, peerId := range cm.peerIds {
		go func(peerId int) {
			args := RequestVoteArgs{
				Term:        savedCurrentTerm,
				CandidateId: cm.id,
			}
			var reply RequestVoteReply

			cm.debugLog("Sending RequestVote to %d: %+v", peerId, args)
			if err := cm.server.Call(peerId, "ConsensusModule.RequestVote", args, &reply); err == nil {
				cm.mu.Lock()
				defer cm.mu.Unlock()
				cm.debugLog("Received RequestVoteReply %+v", reply)

				// if no longer candidate, bail out
				// can happen if we already won election or switched to follower
				if cm.state != Candidate {
					cm.debugLog("While waiting for reply, state=%v", cm.state)
					return
				}

				//revert to follower state if reply's term is higher than ours
				if reply.Term > savedCurrentTerm {
					cm.debugLog("Term out of date in RequestVoteReply")
					cm.becomeFollower(reply.Term)
					return
				} else if reply.Term == savedCurrentTerm {
					if reply.VoteGranted {
						votes := int(atomic.AddInt32(&votesReceived, 1))
						if votes*2 > len(cm.peerIds)+1 {
							// we won the election!
							cm.debugLog("wins election with %d votes", votes)
							cm.startLeader()
							return
						}
					}
				}
			}
		}(peerId)
	}

	// run another election timer in case the election isn't succesful
	go cm.runElectionTimer()
}

func (cm *ConsensusModule) startLeader() {
	cm.state = Leader
	cm.debugLog("Becomes leader; term=%d, log=%v", cm.currentTerm, cm.log)

	go func() {
		ticker := time.NewTicker(50 * time.Millisecond)
		defer ticker.Stop()

		// send periodic heartbeats every 50ms while in leader state
		for {
			cm.leaderSendHeartbeats()
			<-ticker.C

			cm.mu.Lock()
			if cm.state != Leader {
				cm.mu.Unlock()
				return
			}
			cm.mu.Unlock()
		}
	}()
}

func (cm *ConsensusModule) leaderSendHeartbeats() {
	cm.mu.Lock()
	savedCurrentTerm := cm.currentTerm
	cm.mu.Unlock()

	for _, peerId := range cm.peerIds {
		args := AppendEntriesArgs{
			Term:     savedCurrentTerm,
			LeaderId: cm.id,
		}
		go func(peerId int) {
			cm.debugLog("Sending AppendEntries to %v: ni=%d, args=%+v", peerId, 0, args)
			var reply AppendEntriesReply
			if err := cm.server.Call(peerId, "ConsensusModule.AppendEntries", args, reply); err == nil {
				cm.mu.Lock()
				defer cm.mu.Unlock()
				if reply.Term > savedCurrentTerm {
					cm.debugLog("Term out of date in heartbeat reply")
					cm.becomeFollower(reply.Term)
					return
				}
			}
		}(peerId)
	}
}

func (cm *ConsensusModule) becomeFollower(term int) {
	cm.debugLog("Becomes followe with term=%d, log=%v", term, cm.log)
	cm.state = Follower
	cm.currentTerm = term
	cm.votedFor = -1
	cm.electionResetEvent = time.Now()

	go cm.runElectionTimer()
}

// RequestVote RPC
func (cm *ConsensusModule) RequestVote(args RequestVoteArgs, reply *RequestVoteReply) error {
	cm.mu.Lock()
	defer cm.mu.Unlock()
	if cm.state == Dead {
		return nil
	}
	cm.debugLog("RequestVote: %+v [currentTerm=%d, votedFor=%d]", args, cm.currentTerm, cm.votedFor)

	if args.Term > cm.currentTerm {
		cm.debugLog("... term out of date in RequestVote")
		cm.becomeFollower(args.Term)
	}

	if cm.currentTerm == args.Term && (cm.votedFor == -1 || cm.votedFor == args.CandidateId) {
		cm.votedFor = args.CandidateId
		cm.electionResetEvent = time.Now()
	} else {
		reply.VoteGranted = false
	}

	reply.Term = cm.currentTerm
	cm.debugLog("... RequestVote reply: %+v", reply)
	return nil
}

func (cm *ConsensusModule) AppendEntries(args AppendEntriesArgs, reply *AppendEntriesReply) error {
	cm.mu.Lock()
	defer cm.mu.Unlock()
	if cm.state == Dead {
		return nil
	}
	cm.debugLog("AppendEntries: %+v", args)

	if args.Term > cm.currentTerm {
		cm.debugLog("...Term out of date in AppendEntries")
		cm.becomeFollower(args.Term)
	}

	reply.Success = false
	if args.Term == cm.currentTerm {
		if cm.state != Follower {
			cm.becomeFollower(args.Term)
		}
		cm.electionResetEvent = time.Now()
		reply.Success = true
	}

	reply.Term = cm.currentTerm
	cm.debugLog("AppendEntries reply: %+v", *reply)
	return nil
}
