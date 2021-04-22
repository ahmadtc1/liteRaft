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

// CommitEntry is the data reported by raft to the commit channel
// A commit entry notifies the client that consensus was reached on command and it
// can be applied to the client's state machine
type CommitEntry struct {
	// the client command being committed
	Command interface{}

	//the log index at which the client command is committed
	Index int

	// the raft term at which the client command is committed
	Term int
}

type ConsensusModule struct {
	mu sync.Mutex

	// the id of this consensus module
	id int

	// the ids of all our peer servers in the cluster
	peerIds []int

	// the server containing this consensus module. will be used to issue RPCs
	server *Server

	// the channel where CM is going to report committed log entries
	// passed in by client during construction
	commitChan chan<- CommitEntry

	// internal notif channel used by goroutines to commit new entries to
	// log to notify that these entries can be sent on commitChan
	newCommitReadyChan chan struct{}

	// persistens raft state on all servers
	currentTerm int
	votedFor    int
	log         []LogEntry

	// volatile raft state on all servers
	commitIndex        int
	lastApplied        int
	state              CMState
	electionResetEvent time.Time

	// Volatile Raft state on leaders
	nextIndex  map[int]int
	matchIndex map[int]int
}

func NewConsensusModule(id int, peerIds []int, server *Server, ready <-chan interface{}) *ConsensusModule {
	cm := new(ConsensusModule)
	cm.id = id
	cm.peerIds = peerIds
	cm.server = server
	cm.state = Follower
	cm.votedFor = -1
	cm.lastApplied = -1
	cm.nextIndex = make(map[int]int)
	cm.matchIndex = make(map[int]int)

	go func() {
		// cm is quet until ready signalled, then start a countdown for leader election
		<-ready
		cm.mu.Lock()
		cm.electionResetEvent = time.Now()
		cm.mu.Unlock()
		cm.runElectionTimer()
	}()

	go cm.commitChanSender()
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
	close(cm.newCommitReadyChan)
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
			cm.mu.Lock()
			savedLastLogIndex, savedLastLogTerm := cm.lastLogIndexAndTerm()
			cm.mu.Unlock()

			args := RequestVoteArgs{
				Term:         savedCurrentTerm,
				CandidateId:  cm.id,
				LastLogIndex: savedLastLogIndex,
				LastLogTerm:  savedLastLogTerm,
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

func (cm *ConsensusModule) lastLogIndexAndTerm() (int, int) {
	if len(cm.log) > 0 {
		lastIndex := len(cm.log) - 1
		return lastIndex, cm.log[lastIndex].Term
	} else {
		return -1, -1
	}
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
		go func(peerId int) {
			cm.mu.Lock()
			// get the next index for the next log entry for this peer
			ni := cm.nextIndex[peerId]
			// set the prevLogIndex to the one before the next index
			prevLogIndex := ni - 1
			// init the previous log term to -1 (hasn't happened)
			prevLogTerm := -1
			// if we have some entries in our log, determine the previous term using our prevLogIndex
			if prevLogIndex >= 0 {
				prevLogTerm = cm.log[prevLogIndex].Term
			}
			entries := cm.log[ni:]

			args := AppendEntriesArgs{
				Term:         savedCurrentTerm,
				LeaderId:     cm.id,
				PrevLogIndex: prevLogIndex,
				PrevLogTerm:  prevLogTerm,
				Entries:      entries,
				LeaderCommit: cm.commitIndex,
			}
			cm.mu.Unlock()

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

				if cm.state == Leader && savedCurrentTerm == reply.Term {
					if reply.Success {
						cm.nextIndex[peerId] = ni + len(entries)
						cm.matchIndex[peerId] = cm.nextIndex[peerId] - 1
						cm.debugLog("AppendEntries reply from %d success: nextIndex=%d matchIndex=%d", peerId, cm.nextIndex, cm.matchIndex)

						savedCommitIndex := cm.commitIndex
						for i := cm.commitIndex + 1; i < len(cm.log); i++ {
							if cm.log[i].Term == cm.currentTerm {
								matchCount := 1
								for _, peerId := range cm.peerIds {
									if cm.matchIndex[peerId] >= i {
										matchCount++
									}
								}
								if matchCount*2 > len(cm.peerIds)+1 {
									cm.commitIndex = i
								}
							}
						}
						if cm.commitIndex != savedCommitIndex {
							cm.debugLog("Leader sets commitIndex := %d", cm.commitIndex)
							cm.newCommitReadyChan <- struct{}{}
						} else {
							cm.nextIndex[peerId] = ni - 1
							cm.debugLog("AppendEntries reply from %d! success: nextIndex := %d", peerId, ni-1)
						}
					}
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

	lastLogIndex, lastLogTerm := cm.lastLogIndexAndTerm()
	cm.debugLog("RequestVote: %+v [currentTerm=%d, votedFor=%d]", args, cm.currentTerm, cm.votedFor)

	if args.Term > cm.currentTerm {
		cm.debugLog("... term out of date in RequestVote")
		cm.becomeFollower(args.Term)
	}

	if cm.currentTerm == args.Term &&
		(cm.votedFor == -1 || cm.votedFor == args.CandidateId) &&
		(args.LastLogTerm == lastLogTerm && args.LastLogIndex >= lastLogIndex) {

		reply.VoteGranted = true
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

		// check if our log contains entry at prevLogIndex whose term matches prevLogTerm
		if args.PrevLogIndex == -1 ||
			(args.PrevLogIndex < len(cm.log) && args.PrevLogTerm == cm.log[args.PrevLogIndex].Term) {
			reply.Success = true

			// find insertion point
			logInsertIndex := args.PrevLogIndex + 1
			newEntriesIndex := 0

			for {
				if logInsertIndex > len(cm.log) || newEntriesIndex >= len(args.Entries) {
					break
				} else if cm.log[logInsertIndex].Term != args.Entries[newEntriesIndex].Term {
					break
				}
				logInsertIndex++
				newEntriesIndex++
			}

			// by end of loop above
			// logInsertIndex points to end of log or to index where term mismatches w leader entry
			// newEntries points at end of entries or index where term mismatches w corresp. log entry
			if newEntriesIndex > len(args.Entries) {
				cm.debugLog("Inserting entries %v from index %d", args.Entries[newEntriesIndex:], logInsertIndex)
				cm.log = append(cm.log[:logInsertIndex], args.Entries[newEntriesIndex:]...)
				cm.debugLog("... Log is now %v", cm.log)
			}

			// set commit index
			if args.LeaderCommit > cm.commitIndex {
				cm.commitIndex = intMin(args.LeaderCommit, len(cm.log)-1)
				cm.debugLog("...setting commitIndex=%d", cm.commitIndex)
				cm.newCommitReadyChan <- struct{}{}
			}
		}
	}

	reply.Term = cm.currentTerm
	cm.debugLog("AppendEntries reply: %+v", *reply)
	return nil
}

func (cm *ConsensusModule) submit(command interface{}) bool {
	cm.mu.Lock()
	defer cm.mu.Unlock()

	cm.debugLog("Submit received by %v: %v", cm.state, command)
	if cm.state == Leader {
		cm.log = append(cm.log, LogEntry{Command: command, Term: cm.currentTerm})
		cm.debugLog("...log=%v", cm.log)
		return true
	}
	return false
}

func (cm *ConsensusModule) commitChanSender() {
	for range cm.newCommitReadyChan {
		// discover which entries we have to apply
		cm.mu.Lock()
		savedTerm := cm.currentTerm
		savedLastApplied := cm.lastApplied
		var entries []LogEntry
		if cm.commitIndex > cm.lastApplied {
			entries = cm.log[cm.lastApplied+1 : cm.commitIndex+1]
			cm.lastApplied = cm.commitIndex
		}
		cm.mu.Unlock()
		cm.debugLog("commitChanSender entries=%v, savedLastApplied=%d", entries, savedLastApplied)

		for i, entry := range entries {
			cm.commitChan <- CommitEntry{
				Command: entry.Command,
				Index:   savedLastApplied + i + 1,
				Term:    savedTerm,
			}
		}
	}
	cm.debugLog("commitChanSender done")
}

func intMin(a int, b int) int {
	if a < b {
		return a
	}
	return b
}
