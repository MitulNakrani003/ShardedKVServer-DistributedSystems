package raft

//
// this is an outline of the API that raft must expose to
// the service (or tester). see comments below for
// each of these functions for more details.
//
// rf = Make(...)
//   create a new Raft server.
// rf.Start(command interface{}) (index, term, isleader)
//   start agreement on a new log entry
// rf.GetState() (term, isLeader)
//   ask a Raft for its current term, and whether it thinks it is leader
// ApplyMsg
//   each time a new entry is committed to the log, each Raft peer
//   should send an ApplyMsg to the service (or tester)
//   in the same server.
//

import (
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	"../labrpc"
)

import "bytes"
import "../labgob"

//=====================================================================================================================================
// STRUCT DEFINITIONS
//=====================================================================================================================================
//=====================================================================================================================================

// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in Lab 3 you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh; at that point you can add fields to
// ApplyMsg, but set CommandValid to false for these other uses.
type ApplyMsg struct {
	CommandValid bool        // true if Command contains a newly committed log entry
	Command      interface{} // the command to apply to the state machine
	CommandIndex int         // the index of the command in the log

	//SnapShot
	SnapshotValid bool   // true if Snapshot contains a newly created snapshot
	Snapshot      []byte // the snapshot to be installed
	SnapshotIndex int    // the index up to which the snapshot is valid
	SnapshotTerm  int    // the term of the last included entry in the snapshot
}

// LogEntry contains a command for the state machine, and the term when the
// entry was received by the leader.
type LogEntry struct {
	Command interface{} // command for state machine
	Term    int         // term when entry was received by leader
}

type State int

const (
	Follower State = iota
	Candidate
	Leader
)

// A Go object implementing a single Raft peer.
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	currentTerm int        // latest term server has seen
	votedFor    int        // candidateId that received vote in current term (or null if none)
	state       State      // server state: Follower, Candidate, Leader
	log         []LogEntry // log entries

	electionTimer *time.Timer // election timer

	commitedIndex    int           // index of highest log entry known to be committed
	lastAppliedIndex int           // index of highest log entry applied to state machine
	nextIndex        []int         // for each server, index of the next log entry to send to that server
	matchIndex       []int         // for each server, index of highest log entry known to be replicated on server
	applyCh          chan ApplyMsg // channel to send ApplyMsg to service (or tester)
	applyCond        *sync.Cond    // Used to signal the applier goroutine

	lastIncludedIndex int // in Snapshot // index of the last entry included in the snapshot
	lastIncludedTerm  int // in Snapshot // term of the last entry included in the snapshot
}

type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	CandidateId  int // candidate requesting vote
	Term         int // candidate's term
	LastLogIndex int // index of candidate's last log entry
	LastLogTerm  int // term of candidate's last log entry
}

// example RequestVote RPC reply structure.
// field names must start with capital letters!
type RequestVoteReply struct {
	Term        int  // currentTerm, for candidate to update itself
	VoteGranted bool // true means candidate received vote
}

type AppendEntriesArgs struct {
	Term         int        // leader's term
	LeaderId     int        // so follower can redirect clients
	PrevLogIndex int        // index of log entry immediately preceding new ones
	PrevLogTerm  int        // term of prevLogIndex entry
	Entries      []LogEntry // log entries to store (empty for heartbeat; may send more than one for efficiency)
	LeaderCommit int        // leader's commitIndex
}

type AppendEntriesReply struct {
	Term          int  // currentTerm, for leader to update itself
	Success       bool // true if follower contained entry matching prevLogIndex and prevLogTerm
	ConflictIndex int  // index of first entry with term equal to ConflictTerm
	ConflictTerm  int  // term of the conflicting entry
}

type InstallSnapshotArgs struct {
	Term              int    // leader's term
	LeaderId          int    // so follower can redirect clients
	LastIncludedIndex int    // the snapshot replaces all entries up through and including this index
	LastIncludedTerm  int    // term of lastIncludedIndex
	Data              []byte // raw bytes of the snapshot chunk
}

type InstallSnapshotReply struct {
	Term int // currentTerm, for leader to update itself
}

//=====================================================================================================================================
// GETTERS
//=====================================================================================================================================

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	var term int
	var isleader bool

	rf.mu.Lock()
	defer rf.mu.Unlock()
	term = rf.currentTerm
	isleader = (rf.state == Leader)
	return term, isleader
}

func (rf *Raft) GetRaftStateSize() int {
	return rf.persister.RaftStateSize()
}

func (rf *Raft) GetLastApplied() int {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.lastAppliedIndex
}

// Converts between global log index which is the absolute index in the log and local log index is the index in the log array
func (rf *Raft) getGlobalIndex(localIndex int) int {
	return rf.lastIncludedIndex + localIndex
}

// Converts between global log index which is the absolute index in the log and local log index is the index in the log array
func (rf *Raft) getLocalIndex(globalIndex int) int {
	return globalIndex - rf.lastIncludedIndex
}

// Gets the log entry at the given global index
func (rf *Raft) getLogEntry(globalIndex int) LogEntry {
	return rf.log[rf.getLocalIndex(globalIndex)]
}

// Gets the last log index in global indexing
func (rf *Raft) getMyLastLogIndex() int {
	return rf.getGlobalIndex(len(rf.log) - 1)
}

// Gets the last log term in global indexing
func (rf *Raft) getMyLastLogTerm() int {
	return rf.getLogEntry(rf.getMyLastLogIndex()).Term
}

//=====================================================================================================================================
// RAFT PERSISTENCE METHODS
//=====================================================================================================================================

// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
func (rf *Raft) persist() {
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(rf.currentTerm)
	e.Encode(rf.votedFor)
	e.Encode(rf.log)
	e.Encode(rf.lastIncludedIndex)
	e.Encode(rf.lastIncludedTerm)
	data := w.Bytes()
	rf.persister.SaveRaftState(data)
}

// restore previously persisted state.
func (rf *Raft) readPersist(data []byte) {
	if len(data) < 1 { // bootstrap without any state?
		return
	}
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var currentTerm int
	var votedFor int
	var log []LogEntry
	var lastIncludedIndex int
	var lastIncludedTerm int

	if d.Decode(&currentTerm) != nil ||
		d.Decode(&votedFor) != nil ||
		d.Decode(&log) != nil ||
		d.Decode(&lastIncludedIndex) != nil ||
		d.Decode(&lastIncludedTerm) != nil {
		DPrintf("[%d]: Error decoding persisted state", rf.me)
	} else {
		rf.currentTerm = currentTerm
		rf.votedFor = votedFor
		rf.log = log
		rf.lastIncludedIndex = lastIncludedIndex
		rf.lastIncludedTerm = lastIncludedTerm
	}
}

//=====================================================================================================================================
// RAFT HELPER METHODS
//=====================================================================================================================================

// startElection is called to start a new election.
func (rf *Raft) startElection() {
	rf.currentTerm++
	rf.state = Candidate
	rf.votedFor = rf.me
	rf.persist()
	rf.electionTimer.Reset(randomizedElectionTimeout())

	DPrintf("Server %d starting election for term %d", rf.me, rf.currentTerm)

	args := &RequestVoteArgs{
		Term:         rf.currentTerm,
		CandidateId:  rf.me,
		LastLogIndex: rf.getMyLastLogIndex(),
		LastLogTerm:  rf.getMyLastLogTerm(),
	}

	votesReceived := 1 // Vote for self

	for i := range rf.peers {
		if i == rf.me {
			continue
		}

		go func(serverIndex int) {
			reply := &RequestVoteReply{}
			ok := rf.sendRequestVote(serverIndex, args, reply)

			if ok {
				rf.mu.Lock()
				defer rf.mu.Unlock()

				if rf.state != Candidate || rf.currentTerm != args.Term {
					return
				}

				if reply.Term > rf.currentTerm {
					rf.becomeFollower(reply.Term)
					rf.persist()
					return
				}

				if reply.VoteGranted {
					votesReceived++
					if votesReceived > len(rf.peers)/2 {
						if rf.state == Candidate {
							DPrintf("Server %d received majority votes and is becoming leader for term %d", rf.me, rf.currentTerm)
							rf.becomeLeader()
							rf.broadcastAppendEntries() // Send initial empty AppendEntries RPCs (heartbeats) to each server
						}
					}
				}
			}
		}(i)
	}
}

// Must be called with lock held.
func (rf *Raft) becomeLeader() {
	if rf.state != Candidate {
		return
	}
	rf.state = Leader
	lastLogIndex := rf.getMyLastLogIndex()
	lastLogTerm := rf.getMyLastLogTerm()
	DPrintf("Server %d became leader for term %d at log index %d, term %d", rf.me, rf.currentTerm, lastLogIndex, lastLogTerm)
	rf.nextIndex = make([]int, len(rf.peers))
	rf.matchIndex = make([]int, len(rf.peers))
	for i := range rf.peers {
		rf.nextIndex[i] = lastLogIndex + 1
		rf.matchIndex[i] = 0
	}

	go rf.heartbeatLoop()
}

// Must be called with lock held.
func (rf *Raft) becomeFollower(term int) {
	rf.state = Follower
	rf.currentTerm = term
	rf.votedFor = -1
}

// Helper function to get a randomized election timeout
func randomizedElectionTimeout() time.Duration {
	return time.Duration(300+rand.Intn(300)) * time.Millisecond
}

// Must be called with lock held.
// Update the commit index if there exists an N such that N > commitIndex,
func (rf *Raft) updateCommitIndex() {
	for N := rf.getMyLastLogIndex(); N > rf.commitedIndex; N-- { // Check from last log index of leader down to current commit index + 1
		if rf.getLogEntry(N).Term == rf.currentTerm { // Only consider entries from current term
			count := 1 // Count self
			for i := range rf.peers {
				if i != rf.me && rf.matchIndex[i] >= N {
					count++
				}
			}
			if count > len(rf.peers)/2 { // Majority of servers have replicated this entry
				rf.commitedIndex = N
				rf.applyCond.Signal() // Wake up applier
				break
			}
		}
	}
}

func (rf *Raft) replicateLogToPeer(server int) {
	rf.mu.Lock()
	if rf.state != Leader {
		rf.mu.Unlock()
		return
	}

	if rf.nextIndex[server] <= rf.lastIncludedIndex {
		rf.mu.Unlock()
		rf.sendSnapshotToPeer(server)
		return	
	}

	// Building the Append Entries Arguments
	prevLogIndex := rf.nextIndex[server] - 1 // Index of peer's log entry immediately before new ones
	prevLogTerm := rf.getLogEntry(prevLogIndex).Term // Term of peer's prevLogIndex entry
	entries := make([]LogEntry, rf.getMyLastLogIndex()-prevLogIndex)
	copy(entries, rf.log[rf.getLocalIndex(prevLogIndex+1):])

	args := AppendEntriesArgs{
		Term:         rf.currentTerm,
		LeaderId:     rf.me,
		PrevLogIndex: prevLogIndex,
		PrevLogTerm:  prevLogTerm,
		Entries:      entries,
		LeaderCommit: rf.commitedIndex,
	}
	rf.mu.Unlock()

	reply := AppendEntriesReply{}
	ok := rf.sendAppendEntries(server, &args, &reply)
	//
	if ok {
		rf.mu.Lock()
		defer rf.mu.Unlock()

		if rf.state != Leader || rf.currentTerm != args.Term {
			return
		}

		if reply.Term > rf.currentTerm {
			rf.becomeFollower(reply.Term)
			rf.persist()
			return
		}

		if reply.Success {
			rf.matchIndex[server] = args.PrevLogIndex + len(args.Entries)
			rf.nextIndex[server] = rf.matchIndex[server] + 1
			rf.updateCommitIndex()
		} else {
			// Log inconsistency. Decrement nextIndex and retry.
			if reply.ConflictTerm != -1 {
				// Find the last index of ConflictTerm in our log
				lastConflictTermIndex := -1
				for i := rf.getMyLastLogIndex(); i > rf.lastIncludedIndex; i-- {
					if rf.getLogEntry(i).Term == reply.ConflictTerm {
						lastConflictTermIndex = i
						break
					}
				}
				if lastConflictTermIndex != -1 {
					rf.nextIndex[server] = lastConflictTermIndex + 1
				} else {
					rf.nextIndex[server] = reply.ConflictIndex
				}
			} else {
				rf.nextIndex[server] = reply.ConflictIndex
			}
			// nextIndex doesn't go below 1
			if rf.nextIndex[server] < 1 {
				rf.nextIndex[server] = 1
			}
		}
	}
}

// Broadcast AppendEntries RPCs to all peers.
func (rf *Raft) broadcastAppendEntries() {
	for i := range rf.peers {
		if i == rf.me {
			continue
		}
		go rf.replicateLogToPeer(i)
	}
}

// Heartbeat loop that sends periodic heartbeats to follower peers.
func (rf *Raft) heartbeatLoop() {
	heartbeatInterval := 100 * time.Millisecond
	timer := time.NewTimer(heartbeatInterval)
	defer timer.Stop()

	for !rf.killed() {
		rf.mu.Lock()
		if rf.state != Leader {
			rf.mu.Unlock()
			return
		}
		rf.broadcastAppendEntries()
		rf.mu.Unlock()

		// Wait for the next heartbeat interval.
		<-timer.C
		timer.Reset(heartbeatInterval)
	}
}

//=====================================================================================================================================
// SNAPSHOT METHODS
//=====================================================================================================================================

func (rf *Raft) SaveSnapshot(index int, snapshot []byte) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if index <= rf.lastIncludedIndex {
		return
	}

	// Trim the log entries up to the snapshot index. This considers the first index is always a dummy entry.
	rf.lastIncludedTerm = rf.log[index-rf.lastIncludedIndex].Term

	newLog := make([]LogEntry, 1)
	newLog[0].Term = rf.getLogEntry(index).Term
	newLog = append(newLog, rf.log[rf.getLocalIndex(index+1):]...)

	rf.lastIncludedTerm = rf.getLogEntry(index).Term
	rf.lastIncludedIndex = index
	rf.log = newLog

	// Update commit and applied indices
	if rf.commitedIndex < index {
		rf.commitedIndex = index
	}
	if rf.lastAppliedIndex < index {
		rf.lastAppliedIndex = index
	}

	// Persist the new, smaller log and the snapshot metadata.
	// Also save the snapshot data itself.
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(rf.currentTerm)
	e.Encode(rf.votedFor)
	e.Encode(rf.log)
	e.Encode(rf.lastIncludedIndex)
	e.Encode(rf.lastIncludedTerm)
	raftState := w.Bytes()
	rf.persister.SaveStateAndSnapshot(raftState, snapshot)
}

// Sends an InstallSnapshot RPC to a follower
func (rf *Raft) sendSnapshotToPeer(server int) {
	rf.mu.Lock()
	args := InstallSnapshotArgs{
		Term:              rf.currentTerm,
		LeaderId:          rf.me,
		LastIncludedIndex: rf.lastIncludedIndex,
		LastIncludedTerm:  rf.lastIncludedTerm,
		Data:              rf.persister.ReadSnapshot(),
	}
	rf.mu.Unlock()

	reply := InstallSnapshotReply{}
	ok := rf.sendInstallSnapshot(server, &args, &reply)

	if ok {
		rf.mu.Lock()
		defer rf.mu.Unlock()
		if rf.state != Leader || rf.currentTerm != args.Term {
			return
		}
		if reply.Term > rf.currentTerm {
			rf.becomeFollower(reply.Term)
			rf.persist()
			return
		}
		// If successful, update nextIndex and matchIndex for the follower
		rf.matchIndex[server] = args.LastIncludedIndex
		rf.nextIndex[server] = args.LastIncludedIndex + 1
	}
}

// A service wants to switch to snapshot.  Only do so if Raft hasn't
// have more recent info since it communicate the snapshot on applyCh.
func (rf *Raft) ConditionToInstallSnapshot(lastIncludedTerm int, lastIncludedIndex int, snapshot []byte) bool {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	// If we have a more up-to-date committed log, reject the snapshot
	if lastIncludedIndex <= rf.commitedIndex {
		return false
	}

	if lastIncludedIndex > rf.getMyLastLogIndex() {
		// Snapshot is ahead of our log, replace our log entirely
		rf.log = make([]LogEntry, 1)
	} else {
		// Truncate our log and keep entries after lastIncludedIndex
		newLog := make([]LogEntry, 1)
		newLog = append(newLog, rf.log[rf.getLocalIndex(lastIncludedIndex+1):]...)
		rf.log = newLog
	}

	rf.log[0].Term = lastIncludedTerm
	rf.lastIncludedIndex = lastIncludedIndex
	rf.lastIncludedTerm = lastIncludedTerm

	// Update commit and applied indices
	if rf.commitedIndex < lastIncludedIndex {
		rf.commitedIndex = lastIncludedIndex
	}
	if rf.lastAppliedIndex < lastIncludedIndex {
		rf.lastAppliedIndex = lastIncludedIndex
	}

	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(rf.currentTerm)
	e.Encode(rf.votedFor)
	e.Encode(rf.log)
	e.Encode(rf.lastIncludedIndex)
	e.Encode(rf.lastIncludedTerm)
	raftState := w.Bytes()
	rf.persister.SaveStateAndSnapshot(raftState, snapshot)

	return true
}

//=====================================================================================================================================
// ROUTINES
//=====================================================================================================================================

// Goroutine that applies committed log entries to the state machine.
func (rf *Raft) applier() {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	for !rf.killed() {
		if rf.commitedIndex <= rf.lastAppliedIndex {
			rf.applyCond.Wait()
		} else {
			// Apply log entries from lastApplied up to commitIndex.
			rf.lastAppliedIndex++
			applyMsg := ApplyMsg{
				CommandValid: true,
				Command:      rf.getLogEntry(rf.lastAppliedIndex).Command,
				CommandIndex: rf.lastAppliedIndex,
			}

			rf.mu.Unlock()
			rf.applyCh <- applyMsg
			rf.mu.Lock()
		}
	}
}

// The ticker go routine starts a new election if this peer hasn't received heartbeats recently.
func (rf *Raft) ticker() {
	for !rf.killed() {
		<-rf.electionTimer.C
		rf.mu.Lock()
		if rf.state != Leader {
			rf.startElection()
		}
		rf.electionTimer.Reset(randomizedElectionTimeout())
		rf.mu.Unlock()
	}
}

//=====================================================================================================================================
// RPC HANDLERS
//=====================================================================================================================================

// example RequestVote RPC handler.
func (rf *Raft) RequestVoteRPC(args *RequestVoteArgs, reply *RequestVoteReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	defer rf.persist()

	DPrintf("[%d] S%d T%d: Received RequestVoteRPC from %d at T%d", rf.me, rf.state, rf.currentTerm, args.CandidateId, args.Term)

	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		reply.VoteGranted = false
		return
	}

	if args.Term > rf.currentTerm {
		rf.becomeFollower(args.Term)
	}

	canVote := rf.votedFor == -1 || rf.votedFor == args.CandidateId

	myLastLogTerm := rf.getMyLastLogTerm()
	myLastLogIndex := rf.getMyLastLogIndex()

	isLogUpToDate := (args.LastLogTerm > myLastLogTerm) || (args.LastLogTerm == myLastLogTerm && args.LastLogIndex >= myLastLogIndex)

	if canVote && isLogUpToDate {
		rf.votedFor = args.CandidateId
		reply.VoteGranted = true
		rf.electionTimer.Reset(randomizedElectionTimeout())
	} else {
		reply.VoteGranted = false
	}

}

func (rf *Raft) AppendEntriesRPC(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	defer rf.persist()

	if args.Term < rf.currentTerm { // Reply false if leader's term < peer's currentTerm
		reply.Term = rf.currentTerm
		reply.Success = false
		return
	}

	if args.Term > rf.currentTerm { // If RPC term is higher, convert to follower
		rf.becomeFollower(args.Term)
	} else {
		rf.state = Follower // Reset to follower on receiving AppendEntries with same term
	}

	// Reset election time: this is done on receiving valid AppendEntries RPC no matter what
	// Prevents unnecessary elections on network delays or partitions
	rf.electionTimer.Reset(randomizedElectionTimeout())

	reply.Term = rf.currentTerm

	if args.PrevLogIndex < rf.lastIncludedIndex {
		reply.Success = false
		reply.ConflictIndex = rf.lastIncludedIndex + 1
		reply.ConflictTerm = -1
		return
	}

	if args.PrevLogIndex > rf.getMyLastLogIndex() { // Follower's log is too short
		reply.Success = false
		reply.ConflictIndex = rf.getMyLastLogIndex() + 1 // Use helper
		reply.ConflictTerm = -1
		return
	}
	if rf.getLogEntry(args.PrevLogIndex).Term != args.PrevLogTerm { // Term mismatch
		reply.Success = false
		reply.ConflictTerm = rf.getLogEntry(args.PrevLogIndex).Term // Use helper
		// Find the first index of the conflicting term
		firstIndexOfTerm := rf.lastIncludedIndex
		for i := args.PrevLogIndex; i > rf.lastIncludedIndex; i-- {
			if rf.getLogEntry(i-1).Term != reply.ConflictTerm { // Use helper
				firstIndexOfTerm = i
				break
			}
		}
		reply.ConflictIndex = firstIndexOfTerm
		return
	}

	for i, entry := range args.Entries {
		logIndex := args.PrevLogIndex + 1 + i  // Calculate the index in the log for this entry
		if logIndex > rf.getMyLastLogIndex() { // Append any new entries not already in the log
			rf.log = append(rf.log, args.Entries[i:]...)
			break // Breaks once all the entires are appended
		}
		if rf.getLogEntry(logIndex).Term != entry.Term { // If an existing entry conflicts with a new one (same index but different terms), delete the existing entry and all that follow it
			rf.log = rf.log[:rf.getLocalIndex(logIndex)]
			rf.log = append(rf.log, args.Entries[i:]...)
			break
		}
	}

	reply.Success = true

	// Update commit index of follower if LeaderCommit > peer's commitIndex
	// The follower's commit index update code runs on the SECOND AppendEntries RPC as the first one might not have any entries
	// First RPC: Carries the new log entries + OLD LeaderCommit
	// Second RPC: Carries NEW LeaderCommit (after leader committed)
	if args.LeaderCommit > rf.commitedIndex {
		lastNewEntryIndex := args.PrevLogIndex + len(args.Entries)
		rf.commitedIndex = min(args.LeaderCommit, lastNewEntryIndex)
		rf.applyCond.Signal() // Wake up applier
	}
}

func (rf *Raft) InstallSnapshotRPC(args *InstallSnapshotArgs, reply *InstallSnapshotReply) {
	rf.mu.Lock()

	reply.Term = rf.currentTerm
	if args.Term < rf.currentTerm {
		rf.mu.Unlock()
		return
	}

	if args.Term > rf.currentTerm {
		rf.becomeFollower(args.Term)
		rf.persist()
	}
	rf.electionTimer.Reset(randomizedElectionTimeout())
	rf.mu.Unlock()

	// Send snapshot to the service layer to apply
	// This is done without holding the lock to avoid deadlock
	rf.applyCh <- ApplyMsg{
		SnapshotValid: true,
		Snapshot:      args.Data,
		SnapshotTerm:  args.LastIncludedTerm,
		SnapshotIndex: args.LastIncludedIndex,
	}
}

//=====================================================================================================================================
// RPC SENDERS
//=====================================================================================================================================

// sendRequestVote is already provided, but you call it like this.
// It's a wrapper around rf.peers[server].Call("Raft.RequestVote", args, reply)
// example code to send a RequestVote RPC to a server.
// server is the index of the target server in rf.peers[].
// expects RPC arguments in args.
// fills in *reply with RPC reply, so caller should
// pass &reply.
// the types of the args and reply passed to Call() must be
// the same as the types of the arguments declared in the
// handler function (including whether they are pointers).
//
// The labrpc package simulates a lossy network, in which servers
// may be unreachable, and in which requests and replies may be lost.
// Call() sends a request and waits for a reply. If a reply arrives
// within a timeout interval, Call() returns true; otherwise
// Call() returns false. Thus Call() may not return for a while.
// A false return can be caused by a dead server, a live server that
// can't be reached, a lost request, or a lost reply.
//
// Call() is guaranteed to return (perhaps after a delay) *except* if the
// handler function on the server side does not return.  Thus there
// is no need to implement your own timeouts around Call().
//
// look at the comments in ../labrpc/labrpc.go for more details.
//
// if you're having trouble getting RPC to work, check that you've
// capitalized all field names in structs passed over RPC, and
// that the caller passes the address of the reply struct with &, not
// the struct itself.
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVoteRPC", args, reply)
	return ok
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntriesRPC", args, reply)
	return ok
}

func (rf *Raft) sendInstallSnapshot(server int, args *InstallSnapshotArgs, reply *InstallSnapshotReply) bool {
	ok := rf.peers[server].Call("Raft.InstallSnapshotRPC", args, reply)
	return ok
}

// the tester doesn't halt goroutines created by Raft after each test,
// but it does call the Kill() method. your code can use killed() to
// check whether Kill() has been called. the use of atomic avoids the
// need for a lock.
//
// the issue is that long-running goroutines use memory and may chew
// up CPU time, perhaps causing later tests to fail and generating
// confusing debug output. any goroutine with a long-running loop
// should call killed() to check whether it should stop.
func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	rf.applyCond.Broadcast() // Wake up applier to exit
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

//=====================================================================================================================================
// RAFT CONSTRUCT
//=====================================================================================================================================

// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election. even if the Raft instance has been killed,
// this function should return gracefully.
//
// the first return value is the index that the command will appear at
// if it's ever committed. the second return value is the current
// term. the third return value is true if this server believes it is
// the leader.
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	lastindex := -1
	term := -1
	isLeader := true

	rf.mu.Lock()
	defer rf.mu.Unlock()

	// If not leader, return false
	if rf.state != Leader {
		isLeader = false
		return lastindex, term, isLeader
	}

	// Create a new log entry
	newEntry := LogEntry{
		Term:    rf.currentTerm,
		Command: command,
	}

	// Append the new entry to the log
	rf.log = append(rf.log, newEntry)

	rf.persist()

	// Return the index and term of the new entry
	lastindex = rf.getMyLastLogIndex()
	term = rf.currentTerm

	// Start log replication to followers
	rf.broadcastAppendEntries()

	return lastindex, term, isLeader
}

// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	// Statoc Initialization
	rf.state = Follower
	rf.votedFor = -1
	rf.currentTerm = 0
	// Initialize log with a dummy entry at index 0
	rf.log = make([]LogEntry, 1)
	rf.log[0] = LogEntry{Term: 0, Command: nil}
	rf.commitedIndex = 0
	rf.lastAppliedIndex = 0

	rf.lastIncludedIndex = 0
	rf.lastIncludedTerm = 0

	// Channel and Condition Variable Initialization
	rf.applyCh = applyCh
	rf.applyCond = sync.NewCond(&rf.mu)

	// Initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	if rf.lastIncludedIndex > 0 {
		// The service needs to be told to install the snapshot from the persister
		go func() {
			rf.applyCh <- ApplyMsg{
				SnapshotValid: true,
				Snapshot:      persister.ReadSnapshot(),
				SnapshotTerm:  rf.lastIncludedTerm,
				SnapshotIndex: rf.lastIncludedIndex,
			}
		}()
	}
	// Adjust indices based on loaded snapshot
	rf.commitedIndex = max(rf.commitedIndex, rf.lastIncludedIndex)
	rf.lastAppliedIndex = max(rf.lastAppliedIndex, rf.lastIncludedIndex)

	rf.electionTimer = time.NewTimer(randomizedElectionTimeout())

	go rf.ticker()
	go rf.applier()

	return rf
}
