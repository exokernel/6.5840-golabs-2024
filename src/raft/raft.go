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
	//	"bytes"

	"fmt"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	//	"6.5840/labgob"
	"6.5840/labrpc"
)

// An Enum to represent the state of the Raft server
type state int

const (
	Follower  state = 0
	Candidate state = 1
	Leader    state = 2
)

const NobodyID = -1
const electionTimeoutMin = 300

// election timeout jitter, we will add a random number of milliseconds between 0 and electionTimeoutVar to the
// election timeout
const electionTimeoutVar = 200

// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in part 3D you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh, but set CommandValid to false for these
// other uses.
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int

	// For 3D:
	SnapshotValid bool
	Snapshot      []byte
	SnapshotTerm  int
	SnapshotIndex int
}

type LogEntry struct {
	Command interface{} // command for state machine
	Term    int         // term when entry was received by leader (first index is 1)
}

// A Go object implementing a single Raft peer.
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	// Your data here (3A, 3B, 3C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

	// Persistent State
	currentTerm int        // latest term server has seen (initialized to 0 on first boot, increases monotonically)
	votedFor    int        // candidateId that received vote in current term (or null if none)
	log         []LogEntry // log entries; each entry contains command for state machine, and term when entry was received by leader (first index is 1)

	// Volatile State on All Servers
	commitIndex int // index of highest log entry known to be committed (initialized to 0, increases monotonically)
	lastApplied int // index of highest log entry applied to state machine (initialized to 0, increases monotonically)

	// Volatile State on Leaders

	// for each server, index of the next log entry to send to that server (initialized to leader last log index + 1)
	nextIndex []int

	// for each server, index of highest log entry known to be replicated on server (initialized to 0, increases monotonically)
	matchIndex []int

	// Election state
	lastContact     time.Time
	electionTimeout time.Duration
	votesGranted    int

	// Heartbeat
	lastHeartbeat time.Time
	heartbeat     time.Duration

	// State
	state state

	// Channels
	applyCh chan ApplyMsg
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var term int
	var isleader bool
	// Your code here (3A).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	term = rf.currentTerm
	isleader = rf.State() == Leader
	return term, isleader
}

// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
// before you've implemented snapshots, you should pass nil as the
// second argument to persister.Save().
// after you've implemented snapshots, pass the current snapshot
// (or nil if there's not yet a snapshot).
func (rf *Raft) persist() {
	// Your code here (3C).
	// Example:
	// w := new(bytes.Buffer)
	// e := labgob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// raftstate := w.Bytes()
	// rf.persister.Save(raftstate, nil)
}

// restore previously persisted state.
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (3C).
	// Example:
	// r := bytes.NewBuffer(data)
	// d := labgob.NewDecoder(r)
	// var xxx
	// var yyy
	// if d.Decode(&xxx) != nil ||
	//    d.Decode(&yyy) != nil {
	//   error...
	// } else {
	//   rf.xxx = xxx
	//   rf.yyy = yyy
	// }
}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (3D).

}

// example RequestVote RPC arguments structure.
// field names must start with capital letters!
type RequestVoteArgs struct {
	// Your data here (3A, 3B).
	Term         int // candidate’s term
	CandidateId  int // candidate requesting vote
	LastLogIndex int // index of candidate’s last log entry (§5.4)
	LastLogTerm  int // term of candidate’s last log entry (§5.4)
}

// example RequestVote RPC reply structure.
// field names must start with capital letters!
type RequestVoteReply struct {
	// Your data here (3A).
	Term        int  // currentTerm, for candidate to update itself
	VoteGranted bool // true means candidate received vote
}

type AppendEntries struct {
	Term         int        // leader’s term
	LeaderId     int        // so follower can redirect clients
	PrevLogIndex int        // index of log entry immediately preceding new ones
	PrevLogTerm  int        // term of PrevLogIndex entry
	Entries      []LogEntry // log entries to store (empty for heartbeat; may send more than one for efficiency)
	LeaderCommit int        // leader’s commitIndex

}

type AppendEntriesReply struct {
	Term          int  // currentTerm, for leader to update itself
	Success       bool // true if follower contained entry matching prevLogIndex and prevLogTerm
	ConflictTerm  int  // term of conflicting entry (for optimization)
	ConflictIndex int  // index of first entry in log that conflicts with new entries (for optimization)
}

// example RequestVote RPC handler.
// This is called to handle the RequestVote RPC we get from other peers
// This runs on a separate goroutine that is used to send the RPC and receive the reply. After this function builds the
// reply, it is sent it back to the main goroutine using the voteChan. We use the mutex when we mutate the shared state
// of the Raft server.
func (rf *Raft) RequestVote(vote *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (3A, 3B).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	DPrintf("Server %d: RequestVote RPC received from server %d, votedFor: %d, term: %d", rf.me, vote.CandidateId, rf.votedFor, rf.currentTerm)

	// Initialize reply
	reply.Term = rf.currentTerm
	reply.VoteGranted = false

	// Reset the election timeout because we have received a RequestVote RPC
	//rf.lastContact = time.Now()

	if vote.Term > rf.currentTerm {
		//DPrintf("Server %d: RequestVote RPC received with term %d > currentTerm %d. Updating my term", rf.me, args.Term, rf.currentTerm)
		DPrintf("Server %d: RequestVote RPC received with term %d > currentTerm %d. Updating my term", rf.me, vote.Term, rf.currentTerm)

		rf.currentTerm = vote.Term
		rf.votedFor = NobodyID
		rf.persist()
		rf.setState(Follower)
	}

	// Reply false if term < currentTerm
	if vote.Term < rf.currentTerm {
		DPrintf("Server %d: RequestVote RPC reply sent to server %d. Term %d < currentTerm %d", rf.me, vote.CandidateId, vote.Term, rf.currentTerm)
		return
	}

	// If votedFor is null or candidateId, and candidate’s log is at least as up-to-date as receiver’s log, grant vote
	if rf.votedFor == NobodyID || rf.votedFor == vote.CandidateId {

		// Check if candidate's log is at least as up-to-date as receiver's log
		lastLogIndex := len(rf.log)
		var lastLogTerm int
		if lastLogIndex > 0 {
			lastLogTerm = rf.log[lastLogIndex-1].Term
		}

		if vote.LastLogTerm > lastLogTerm || (vote.LastLogTerm == lastLogTerm && vote.LastLogIndex >= lastLogIndex) {
			// Reset the election timeout because we have granted a vote
			rf.lastContact = time.Now()
			// Check if candidate's log is at least as up-to-date as receiver's log
			// if args.LastLogTerm > rf.lastLogTerm() || (args.LastLogTerm == rf.lastLogTerm() && args.LastLogIndex >= rf.lastLogIndex()) {
			reply.VoteGranted = true
			rf.votedFor = vote.CandidateId
			rf.persist()
			DPrintf("Server %d: Vote granted to server %d", rf.me, vote.CandidateId)
		} else {
			DPrintf("Server %d: RequestVote RPC reply sent to server %d. Candidate's log is not up-to-date", rf.me, vote.CandidateId)
		}
	}
}

func (rf *Raft) AppendEntries(leader *AppendEntries, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	// Initialize reply
	reply.Term = rf.currentTerm
	reply.Success = false

	// If term in the request > currentTerm, update currentTerm and convert to follower
	if leader.Term > rf.currentTerm {
		DPrintf("Server %d: AppendEntries RPC received with term %d > currentTerm %d. Updating term and becoming follower",
			rf.me, leader.Term, rf.currentTerm)
		rf.currentTerm = leader.Term
		rf.votedFor = NobodyID
		rf.persist()
		rf.setState(Follower)
	}

	// 1. Reply false if term < currentTerm
	if leader.Term < rf.currentTerm {
		return
	}

	// Reset election timeout since we've heard from leader
	rf.lastContact = time.Now()

	// 2. Reply false if log doesn't contain an entry at prevLogIndex whose term matches prevLogTerm
	if leader.PrevLogIndex > 0 {
		if len(rf.log) < leader.PrevLogIndex {
			// Log too short
			reply.ConflictTerm = -1
			reply.ConflictIndex = len(rf.log)
			return
		}

		if rf.log[leader.PrevLogIndex-1].Term != leader.PrevLogTerm {
			// Term mismatch
			reply.ConflictTerm = rf.log[leader.PrevLogIndex-1].Term

			// Find first index of the conflicting term
			reply.ConflictIndex = leader.PrevLogIndex
			for i := leader.PrevLogIndex - 2; i >= 0; i-- {
				if rf.log[i].Term != reply.ConflictTerm {
					reply.ConflictIndex = i + 1
					break
				}
			}
			return
		}
	}

	// 3. If an existing entry conflicts with a new one, delete the existing entry and all that follow it
	newLogIndex := leader.PrevLogIndex
	for _, entry := range leader.Entries {
		newLogIndex++

		if newLogIndex <= len(rf.log) {
			// Check for conflict
			if rf.log[newLogIndex-1].Term != entry.Term {
				// Conflict found, truncate log from here
				rf.log = rf.log[:newLogIndex-1]
				break
			}
		} else {
			// We've reached the end of the existing log
			break
		}
	}

	// 4. Append any new entries not already in the log
	for i, entry := range leader.Entries {
		logIndex := leader.PrevLogIndex + i + 1
		if logIndex > len(rf.log) {
			newEntry := LogEntry{
				Command: entry.Command,
				Term:    entry.Term,
			}
			rf.log = append(rf.log, newEntry)
		}
	}
	rf.persist()

	// 5. Update commitIndex if needed
	if leader.LeaderCommit > rf.commitIndex {
		rf.commitIndex = min(leader.LeaderCommit, len(rf.log))
		rf.applyCommittedEntries()
	}

	reply.Success = true
}

func (rf *Raft) debugPrintLog() {
	if !Debug {
		return
	}
	str := fmt.Sprintf("Server %d: Log entries: [", rf.me)
	for i, entry := range rf.log {
		str += fmt.Sprintf("%d:%v ", i+1, entry.Command)
	}
	str += "]"
	str += fmt.Sprintf(" commitIndex: %d, lastApplied: %d, term: %d", rf.commitIndex, rf.lastApplied, rf.currentTerm)
	DPrintf(str)
}

// Apply the log entries up to the commitIndex to the state machine
// Only call this function when the lock is held
func (rf *Raft) applyCommittedEntries() {
	for rf.lastApplied < rf.commitIndex {
		rf.lastApplied++
		entry := rf.log[rf.lastApplied-1] // log index starts at 1
		// Apply the log entry to the state machine
		rf.applyCh <- ApplyMsg{
			CommandValid: true,
			Command:      entry.Command,
			CommandIndex: rf.lastApplied,
		}
		DPrintf("Leader %d: Applied log entry %v at index %d to state machine", rf.me, entry.Command, rf.lastApplied)
	}
}

// Define the min function
func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}

func truncateLog(log []LogEntry, index int) []LogEntry {
	if index < 0 || index > len(log) {
		return log // Index out of bounds, return the original slice
	}
	return log[:index] // Keep entries up to the specified index (exclusive)
}

// Helper functions to get the last log term and index
//func (rf *Raft) lastLogTerm() int {
//	if len(rf.log) == 0 {
//		return 0
//	}
//	sliceIndex := len(rf.log) - 1
//	if sliceIndex < 0 || sliceIndex >= len(rf.log) {
//		DPrintf("Server %d: Index %d out of bounds, sliceIndex: %d", rf.me, len(rf.log), sliceIndex)
//		return 0
//	}
//	LogEntry := rf.log[sliceIndex]
//	return LogEntry.Term
//}

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
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntries, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

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
	isLeader := true

	// Your code here (3B).
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if rf.state != Leader {
		return -1, rf.currentTerm, false
	}

	// Calculate the new entry's index based on the current log length.
	index := len(rf.log) + 1
	term := rf.currentTerm

	logent := LogEntry{
		Command: command,
		Term:    term,
	}
	// Append the log entry to the log
	rf.log = append(rf.log, logent)
	rf.persist()

	// Update the nextIndex and matchIndex for the leader
	rf.matchIndex[rf.me] = len(rf.log)
	rf.nextIndex[rf.me] = len(rf.log) + 1

	DPrintf("Server %d: Command %v appended to log at index %d", rf.me, command, index)

	// Trigger replication to followers (implement this separately)
	go rf.startAgreement()

	return index, term, isLeader
}

// Append the log entry and send AppendEntries RPCs to all other servers to replicate the log.
// When a majority of servers have appended the log entry, the leader can commit the log entry and apply it to the state machine.
// The leader's next heartbeat will include the commitIndex, and the followers will apply the log entries up to the commitIndex to their state machines.
func (rf *Raft) startAgreement() {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	// Send AppendEntries RPCs to all other servers to replicate the log
	for idx := range rf.peers {
		if idx == rf.me {
			continue // don't send AppendEntries RPC to self
		}

		go func(i int) {
			rf.appendEntriesAndHandleResponse(i)
		}(idx)
	}
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
	// Your code here, if desired.
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

func (rf *Raft) setState(state state) {
	rf.state = state
}

func (rf *Raft) State() state {
	return rf.state
}

func (rf *Raft) ticker() {
	// make a waitgroup to wait for all goroutines to finish
	wg := sync.WaitGroup{}

	for !rf.killed() {
		// Your code here (3A)
		// Check if a leader election should be started.

		//DPrintf("Server %d: locking mutex 1", rf.me)
		rf.mu.Lock()
		lastContact := rf.lastContact
		electionTimeout := rf.electionTimeout
		currentState := rf.State()
		lastHeartbeat := rf.lastHeartbeat
		heartbeat := rf.heartbeat
		rf.mu.Unlock()

		if currentState == Leader {
			if time.Since(lastHeartbeat) >= heartbeat {
				rf.mu.Lock()
				rf.lastHeartbeat = time.Now()
				rf.mu.Unlock()
				DPrintf("Leader %d: Sending heartbeats to peers, commitIndex: %d", rf.me, rf.commitIndex)

				// send heartbeats to all peers
				for idx := range rf.peers {
					if idx == rf.me {
						continue // don't send AppendEntries RPC to self
					}

					wg.Add(1)
					peerIdx := idx
					go func() {
						defer wg.Done()
						rf.appendEntriesAndHandleResponse(peerIdx)
					}()
				}
			}
		}

		// if election timeout elapses without receiving AppendEntries RPC from current leader or granting vote to candidate: convert to candidate
		// check if it has been too long since we last heard from the leader or since we last voted for a leader
		// if so, start election by sending a RequestVote RPC to all other serversz
		if time.Since(lastContact) >= electionTimeout && (currentState != Leader) {
			rf.mu.Lock()
			rf.votesGranted = 0
			rf.setState(Candidate)
			rf.votedFor = NobodyID

			DPrintf("Server %d: ELECTION STARTED. Sending RequestVote RPC to peers", rf.me)

			// reset the election timeout to now + sometime + random jitter
			rf.electionTimeout = electionTimeoutMin*time.Millisecond + time.Duration(rand.Int63()%electionTimeoutVar)*time.Millisecond // between 1.5 and 2 seconds
			DPrintf("Server %d: Election timeout reset to %v", rf.me, rf.electionTimeout)

			// On conversion to candidate, start election:

			// • Increment currentTerm
			DPrintf("Server %d: Incrementing currentTerm from %d to %d", rf.me, rf.currentTerm, rf.currentTerm+1)
			rf.currentTerm++
			// • Vote for self
			rf.votesGranted++
			rf.votedFor = rf.me
			rf.persist()
			rf.mu.Unlock()

			// • Reset election timer (If we are here it means the election timer has already elapsed without receiving
			//   a message from the leader or a vote request from another candidate. A new election timer has already
			//   been started in the electionTimeout goroutine)

			// • Send RequestVote RPCs to all other servers
			for idx := range rf.peers {
				if idx == rf.me {
					continue // don't send RequestVote RPC to self
				}

				wg.Add(1)
				peerIdx := idx
				go func() {
					defer wg.Done()
					rf.requestVoteAndHandleResponse(peerIdx)
				}()
			}
		}

		// pause for a random amount of time between 50 and 350
		// milliseconds.
		ms := 25 + (rand.Int63() % 25)
		time.Sleep(time.Duration(ms) * time.Millisecond)
	}
	wg.Wait() // wait for all goroutines to finish
}

func (rf *Raft) requestVoteAndHandleResponse(peerIdx int) {
	// if RPC response contains term T > currentTerm: set currentTerm = T, convert to follower
	// if RPC response contains term T <= currentTerm && vote is granted: increment vote count
	// if votes received from majority of servers: become leader
	rf.mu.Lock()
	// send RequestVote RPC to peer
	request := &RequestVoteArgs{
		Term:         rf.currentTerm,
		CandidateId:  rf.me,
		LastLogIndex: len(rf.log),
		LastLogTerm:  0,
	}
	if len(rf.log) > 0 {
		request.LastLogTerm = rf.log[len(rf.log)-1].Term
	}
	reply := &RequestVoteReply{}
	rf.mu.Unlock()

	ok := rf.sendRequestVote(peerIdx, request, reply)
	if !ok {
		DPrintf("Server %d: RequestVote RPC to server %d failed", rf.me, peerIdx)
		return
	}

	rf.mu.Lock()
	defer rf.mu.Unlock()
	DPrintf("Server %d: RequestVote RPC reply received from server %d", rf.me, peerIdx)
	if reply.Term > rf.currentTerm {
		DPrintf("Server %d: Received RequestVote RPC reply with term %d > currentTerm %d so updating currentTerm and becoming follower", rf.me, reply.Term, rf.currentTerm)
		rf.currentTerm = reply.Term
		rf.votedFor = NobodyID
		rf.persist()
		rf.setState(Follower)
	} else if reply.VoteGranted {
		rf.votesGranted++
		if rf.votesGranted > len(rf.peers)/2 {
			rf.setState(Leader)
			DPrintf("Server %d: Became leader, votesGranted: %d, totalPeers: %d", rf.me, rf.votesGranted, len(rf.peers))

			// Initialize leader state

			// Initialize nextIndex for each server, initialized to leader last log index + 1
			nextIndex := len(rf.log) + 1
			rf.nextIndex = make([]int, len(rf.peers))
			for idx := range rf.peers {
				rf.nextIndex[idx] = nextIndex
			}
			DPrintf("Server %d: Leader log length: %d, nextIndex: %v", rf.me, len(rf.log), rf.nextIndex)

			// Initialize matchIndex for each server, initialized to 0, increases monotonically
			rf.matchIndex = make([]int, len(rf.peers))
			rf.matchIndex[rf.me] = len(rf.log)
		}
	}
}

func (rf *Raft) appendEntriesAndHandleResponse(peerIdx int) {
	for {
		rf.mu.Lock()

		// Critical check: if we're no longer the leader, stop attempting to append entries
		if rf.State() != Leader || rf.killed() {
			rf.mu.Unlock()
			return
		}

		entries := &AppendEntries{
			Term:         rf.currentTerm,
			LeaderId:     rf.me,
			LeaderCommit: rf.commitIndex,
		}

		// Make sure entries.PrevLogIndex and entries.PrevLogTerm are set correctly
		// Start with what the follower might have
		prevLogIndex := rf.nextIndex[peerIdx] - 1
		var prevLogTerm int
		if prevLogIndex > 0 && prevLogIndex <= len(rf.log) {
			prevLogTerm = rf.log[prevLogIndex-1].Term
		}

		entries.PrevLogIndex = prevLogIndex
		entries.PrevLogTerm = prevLogTerm

		// Create a deep copy of the entries by making a new slice and copying each element
		entriesCopy := make([]LogEntry, len(rf.log)-prevLogIndex)
		copy(entriesCopy, rf.log[prevLogIndex:])
		entries.Entries = entriesCopy

		request := entries
		reply := &AppendEntriesReply{}

		if len(entries.Entries) > 0 {
			rf.debugPrintLog()
			str := fmt.Sprintf("Leader %d: Sending AppendEntries to server %d, entries: [", rf.me, peerIdx)
			i := rf.nextIndex[peerIdx]
			for _, entry := range entries.Entries {
				str += fmt.Sprintf("%d:%v ", i, entry.Command)
				i++
			}
			str += "]"
			str += fmt.Sprintf(" PREVLOGINDEX: %d, PREVLOGTERM: %d, AE TERM: %d, CURRENT TERM: %d",
				entries.PrevLogIndex, entries.PrevLogTerm, entries.Term, rf.currentTerm)
			DPrintf(str)
		}

		// Important: store the current term to check later if we're still valid
		currentTerm := rf.currentTerm
		rf.mu.Unlock()

		ok := rf.sendAppendEntries(peerIdx, request, reply)
		if !ok {
			if len(entries.Entries) > 0 {
				DPrintf("Leader %d: AppendEntries RPC to server %d failed. Entries %d", rf.me, peerIdx, len(entries.Entries))
				// Retry after sleeping for a very short time with some jitter
				jitter := time.Duration(rand.Int63()%5) * time.Millisecond
				sleep := 5*time.Millisecond + jitter
				time.Sleep(sleep)
				continue // Use continue instead of goto
			}
			DPrintf("Leader %d: Hearbeat AE to server %d failed. Entries %d", rf.me, peerIdx, len(entries.Entries))
			return
		}

		rf.mu.Lock()

		// If we're no longer the leader or our term has changed, stop
		if rf.State() != Leader || rf.currentTerm != currentTerm {
			rf.mu.Unlock()
			return
		}

		DPrintf("Leader %d: AppendEntries RPC reply received from server %d. entriesTerm: %d, myTerm: %d peerTerm %d",
			rf.me, peerIdx, entries.Term, rf.currentTerm, reply.Term)

		if reply.Term > rf.currentTerm {
			// become follower
			DPrintf("Leader %d: Became follower because we got a reply with a new term. Our term: %d, Response term: %d",
				rf.me, rf.currentTerm, reply.Term)
			rf.votedFor = NobodyID
			rf.currentTerm = reply.Term // update currentTerm
			rf.persist()
			rf.setState(Follower)
			rf.mu.Unlock()
			return
		}

		// If successful: update nextIndex and matchIndex for follower
		if reply.Success {
			if len(entries.Entries) > 0 {
				rf.nextIndex[peerIdx] = entries.PrevLogIndex + len(entries.Entries) + 1
				rf.matchIndex[peerIdx] = entries.PrevLogIndex + len(entries.Entries)
			}

			// Process any newly committed entries
			rf.processNewlyCommittedEntries()
			rf.mu.Unlock()
			return // We're done on success
		} else {
			// If AppendEntries fails because of log inconsistency: decrement nextIndex and retry
			DPrintf("Leader %d: AppendEntries RPC to server %d failed bc of log inconsistency. Next index %d -> %d",
				rf.me, peerIdx, rf.nextIndex[peerIdx], rf.nextIndex[peerIdx]-1)

			if reply.ConflictTerm == -1 {
				// Follower's log is too short
				rf.nextIndex[peerIdx] = reply.ConflictIndex + 1
			} else {
				// Try to find the conflicting term in our log
				conflictTermIndex := -1
				for i := len(rf.log) - 1; i >= 0; i-- {
					if rf.log[i].Term == reply.ConflictTerm {
						conflictTermIndex = i + 1
						break
					}
				}

				if conflictTermIndex != -1 {
					// Found the term, jump to after the last entry of that term
					rf.nextIndex[peerIdx] = conflictTermIndex + 1
				} else {
					// Couldn't find the term, jump to follower's first index of the term
					rf.nextIndex[peerIdx] = reply.ConflictIndex
				}
			}

			rf.mu.Unlock()

			// Retry after sleeping for a very short time with some jitter
			jitter := time.Duration(rand.Int63()%5) * time.Millisecond
			sleep := 5*time.Millisecond + jitter
			time.Sleep(sleep)
			// Loop will continue
		}
	}
}

// Extract the commit logic to a separate function for clarity
func (rf *Raft) processNewlyCommittedEntries() {
	// Here we want to check if we can commit any log entries.
	// If there exists an N such that N > commitIndex, a majority of matchIndex[i] ≥ N, and log[N].term == currentTerm:
	// set commitIndex = N (§5.3)
	for N := len(rf.log); N > 0 && N > rf.commitIndex; N-- {
		DPrintf("Leader %d: Checking commit for N=%d, commitIndex=%d, log length=%d", rf.me, N, rf.commitIndex, len(rf.log))
		if rf.log[N-1].Term == rf.currentTerm {
			DPrintf("Leader %d: Entry at N=%d has matching term %d", rf.me, N, rf.currentTerm)
			matched := 1
			for idx := range rf.peers {
				if idx == rf.me {
					continue
				}
				DPrintf("Leader %d: Checking peer %d: matchIndex=%d, N=%d", rf.me, idx, rf.matchIndex[idx], N)
				if rf.matchIndex[idx] >= N {
					matched++
				}
			}
			DPrintf("Leader %d: For N=%d: matched=%d, needed=%d", rf.me, N, matched, len(rf.peers)/2+1)
			if matched > len(rf.peers)/2 {
				rf.commitIndex = N
				DPrintf("Leader %d: CommitIndex set to %d", rf.me, rf.commitIndex)

				// Apply committed entries to state machine
				rf.applyCommittedEntries()
				break
			}
		} else {
			DPrintf("Leader %d: Entry at N=%d has term %d != currentTerm %d", rf.me, N, rf.log[N-1].Term, rf.currentTerm)
		}
	}
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
func Make(peers []*labrpc.ClientEnd, me int, persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me
	rf.applyCh = applyCh

	// Your initialization code here (3A, 3B, 3C).
	rf.setState(Follower)
	DPrintf("Server %d: Started as follower", rf.me)

	rf.votedFor = NobodyID
	rf.lastContact = time.Now()
	rf.electionTimeout = electionTimeoutMin*time.Millisecond + time.Duration(rand.Int63()%electionTimeoutVar)*time.Millisecond // between 1.5 and 2 seconds
	DPrintf("Server %d: Election timeout set to %v", rf.me, rf.electionTimeout)
	rf.heartbeat = 100 * time.Millisecond // 100 milliseconds which is 10 heartbeats per second

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	go rf.ticker()

	return rf
}
