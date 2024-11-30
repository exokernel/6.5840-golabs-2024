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
const electionTimeoutMin = 800
const electionTimeoutVar = 400 // election timeout jitter, we will add a random number of milliseconds between 0 and electionTimeoutVar to the election timeout

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

type logEntry struct {
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
	currentTerm int         // latest term server has seen (initialized to 0 on first boot, increases monotonically)
	votedFor    int         // candidateId that received vote in current term (or null if none)
	log         []*logEntry // log entries; each entry contains command for state machine, and term when entry was received by leader (first index is 1)

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
	Term         int         // leader’s term
	LeaderId     int         // so follower can redirect clients
	PrevLogIndex int         // index of log entry immediately preceding new ones
	PrevLogTerm  int         // term of PrevLogIndex entry
	Entries      []*logEntry // log entries to store (empty for heartbeat; may send more than one for efficiency)
	LeaderCommit int         // leader’s commitIndex

}

type AppendEntriesReply struct {
	Term    int  // currentTerm, for leader to update itself
	Success bool // true if follower contained entry matching prevLogIndex and prevLogTerm
}

// example RequestVote RPC handler.
// This is called to handle the RequestVote RPC we get from other peers
// This runs on a separate goroutine that is used to send the RPC and receive the reply. After this function builds the
// reply, it is sent it back to the main goroutine using the voteChan. We use the mutex when we mutate the shared state
// of the Raft server.
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (3A, 3B).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	DPrintf("Server %d: RequestVote RPC received from server %d, votedFor: %d, term: %d", rf.me, args.CandidateId, rf.votedFor, rf.currentTerm)

	// Initialize reply
	reply.Term = rf.currentTerm
	reply.VoteGranted = false

	// Reset the election timeout because we have received a RequestVote RPC
	//rf.lastContact = time.Now()

	if args.Term > rf.currentTerm {
		//DPrintf("Server %d: RequestVote RPC received with term %d > currentTerm %d. Updating my term", rf.me, args.Term, rf.currentTerm)
		DPrintf("Server %d: RequestVote RPC received with term %d > currentTerm %d. Updating my term", rf.me, args.Term, rf.currentTerm)

		rf.currentTerm = args.Term
		rf.votedFor = NobodyID
		rf.persist()
		rf.setState(Follower)
	}

	// Reply false if term < currentTerm
	if args.Term < rf.currentTerm {
		DPrintf("Server %d: RequestVote RPC reply sent to server %d. Term %d < currentTerm %d", rf.me, args.CandidateId, args.Term, rf.currentTerm)
		return
	}

	// If votedFor is null or candidateId, and candidate’s log is at least as up-to-date as receiver’s log, grant vote
	if rf.votedFor == NobodyID || rf.votedFor == args.CandidateId {

		// Check if candidate's log is at least as up-to-date as receiver's log
		lastLogIndex := len(rf.log)
		var lastLogTerm int
		if lastLogIndex > 0 {
			lastLogTerm = rf.log[lastLogIndex-1].Term
		}

		if args.LastLogTerm > lastLogTerm || (args.LastLogTerm == lastLogTerm && args.LastLogIndex >= lastLogIndex) {
			// Reset the election timeout because we have granted a vote
			rf.lastContact = time.Now()
			// Check if candidate's log is at least as up-to-date as receiver's log
			// if args.LastLogTerm > rf.lastLogTerm() || (args.LastLogTerm == rf.lastLogTerm() && args.LastLogIndex >= rf.lastLogIndex()) {
			reply.VoteGranted = true
			rf.votedFor = args.CandidateId
			rf.persist()
			DPrintf("Server %d: Vote granted to server %d", rf.me, args.CandidateId)
		} else {
			DPrintf("Server %d: RequestVote RPC reply sent to server %d. Candidate's log is not up-to-date", rf.me, args.CandidateId)
		}
	}
}

func (rf *Raft) AppendEntries(args *AppendEntries, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	// Your code here (3A, 3B).
	DPrintf("Server %d: AppendEntries RPC received from server %d, term: %d, state: %d", rf.me, args.LeaderId, args.Term, rf.State())

	// Initialize reply
	reply.Term = rf.currentTerm
	reply.Success = false
	if args.Term > rf.currentTerm {
		DPrintf("Server %d: AppendEntries RPC received with term %d > currentTerm %d. Updating my term", rf.me, args.Term, rf.currentTerm)
		rf.currentTerm = args.Term
		rf.votedFor = NobodyID
		rf.persist()
		if rf.State() != Follower {
			DPrintf("Server %d: Becoming follower: received term %d > currentTerm %d", rf.me, args.Term, rf.currentTerm)
			rf.setState(Follower)
		}
	}

	// Reset the election timeout because we have received an AppendEntries RPC
	rf.lastContact = time.Now()

	switch rf.State() {
	case Follower:
		// already a follower, just log the message
		DPrintf("Server %d: Recieved AppendEntries and already a follower", rf.me)
	case Candidate:
		// switch to follower
		rf.setState(Follower)
		DPrintf("Server %d: Recieved AppendEntries and became follower", rf.me)
	case Leader:
		DPrintf("Server %d: Recieved AppendEntries but I'm a leader. Something is wrong", rf.me)
	}

	// 1. Reply false if term < currentTerm (§5.1)
	if args.Term < rf.currentTerm {
		DPrintf("Server %d: AppendEntries RPC reply sent to server %d. Term %d < currentTerm %d", rf.me, args.LeaderId, args.Term, rf.currentTerm)
		return // Failed AppendEntries
	}

	// Note: Even heartbeats can contain log entries bc they are used to retry failed appends (e.g. follower logs is
	// inconsistent with leader). The leader will have decremented nextIndex for the follower that failed to append.
	if len(args.Entries) > 0 {
		// 2. Reply false if log doesn’t contain an entry at prevLogIndex whose term matches prevLogTerm (§5.3)
		prevLogSliceIndex := args.PrevLogIndex - 1
		if prevLogSliceIndex < 0 || prevLogSliceIndex >= len(rf.log) {
			DPrintf("Server %d: Index %d out of bounds, sliceIndex: %d", rf.me, args.PrevLogIndex, prevLogSliceIndex)
			//panic("Index out of bounds")
			return // Failed AppendEntries
		}
		matchingPrevIndexLogEntry := rf.log[prevLogSliceIndex]
		if matchingPrevIndexLogEntry.Term != args.PrevLogTerm {
			DPrintf("Server %d: AppendEntries RPC reply sent to server %d. Log doesn't contain an entry at prevLogIndex %d whose term matches prevLogTerm %d", rf.me, args.LeaderId, args.PrevLogIndex, args.PrevLogTerm)
			return // Failed AppendEntries
		}

		// 3. If an existing entry conflicts with a new one (same index but different terms), delete the existing entry and all that follow it (§5.3)
		for i := prevLogSliceIndex + 1; i < len(rf.log); i++ {
			// i - prevLogSliceIndex - 1 gives the correct index into args.Entries
			if rf.log[i].Term != args.Entries[i-prevLogSliceIndex-1].Term {
				rf.log = truncateLog(rf.log, i)
				break
			}
		}

		// 4. Append any new entries not already in the log
		for i, entry := range args.Entries {
			logIndex := args.PrevLogIndex + 1 + i
			if logIndex-1 >= len(rf.log) {
				rf.log = append(rf.log, entry)
			}
		}
		rf.persist()
	}

	// 5. If leaderCommit > commitIndex, set commitIndex = min(leaderCommit, index of last new entry) (§5.3)
	// The min() ensures that the follower's commitIndex is updated safely, preventing it committing entries that it
	// hasn't received yet. The leader's commitIndex could be beyond the follower's last log index. In that case, we
	// adjust the commitIndex to the follower's last log index.
	if args.LeaderCommit > rf.commitIndex {
		rf.commitIndex = min(args.LeaderCommit, len(rf.log))
		rf.applyCommittedEntries()
	}

	reply.Term = rf.currentTerm
	reply.Success = true // Successful AppendEntries
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
	}
}

// Define the min function
func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}

func truncateLog(log []*logEntry, index int) []*logEntry {
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
	index := -1
	term := -1
	isLeader := true

	// Your code here (3B).
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if rf.state != Leader {
		return -1, rf.currentTerm, false
	}

	index = len(rf.log) + 1 // first index should be one
	term = rf.currentTerm

	DPrintf("Server %d: Command %v appended to log at index %d", rf.me, command, index)

	// Append to the log and trigger replication to followers (implement this separately)
	go rf.startAgreement(index, command)

	return index, term, isLeader
}

// Append the log entry and send AppendEntries RPCs to all other servers to replicate the log.
// When a majority of servers have appended the log entry, the leader can commit the log entry and apply it to the state machine.
// The leader's next heartbeat will include the commitIndex, and the followers will apply the log entries up to the commitIndex to their state machines.
func (rf *Raft) startAgreement(index int, command interface{}) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	sliceIndex := index - 1
	if sliceIndex < 0 || sliceIndex != len(rf.log) {
		DPrintf("Server %d: Index %d out of bounds, sliceIndex: %d", rf.me, index, sliceIndex)
		return
	}

	logent := &logEntry{
		Command: command,
		Term:    rf.currentTerm,
	}

	// Append the log entry
	rf.log = append(rf.log, logent)

	// Send AppendEntries RPCs to all other servers to replicate the log
	for idx := range rf.peers {
		if idx == rf.me {
			continue // don't send AppendEntries RPC to self
		}

		peerIdx := idx
		if len(rf.log) >= rf.nextIndex[peerIdx] {
			// If last log index ≥ nextIndex for a follower: send AppendEntries RPC with log entries starting at nextIndex
			entries := &AppendEntries{
				Term:         rf.currentTerm,
				LeaderId:     rf.me,
				PrevLogIndex: rf.nextIndex[peerIdx] - 1,
				PrevLogTerm:  rf.log[rf.nextIndex[peerIdx]-1].Term,
				Entries:      rf.log[rf.nextIndex[peerIdx]:],
				LeaderCommit: rf.commitIndex,
			}

			go func() {
				rf.appendEntriesAndHandleResponse(peerIdx, entries)
			}()
		}
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
				DPrintf("Server %d: Sending heartbeats to peers", rf.me)

				// send heartbeats to all peers
				for idx := range rf.peers {
					if idx == rf.me {
						continue // don't send AppendEntries RPC to self
					}
					// send AppendEntries RPC to peer
					heartbeatEnt := &AppendEntries{
						Term:         rf.currentTerm,
						LeaderId:     rf.me,
						LeaderCommit: rf.commitIndex,
					}

					if len(rf.log) >= rf.nextIndex[idx] {
						// If last log index ≥ nextIndex for a follower: send AppendEntries RPC with log entries starting at nextIndex
						// If this happens in the heartbeat, it means the follower is behind and this is a retry
						heartbeatEnt.PrevLogIndex = rf.nextIndex[idx] - 1
						heartbeatEnt.PrevLogTerm = rf.log[rf.nextIndex[idx]-1].Term
						heartbeatEnt.Entries = rf.log[rf.nextIndex[idx]:]
					}

					wg.Add(1)
					peerIdx := idx
					go func() {
						defer wg.Done()
						rf.appendEntriesAndHandleResponse(peerIdx, heartbeatEnt)
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

			DPrintf("Server %d: Election started. Sending RequestVote RPC to peers", rf.me)

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
		ms := 50 + (rand.Int63() % 300)
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

			// Initialize matchIndex for each server, initialized to 0, increases monotonically
			rf.matchIndex = make([]int, len(rf.peers))
		}
	}
}

func (rf *Raft) appendEntriesAndHandleResponse(peerIdx int, entries *AppendEntries) {
	rf.mu.Lock()
	request := entries
	reply := &AppendEntriesReply{}
	rf.mu.Unlock()

	ok := rf.sendAppendEntries(peerIdx, request, reply)
	if !ok {
		DPrintf("Server %d: AppendEntries RPC to server %d failed", rf.me, peerIdx)
		return
	}

	rf.mu.Lock()
	defer rf.mu.Unlock()
	DPrintf("Server %d: AppendEntries RPC reply received from server %d", rf.me, peerIdx)
	if reply.Term > rf.currentTerm {
		// become follower
		rf.votedFor = NobodyID
		rf.currentTerm = reply.Term // update currentTerm
		rf.persist()
		rf.setState(Follower)
		DPrintf("Server %d: Became follower", rf.me)
	}

	// TODO: handle AppendEntries RPC reply
	// If we got enough successful responses we can update the commitIndex, and other things specified in the paper.
	// Do we need to handle heartbeats differently? We know this was a heartbeat if len(entries.Entries) == 0
	// Would it be better to have a separate function for heartbeats to make the code more readable? Probably yes.

	// If this is a heartbeat response, do nothing
	if len(entries.Entries) == 0 {
		return
	}

	// If successful: update nextIndex and matchIndex for follower (§5.3)
	if reply.Success {
		// update nextIndex and matchIndex based on the entries sent
		// Note: raft log indexes start at 1
		// prevLogIndex is the index of the log entry immediately preceding the new ones
		// nextIndex is the index of the next log entry to send to that server
		rf.nextIndex[peerIdx] = entries.PrevLogIndex + len(entries.Entries) + 1
		// matchIndex is the index of the highest log entry known to be replicated on server
		rf.matchIndex[peerIdx] = entries.PrevLogIndex + len(entries.Entries)
	} else {
		// If AppendEntries fails because of log inconsistency: decrement nextIndex and retry (§5.3)
		// TODO: how does the retry happen? Do we need to kick off another goroutine to keep retrying or do we just
		// keep trying as we get more commands from clients?

		// Okay, according to GPT we handle the retries for a follower in subsequent append entries be they heartbeats or
		// due to client commands. This means we have to adjust the code for heartbeats to handle log entries for the
		// specific followers that are behind.

		// If followers crash or run slowly, or if network packets are lost, the leader retries Append-
		// Entries RPCs indefinitely (even after it has responded to the client) until all followers eventually store
		// all log entries.
		rf.nextIndex[peerIdx]--
	}

	// Here we want to check if we can commit any log entries. We can only commit log entries that have been replicated
	// to a majority of servers, including this server (the leader).
	// If there exists an N such that N > commitIndex, a majority of matchIndex[i] ≥ N, and log[N].term == currentTerm:
	// set commitIndex = N (§5.3)
	for N := len(rf.log) - 1; N > rf.commitIndex; N-- {
		if rf.log[N].Term == rf.currentTerm {
			matched := 1
			for idx := range rf.peers {
				if idx == rf.me {
					continue
				}
				if rf.matchIndex[idx] >= N {
					matched++
				}
			}
			if matched > len(rf.peers)/2 {
				rf.commitIndex = N
				DPrintf("Server %d: CommitIndex set to %d", rf.me, rf.commitIndex)

				// When the entry has been safely replicated, the leader applies the entry to its
				// state machine and returns the result.

				// Apply the log entries up to the commitIndex to the state machine.
				for i := rf.lastApplied + 1; i <= rf.commitIndex; i++ {
					applyMsg := ApplyMsg{
						CommandValid: true,
						Command:      rf.log[i].Command,
						CommandIndex: i,
					}
					DPrintf("Server %d: Applying log entry %v at index %d", rf.me, rf.log[i].Command, i)
					rf.lastApplied = i
					rf.applyCh <- applyMsg
				}
				break
			}
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
