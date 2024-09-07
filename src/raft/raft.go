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
	"log"
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
	command []byte // command for state machine
	term    int    // term when entry was received by leader (first index is 1)
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
	nextIndex  []int // for each server, index of the next log entry to send to that server (initialized to leader last log index + 1)
	matchIndex []int // for each server, index of highest log entry known to be replicated on server (initialized to 0, increases monotonically)

	// Election state
	lastContact     time.Time
	electionTimeout time.Duration

	// Heartbeat
	lastHeartbeat time.Time
	heartbeat     time.Duration

	// State
	state state
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var term int
	var isleader bool
	// Your code here (3A).
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
	Term         int // leader’s term
	LeaderId     int // so follower can redirect clients
	PrevLogIndex int // index of log entry immediately preceding new ones
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
	log.Printf("Server %d: RequestVote RPC received from server %d, votedFor: %d, term: %d", rf.me, args.CandidateId, rf.votedFor, rf.currentTerm)
	// Your code here (3A, 3B).
	rf.mu.Lock()
	defer rf.mu.Unlock()

	// Initialize reply
	reply.Term = rf.currentTerm
	reply.VoteGranted = false

	// Reset the election timeout because we have received a RequestVote RPC
	rf.lastContact = time.Now()

	// Reply false if term < currentTerm
	if args.Term < rf.currentTerm {
		log.Printf("Server %d: RequestVote RPC reply sent to server %d. Term %d < currentTerm %d", rf.me, args.CandidateId, args.Term, rf.currentTerm)
		return
	}

	// If votedFor is null or candidateId, and candidate’s log is at least as up-to-date as receiver’s log, grant vote
	if rf.votedFor == NobodyID || rf.votedFor == args.CandidateId {
		// Check if candidate's log is at least as up-to-date as receiver's log
		//if args.LastLogTerm > rf.lastLogTerm() || (args.LastLogTerm == rf.lastLogTerm() && args.LastLogIndex >= rf.lastLogIndex()) {
		reply.VoteGranted = true
		rf.votedFor = args.CandidateId
		rf.persist()
		log.Printf("Server %d: Vote granted to server %d", rf.me, args.CandidateId)
		//}
	}
}

func (rf *Raft) AppendEntries(args *AppendEntries, reply *AppendEntriesReply) {
	log.Printf("Server %d: AppendEntries RPC received from server %d, term: %d, state: %d", rf.me, args.LeaderId, args.Term, rf.State())
	// Your code here (3A, 3B).
	rf.mu.Lock()
	defer rf.mu.Unlock()

	// Initialize reply
	reply.Term = rf.currentTerm
	reply.Success = false

	// Reset the election timeout because we have received an AppendEntries RPC
	rf.lastContact = time.Now()
	//if rf.State() == Candidate {
	rf.setState(Follower)
	log.Printf("Server %d: Recieved AppendEntries and became follower", rf.me)
	//}

	// Reply false if term < currentTerm
	if args.Term < rf.currentTerm {
		log.Printf("Server %d: AppendEntries RPC reply sent to server %d. Term %d < currentTerm %d", rf.me, args.LeaderId, args.Term, rf.currentTerm)
		return
	}
}

// Helper functions to get the last log term and index
func (rf *Raft) lastLogTerm() int {
	if len(rf.log) == 0 {
		return 0
	}
	return rf.log[len(rf.log)-1].term
}

func (rf *Raft) lastLogIndex() int {
	if len(rf.log) == 0 {
		return -1
	}
	return len(rf.log) - 1
}

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

	return index, term, isLeader
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
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.state
}

func (rf *Raft) ticker() {
	voteChan := make(chan *RequestVoteReply)
	defer close(voteChan)

	appendChan := make(chan *AppendEntriesReply)
	defer close(appendChan)

	votesGranted := 0

	rf.electionTimeout = 1500*time.Millisecond + time.Duration(rand.Int63()%500)*time.Millisecond // between 1.5 and 2 seconds
	log.Printf("Server %d: Election timeout set to %v", rf.me, rf.electionTimeout)

	rf.heartbeat = 100 * time.Millisecond // 100 milliseconds which is 10 heartbeats per second

	for !rf.killed() {
		// Your code here (3A)
		// Check if a leader election should be started.

		// if election timeout elapses without receiving AppendEntries RPC from current leader or granting vote to candidate: convert to candidate
		// check if it has been too long since we last heard from the leader or since we last voted for a leader
		// if so, start election by sending a RequestVote RPC to all other servers
		if time.Since(rf.lastContact) >= rf.electionTimeout && (rf.State() == Follower || rf.State() == Candidate) {
			rf.setState(Candidate)
			rf.votedFor = NobodyID
			votesGranted = 0

			log.Printf("Server %d: Election started. Sending RequestVote RPC to peers", rf.me)

			// reset the election timeout to now + sometime + random jitter
			rf.electionTimeout = 1500*time.Millisecond + time.Duration(rand.Int63()%500)*time.Millisecond // between 1.5 and 2 seconds
			log.Printf("Server %d: Election timeout reset to %v", rf.me, rf.electionTimeout)

			// On conversion to candidate, start election:

			// • Increment currentTerm
			rf.currentTerm++
			// • Vote for self
			//rf.votedFor = rf.me
			votesGranted++
			rf.persist()

			// • Reset election timer (If we are here it means the election timer has already elapsed without receiving
			//   a message from the leader or a vote request from another candidate. A new election timer has already
			//   been started in the electionTimeout goroutine)

			// • Send RequestVote RPCs to all other servers
			for idx := range rf.peers {
				if idx == rf.me {
					continue // don't send RequestVote RPC to self
				}
				// send RequestVote RPC to peer
				args := &RequestVoteArgs{
					Term:        rf.currentTerm,
					CandidateId: rf.me,
				}
				reply := &RequestVoteReply{}

				go func(idx int, args *RequestVoteArgs, reply *RequestVoteReply, replyChan chan *RequestVoteReply) {
					gotreply := rf.sendRequestVote(idx, args, reply)
					if gotreply {
						log.Printf("Server %d: RequestVote RPC reply received from server %d", rf.me, idx)
						voteChan <- reply
					} else {
						log.Printf("Server %d: RequestVote RPC to server %d failed", rf.me, idx)
					}
				}(idx, args, reply, voteChan)
			}
		}

		// read from channels
		if rf.State() == Candidate {
			select {
			case voteReply := <-voteChan:
				// handle reply
				if voteReply.Term > rf.currentTerm {
					// if RPC response contains term T > currentTerm: set currentTerm = T, convert to follower
					rf.currentTerm = voteReply.Term
					rf.votedFor = NobodyID
					rf.persist()
					rf.setState(Follower)
				} else {
					// if RPC response contains term T < currentTerm: ignore
					// if RPC response contains term T = currentTerm: count vote
					if voteReply.VoteGranted {
						// count vote
						votesGranted++

						// if votes received from majority of servers: become leader
						if votesGranted > len(rf.peers)/2 {
							rf.setState(Leader)
							log.Printf("Server %d: Became leader, votesGranted: %d, totalPeers: %d", rf.me, votesGranted, len(rf.peers))
						}
					}
				}
			default:
				// log something and continue
				log.Printf("Server %d: No votes received yet", rf.me)
			}
		}

		if rf.State() == Leader {

			if time.Since(rf.lastHeartbeat) >= rf.heartbeat {

				rf.lastHeartbeat = time.Now()
				log.Printf("Server %d: Sending heartbeats to peers", rf.me)

				// send heartbeats to all peers
				for idx := range rf.peers {
					if idx == rf.me {
						continue // don't send AppendEntries RPC to self
					}
					// send AppendEntries RPC to peer
					args := &AppendEntries{
						LeaderId: rf.me,
					}
					reply := &AppendEntriesReply{}
					go func(idx int, args *AppendEntries, reply *AppendEntriesReply, appendChan chan *AppendEntriesReply) {
						gotreply := rf.sendAppendEntries(idx, args, reply)
						if gotreply {
							log.Printf("Server %d: AppendEntries RPC reply received from server %d", rf.me, idx)
							appendChan <- reply
						} else {
							log.Printf("Server %d: AppendEntries RPC to server %d failed", rf.me, idx)
						}
					}(idx, args, reply, appendChan)
				}
			}

			select {
			case appendReply := <-appendChan:
				// handle reply
				if appendReply.Term > rf.currentTerm {
					// become a follower
					rf.currentTerm = appendReply.Term
					rf.votedFor = NobodyID
					rf.persist()
					rf.setState(Follower)
					log.Printf("Server %d: Became follower", rf.me)
				}
			default:
				// log something and continue
				//log.Printf("Server %d: No AppendEntries replies received yet", rf.me)
			}
		}

		// pause for a random amount of time between 50 and 350
		// milliseconds.
		ms := 50 + (rand.Int63() % 300)
		time.Sleep(time.Duration(ms) * time.Millisecond)
	}
}

//func (rf *Raft) electionTimeout() {
//	// sleeps for the election timeout duration
//	// when we wake up, we check if we have heard from the leader via AppendEntries RPC or if we have granted our vote
//	// to a candidate, if not, we start an election
//	for !rf.killed() {
//		log.Printf("Server %d: electionTimeout duration started", rf.me)
//		// sleep for the electionTimeout duration. Add some random jitter to prevent split votes
//		// Because the tester limits you tens of heartbeats per second, you will have to use an election timeout larger
//		// than the paper's 150 to 300 milliseconds, but not too large, because then you may fail to elect a leader
//		// within five seconds.
//		ms := 1500 + (rand.Int63() % 500) // between 1.5 and 2 seconds
//		electionTimeout := time.Duration(ms) * time.Millisecond
//		time.Sleep(electionTimeout)
//
//		// check if the election timeout has elapsed without receiving AppendEntries RPC from current leader or granting vote to candidate
//		// if so, convert to candidate
//		rf.mu.Lock()
//		if !rf.receivedAppendEntries && rf.votedFor == nil {
//			rf.electionShouldStart = true
//			log.Printf("Server %d: Starting election", rf.me)
//		}
//		// regardless of if an election should be started, reset the receivedAppendEntries flag
//		// so we can check it again in the next electionTimeout. We don't reset votedFor here because
//		// we want to remember who we voted for in the last election until a new term starts.
//		rf.receivedAppendEntries = false
//		rf.mu.Unlock()
//	}
//}

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

	// Your initialization code here (3A, 3B, 3C).

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start election timeout
	//go rf.electionTimeout()

	rf.setState(Follower)
	log.Printf("Server %d: Started as follower", rf.me)

	// start ticker goroutine to start elections
	go rf.ticker()

	return rf
}
