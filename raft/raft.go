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
	"runtime"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	//	"Rafting-in-Ganges/labgob"
	"Rafting-in-Ganges/labrpc"
)

func (rf *Raft) debug(args ...any) {
	pc, _, _, ok := runtime.Caller(1)
	details := runtime.FuncForPC(pc)
	if !ok || details == nil {
		fmt.Println("some issue getting the function name")
	}
	callerPath := strings.Split(details.Name(), ".")
	callerName := callerPath[len(callerPath)-1]

	return

	fmt.Printf("[S%d:%s:%d]\t[%s]\t", rf.me, rf.state, rf.currentTerm, callerName)

	for _, arg := range args {
		fmt.Printf("%s\t", arg)
	}
	fmt.Println()
}

const (
	// states
	Follower  = "FOLLOWER"
	Candidate = "CANDIDATE"
	Leader    = "LEADER"

	// timers
	Tick      = 10 * time.Millisecond
	Heartbeat = 100 * time.Millisecond
	Election  = 750 * time.Millisecond
)

// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in part 2D you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh, but set CommandValid to false for these
// other uses.
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int

	// For 2D:
	SnapshotValid bool
	Snapshot      []byte
	SnapshotTerm  int
	SnapshotIndex int
}

type LogEntry struct {
	index   int
	term    int
	command interface{}
}

// A Go object implementing a single Raft peer.
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

	state         string           // one of FOLLOWER, CANDIDATE, LEADER
	internalTimer <-chan time.Time // internal timer. either electionTimer duration or heartbeat duration

	// persistent state
	currentTerm int        // latest term server has seen (initialized to 0 on first boot, increases monotonically)
	votedFor    int        // candidateId that received vote in current term (or null if none)
	log         []LogEntry // log entries; each entry contains command for state machine, and term when entry was received by leader (first index is 1)

	// volatile state
	commitIndex int // index of highest log entry known to be committed (initialized to 0, increases monotonically)
	lastApplied int // index of highest log entry applied to state machine (initialized to 0, increases monotonically)

	// volatile state on leaders
	nextIndex  []int // for each server, index of next log entry to send to that server (initialized to leader log index + 1)
	matchIndex []int // for each server, index of highest log entry known to be replicated on server (initialized to 0, increases monotonically)
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var term int
	var isleader bool
	// Your code here (2A).

	rf.mu.Lock()
	defer rf.mu.Unlock()

	term = rf.currentTerm
	isleader = (rf.state == Leader)

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
	// Your code here (2C).
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
	// Your code here (2C).
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
	// Your code here (2D).

}

// Assumes the server is locked.
// Defaults the server state to a Follower state with
// currentTerm, internalTimer, votedFor reseted.
func (rf *Raft) toFollower(term int) {
	rf.currentTerm = term
	rf.state = Follower
	rf.votedFor = -1
}

type AppendEntriesArgs struct {
	Term         int   // leader's term
	LeaderId     int   // so follower can redirect clients
	PrevLogIndex int   // index of log entry immediately preceding new ones
	PrevLogTerm  int   // term of prevLogIndex entry
	Entries      []int // log entries to store (empty for heartbeat; may send more than one for efficiency)
	LeaderCommit int   // leader's commitIndex
}

type AppendEntriesReply struct {
	Term    int  // currentTerm, for leader to update itself
	Success bool // true if follower contained entry matching prevLogIndex and prevLogTerm
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	// Reply false if term < currentTerm
	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		reply.Success = false
		return
	}

	// Reply false if log doesn't contain an entry at prevLogIndex
	// whose term matches prevLogTerm
	// if len(rf.log) < args.PrevLogIndex || rf.log[args.PrevLogIndex].term != args.PrevLogTerm {
	// 	reply.Term = rf.currentTerm
	// 	reply.Success = false
	// }

	// If an existing entry conflicts with a new one
	// (same index but different terms),
	// delete the existing entry and all that follow it

	// Append any new entries not already in the log

	// If leaderCommit > commitIndex,
	// set commitIndex = min(leaderCommit, index of last new entry)

	// AppendEntry resets the state to Follower,
	// and thus also the internalTimer to the default Election + random time
	// We always update the currentTerm
	// After server updates, we set the appropirate reply.

	rf.toFollower(args.Term)
	rf.debug("Received AE RPC. Converted to follower.")

	reply.Term = rf.currentTerm
	reply.Success = true
	return
}

func (rf *Raft) sendAppendEntries() {
	args := AppendEntriesArgs{
		Term:         rf.currentTerm,
		LeaderId:     rf.me,
		PrevLogIndex: 1,
		PrevLogTerm:  -1,
		// Entries:      nil,
		LeaderCommit: -1,
	}

	for server := range rf.peers {
		if server == rf.me {
			continue
		}

		go func(server int) {
			reply := AppendEntriesReply{}
			rf.peers[server].Call("Raft.AppendEntries", &args, &reply)
			if reply.Term > rf.currentTerm {
				rf.toFollower(reply.Term)
			}
		}(server)
	}
}

// example RequestVote RPC arguments structure.
// field names must start with capital letters!
type RequestVoteArgs struct {
	Term         int // candidate's term
	CandidateId  int // candidate requesting vote
	LastLogIndex int // index of candidate's last log entry
	LastLogTerm  int // term of candidate's last log entry
}

// example RequestVote RPC reply structure.
// field names must start with capital letters!
type RequestVoteReply struct {
	Term        int  // currentTerm, for candidate to update itself
	VoteGranted bool // true means candidate received vote
}

// example RequestVote RPC handler.
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()

	// Reply false if term < currentTerm
	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		reply.VoteGranted = false
		return
	}

	// if you reach here, the term is up to date
	// if its more, then catch up by becoming a Follower
	if args.Term > rf.currentTerm {
		rf.toFollower(args.Term)
	}

	// If votedFor is null or candidateId,
	// and candidate's log is at least as up-to-date as receiver's log
	// grant vote
	// --> by default not granting vote
	rf.debug(rf.currentTerm, args.Term, len(rf.log), args.LastLogIndex)
	if (rf.votedFor == -1 || rf.votedFor == args.CandidateId) &&
		(rf.currentTerm <= args.Term && len(rf.log) <= args.LastLogIndex) {
		reply.Term = rf.currentTerm
		reply.VoteGranted = true
		return
	}

	// default case
	reply.Term = rf.currentTerm
	reply.VoteGranted = false
	return
}

func (rf *Raft) collectVotes(voteCh chan RequestVoteReply) {
	rf.currentTerm++

	args := RequestVoteArgs{
		Term:         rf.currentTerm,
		CandidateId:  rf.me,
		LastLogIndex: 1,
		LastLogTerm:  0,
	}

	for server := range rf.peers {
		if server == rf.me {
			continue
		}

		go func(server int) {
			reply := RequestVoteReply{}
			rf.peers[server].Call("Raft.RequestVote", &args, &reply)
			voteCh <- reply
		}(server)
	}

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
// func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
// 	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
// 	return ok
// }

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

	// Your code here (2B).

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

func (rf *Raft) followerLoop() {
	rf.internalTimer = time.After(Election + time.Duration(rand.Int63())%Election)

	for rf.state == Follower {
		select {
		case <-rf.internalTimer:
			rf.debug("Election timer went off. Starting elections.")
			rf.state = Candidate
		default:
			rf.debug("Sleeping")
			time.Sleep(Tick)
		}
	}
}

func (rf *Raft) candidateLoop() {
	// case 1: spawn a ticker that resets this candidate's existence
	// if enough time passes without substantial results from elections
	rf.internalTimer = time.After(Election + time.Duration(rand.Int63())%Election)

	// case 2: spawn a goroutine that collects votes,
	// and notifies the result on a channel
	// once the RequestVote RPC is sent, and if the electionTimer hits
	// we are pretty much wasting computation from goroutines
	// BUT, we can't quit (using another channel) since if any other server
	// returns some reply late, golang will panic
	voteCh := make(chan RequestVoteReply)
	rf.collectVotes(voteCh)
	votes := 1

	for rf.state == Candidate {
		select {
		case <-rf.internalTimer:
			rf.debug("Election timer went off. Redoing elections.")
			return
		case reply := <-voteCh:
			rf.debug("Got vote reply form someone.", reply.VoteGranted, reply.Term)

			// if vote granted
			if reply.VoteGranted {
				votes++
			}

			// if majority, convert to leader
			if votes > len(rf.peers)/2 {
				rf.state = Leader
			}

		// add another channel here to listen to new AE RPC
		default:
			rf.debug("Sleeping")
			time.Sleep(Tick)
		}

	}
}

func (rf *Raft) leaderLoop() {
	rf.debug("First heartbeat to declare authority.")
	rf.sendAppendEntries()

	rf.internalTimer = time.After(Heartbeat)

	for rf.state == Leader {
		select {
		case <-rf.internalTimer:
			rf.debug("Will send AEs now.")
			rf.sendAppendEntries()
		default:
			rf.debug("Sleeping")
			time.Sleep(Tick)
		}
	}
}

func (rf *Raft) loop() {
	for rf.killed() == false {
		// Your code here (2A)
		// Check if a leader election should be started.

		switch rf.state {
		case Follower:
			rf.followerLoop()
		case Candidate:
			rf.candidateLoop()
		case Leader:
			rf.leaderLoop()
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
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	// Your initialization code here (2A, 2B, 2C).
	rf.toFollower(0)

	// temporary log init to nil as first value
	rf.log = append(rf.log, LogEntry{
		index: 0,
		term:  0,
	})

	rf.debug()

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	go rf.loop()

	return rf
}
