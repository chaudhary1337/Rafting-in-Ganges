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
	"Rafting-in-Ganges/labgob"
	"Rafting-in-Ganges/labrpc"
	"bytes"
	"fmt"
	"math/rand"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

func (rf *Raft) debug(args ...interface{}) {
	return

	counter, _, _, _ := runtime.Caller(1)
	fullName := strings.Split(runtime.FuncForPC(counter).Name(), ".")
	name := fullName[len(fullName)-1]

	indent := strings.Repeat("\t", rf.me)
	fmt.Printf("%s[S%d:%s:%d]{%s}", indent, rf.me, rf.state, rf.currentTerm, name)
	for _, arg := range args {
		fmt.Printf(" (%s)", arg)
	}
	fmt.Println()
}

const (
	// states
	Leader    = "Leader"
	Candidate = "Candidate"
	Follower  = "Follower"
	// times
	HeartBeat    = 100 // the minimum possible amount for the tester
	ElectionBase = 310 // ~3x of heartbeat
	ElectionVar  = 190 // introduces inconsistency in election timers
)

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
	CommandValid bool
	CommandIndex int
	Command      interface{}
}

type LogEntry struct {
	Term    int         // and term when entry was received by leader (first index is 1)
	Command interface{} // each entry contains command for state machine,
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
	state             string        // Follower or Candidate or Leader
	votes             int           // the number of votes a candidate gets
	detectFollower    chan bool     // detects when converted to a follower
	detectElectionWin chan bool     // detects when converted to leader
	applyCh           chan ApplyMsg // for sending commands to state machine

	// persistent state
	currentTerm int        // currentTerm latest term server has seen (initialized to 0 on first boot, increases monotonically)
	votedFor    int        // candidateId that received vote in current term (or null if none)
	log         []LogEntry // log entries

	// volatile state
	commitIndex int // index of highest log entry known to be committed (initialized to 0, increases monotonically)
	lastApplied int // index of highest log entry applied to state machine (initialized to 0, increases monotonically)

	// volatile state for leader
	nextIndex  []int // for each server, index of the next log entry to send to that server (initialized to leader last log index + 1)
	matchIndex []int // for each server, index of highest log entry known to be replicated on server (initialized to 0, increases monotonically)
}

// ============================== RequestVote RPC Logic ==============================

// example RequestVote RPC arguments structure.
// field names must start with capital letters!
type RequestVoteArgs struct {
	Term         int // candidate's term
	CandidateId  int // candidate who is requesting vote
	LastLogIndex int // index of candidate's last log entry
	LastLogTerm  int // term of candidate's last log entry
}

// example RequestVote RPC reply structure.
// field names must start with capital letters!
type RequestVoteReply struct {
	Term        int  // currentTerm for the candidate to update itself
	VoteGranted bool // true means the candidate received a vote
}

// receiver implementation
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	defer rf.persist()

	// 2A
	reply.Term = rf.currentTerm

	if args.Term < rf.currentTerm {
		rf.debug("stale candidate")

		reply.VoteGranted = false
		return
	}

	// 2A && 2B
	if (rf.votedFor == -1 || rf.votedFor == args.CandidateId) && rf.isUpToDate(args) {
		rf.debug("good citizen")

		reply.VoteGranted = true
		rf.votedFor = args.CandidateId
		rf.toFollower(args.Term, args.CandidateId)
		return
	}

	// 2A
	if args.Term > rf.currentTerm {
		rf.toFollower(args.Term)
	}

	reply.VoteGranted = false
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
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) {
	// if not ok, network failure.
	// will rely on other servers to respond, let this one go
	if ok := rf.peers[server].Call("Raft.RequestVote", args, reply); !ok {
		return
	}

	rf.mu.Lock()
	defer rf.mu.Unlock()
	defer rf.persist()

	// 2A
	if rf.state != Candidate || args.Term != rf.currentTerm {
		rf.debug("left politics/term mismatch")
		return
	}

	// 2A
	if reply.Term >= rf.currentTerm {
		rf.debug("stale candidate (me)")
		rf.toFollower(reply.Term)
	}

	// 2A
	if reply.VoteGranted {
		rf.debug("got vote")
		rf.votes++

		// only if a new vote granted, we need to trigger this condition
		if rf.votes == len(rf.peers)/2+1 {
			rf.debug("got majority")
			rf.notify(rf.detectElectionWin)
		}
	}
}

func (rf *Raft) toCandidate() {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	defer rf.persist()
	rf.debug()

	// update states
	rf.state = Candidate
	rf.currentTerm++
	rf.votedFor = rf.me
	rf.votes = 1

	// preparing args
	args := RequestVoteArgs{
		Term:         rf.currentTerm,
		CandidateId:  rf.me,
		LastLogIndex: rf.getLastIndex(),
		LastLogTerm:  rf.getLastTerm(),
	}

	// broadcast
	for server := range rf.peers {
		if server != rf.me {
			go rf.sendRequestVote(server, &args, &RequestVoteReply{})
		}
	}
}

// ============================== AppendEntries RPC Logic ==============================

type AppendEntriesArgs struct {
	// 2A
	Term     int // leader's term
	LeaderId int // for follower to redirect clients
	// 2B
	PrevLogIndex int        // index of log entry immediately preceding new ones
	PrevLogTerm  int        // term of prevLogIndex entry
	Entries      []LogEntry // log entries to store (empty for heartbeat; may send more than one for efficiency)
	LeaderCommit int        // leader's commitIndex
}

type AppendEntriesReply struct {
	// 2A
	Term    int  // currentTerm for leader to update itself
	Success bool // true if follower contained entry matching prevLogIndex and prevLogTerm
	// 2B
	ConflictIndex int // earliest time for the same ConflictTerm
	ConflictTerm  int // used for above
}

// receiver implementation
func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	defer rf.persist()

	reply.Term = rf.currentTerm

	// 2A
	if args.Term < rf.currentTerm {
		rf.debug("stale leader")
		reply.Success = false
		reply.ConflictIndex = -1
		reply.ConflictTerm = -1
		return
	}

	// 2B
	if rf.getLastIndex() < args.PrevLogIndex {
		rf.debug("no entry at PrevLogIndex")

		reply.Success = false
		reply.ConflictIndex = rf.getLastIndex() + 1
		reply.ConflictTerm = -1
		return
	}
	if rf.log[args.PrevLogIndex].Term != args.PrevLogTerm {
		rf.debug("wrong entry at PrevLogIndex")

		lastTerm := rf.getLastTerm()
		firstValid := rf.findLogMatch(false, lastTerm)

		reply.Success = false
		reply.ConflictIndex = firstValid
		reply.ConflictTerm = lastTerm
		return
	}

	// leader sent right entries, they need to be written
	for i := 0; i < len(args.Entries); i++ {
		log_i := args.PrevLogIndex + 1 + i
		// if no data present, add
		if rf.getLastIndex() < log_i {
			rf.log = append(rf.log, args.Entries[i])
			continue
		}
		// exclude this term and beyond - this log is wrong
		// and add this new entry
		if args.Entries[i].Term != rf.log[log_i].Term {
			rf.log = append(rf.log[:log_i], args.Entries[i])
			continue
		}
	}

	if args.LeaderCommit > rf.commitIndex {
		rf.debug("updating commitIndex")
		rf.commitIndex = min(args.LeaderCommit, rf.getLastIndex())
	}

	// apply
	go rf.apply()

	// defaults
	reply.Success = true
	rf.toFollower(args.Term)
	rf.debug("got heartbeat")
}

// sender implementation
func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) {
	// if not ok, network failure.
	// will retry automatically in next heartbeat
	if ok := rf.peers[server].Call("Raft.AppendEntries", args, reply); !ok {
		return
	}

	rf.mu.Lock()
	defer rf.mu.Unlock()
	defer rf.persist()

	// 2A
	// state and term checks
	if rf.state != Leader || args.Term != rf.currentTerm {
		rf.debug("leader no more/term mismatch")
		return
	}
	if reply.Term > rf.currentTerm {
		rf.debug("stale leader (me)")
		rf.toFollower(reply.Term)
		return
	}

	// 2B
	// log checks

	// if reply is not Success, then we can't update matchIndex, since we are yet to match and confirm logs
	if !reply.Success {
		if reply.ConflictTerm == -1 {
			// log entry is not present,
			// the follower tells to supply logs from ConflictIndex,
			// which is where its log ends
			rf.nextIndex[server] = reply.ConflictIndex
		} else {
			// if entry is present, but wrong,
			// we find the latest index of this ConflictTerm
			lastValid := rf.findLogMatch(true, reply.ConflictTerm)

			// if that ConflictTerm exists, okay
			// if it does not exist, we directly use the ConflictIndex
			if lastValid != 0 {
				rf.nextIndex[server] = lastValid + 1
			} else {
				rf.nextIndex[server] = reply.ConflictIndex
			}
		}

		// can't update matchIndex since we still don't know if they match till nextIndex[server] - 1.
		// sending another append entry separately
		// instead of waiting for the next heartbeat
		rf.prepareAppendEntries(server)
		return
	}

	// update matchIndex and nextIndex if reply.Success
	if rf.matchIndex[server] < args.PrevLogIndex+len(args.Entries) {
		rf.matchIndex[server] = args.PrevLogIndex + len(args.Entries)
	}
	rf.nextIndex[server] = rf.matchIndex[server] + 1

	rf.updateCommitIndex()
}

// unlocked
func (rf *Raft) prepareAppendEntries(server int) {
	prevLogIndex := rf.nextIndex[server] - 1

	// making copy
	entries := []LogEntry{}
	entries = append(entries, rf.log[prevLogIndex+1:]...)

	// preparing individual args
	args := AppendEntriesArgs{
		// 2A
		Term:     rf.currentTerm,
		LeaderId: rf.me,
		// 2B
		PrevLogIndex: prevLogIndex,
		PrevLogTerm:  rf.log[prevLogIndex].Term,
		Entries:      entries,
		LeaderCommit: rf.commitIndex,
	}

	go rf.sendAppendEntries(server, &args, &AppendEntriesReply{})
}

func (rf *Raft) toLeader(firstTime bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	rf.debug()

	// update states
	rf.state = Leader

	if firstTime {
		// reinit on first time
		rf.nextIndex = make([]int, len(rf.peers))
		rf.matchIndex = make([]int, len(rf.peers))
		lastIndex := rf.getLastIndex()

		for server := range rf.peers {
			rf.nextIndex[server] = lastIndex + 1
			rf.matchIndex[server] = -1
		}
	}

	// boradcast
	for server := range rf.peers {
		if server == rf.me {
			continue
		}
		rf.prepareAppendEntries(server)
	}
}

// ============================== MAIN Logic ==============================

func (rf *Raft) loop() {
	for !rf.killed() {
		switch rf.loadState() {
		case Follower:
			select {
			case <-rf.detectFollower:
				rf.debug("detected follower")
			case <-time.After(getElectionTimeout()):
				rf.debug("election timer")
				rf.toCandidate()
			}
		case Candidate:
			select {
			case <-rf.detectFollower:
				rf.debug("detected follower")
			case <-rf.detectElectionWin:
				rf.debug("won elections")
				rf.toLeader(true)
			case <-time.After(getElectionTimeout()):
				rf.debug("election timer")
				rf.toCandidate()
			}
		case Leader:
			select {
			case <-rf.detectFollower:
				rf.debug("detected follower")
			case <-time.After(getHeartbeat()):
				rf.debug("heartbeat")
				rf.toLeader(false)
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

	// Your initialization code here (2A, 2B, 2C).
	rf.log = append(rf.log, LogEntry{Term: -1}) // starts with index 1
	rf.commitIndex = 0
	rf.lastApplied = 0
	rf.detectFollower = make(chan bool)
	rf.detectElectionWin = make(chan bool)
	rf.toFollower(0)

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	rf.debug()
	go rf.loop()

	return rf
}

// ============================== General Helpers Logic ==============================

// the tester calls Kill() when a Raft instance won't
// be needed again. for your convenience, we supply
// code to set rf.dead (without needing a lock),
// and a killed() method to test rf.dead in
// long-running loops. you can also add your own
// code to Kill(). you're not required to do anything
// about this, but it may be convenient (for example)
// to suppress debug output from a Kill()ed instance.
func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	// Your code here (2A).
	rf.mu.Lock()
	defer rf.mu.Unlock()

	return rf.currentTerm, rf.state == Leader
}

// locks and loads state
func (rf *Raft) loadState() string {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	return rf.state
}

// non-blocking send for unbuffered channel
func (rf *Raft) notify(ch chan bool) {
	select {
	case ch <- true:
	default:
	}
}

// unlocked and resets follower
func (rf *Raft) toFollower(params ...int) {
	rf.state = Follower

	if len(params) < 1 {
		panic("toFollower needs a term supplied always")
	}
	rf.currentTerm = params[0]

	if len(params) > 1 {
		rf.votedFor = params[1]
	} else {
		rf.votedFor = -1
	}

	rf.notify(rf.detectFollower)
}

// gets a time.Duration as return that can be used in time.After
func getElectionTimeout() time.Duration {
	totalTime := ElectionBase + rand.Intn(ElectionVar)
	return time.Duration(totalTime) * time.Millisecond
}

// gets a time.Duration as return that can be used in time.After
func getHeartbeat() time.Duration {
	return time.Duration(HeartBeat) * time.Millisecond
}

// ============================== Log Helpers Logic ==============================

// unlocked
func (rf *Raft) getLastIndex() int {
	return len(rf.log) - 1
}

// unlocked
func (rf *Raft) getLastTerm() int {
	return rf.log[rf.getLastIndex()].Term
}

// unlocked. source: 5.4.1
func (rf *Raft) isUpToDate(args *RequestVoteArgs) bool {
	// rf.debug("term", rf.getLastTerm(), args.LastLogTerm)
	// rf.debug("index", rf.getLastIndex(), args.LastLogIndex)

	if rf.getLastTerm() != args.LastLogTerm {
		return rf.getLastTerm() < args.LastLogTerm
	} else {
		return rf.getLastIndex() <= args.LastLogIndex
	}
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
	rf.mu.Lock()
	defer rf.mu.Unlock()
	defer rf.persist()

	if rf.state != Leader {
		return -1, -1, false
	}

	rf.log = append(rf.log, LogEntry{
		Term:    rf.currentTerm,
		Command: command,
	})

	// rf.debug("leader", rf.log)
	return rf.getLastIndex(), rf.currentTerm, true
}

// locked. moves lastApplied to commitIndex
// If commitIndex > lastApplied: increment lastApplied, apply log[lastApplied] to state machine
func (rf *Raft) apply() {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	// rf.debug(rf.commitIndex, rf.lastApplied)

	for rf.commitIndex > rf.lastApplied {
		rf.lastApplied++

		rf.applyCh <- ApplyMsg{
			CommandValid: true,
			CommandIndex: rf.lastApplied,
			Command:      rf.log[rf.lastApplied].Command,
		}
	}
}

// unlocked. gets the lastMatch/first match value from log (with term)
func (rf *Raft) findLogMatch(lastMatch bool, term int) int {
	lastIndex := rf.getLastIndex()

	if lastMatch {
		for index := lastIndex; index >= 1; index-- {
			if rf.log[index].Term == term {
				return index
			}
		}

		return 0
	} else {
		for index := 1; index <= lastIndex; index++ {
			if rf.log[index].Term == term {
				return index
			}
		}

		return lastIndex + 1
	}
}

// unlocked. moves the commit index ahead if it can, and applies state changes if matchIndex allows
func (rf *Raft) updateCommitIndex() {
	// see if leader can update its commitIndex to move forward
	// we start from the end to see if we can move it all the way to the log length
	for N := rf.getLastIndex(); N >= rf.commitIndex+1; N-- {
		// if term does not match, we can't do anything
		if rf.log[N].Term != rf.currentTerm {
			continue
		}

		// same term -means-> same leader, means I (leader) am the one committing
		count := 1
		for server := range rf.peers {
			if server != rf.me && rf.matchIndex[server] >= N {
				count++
			}
		}

		// the last majority supported N is taken as commitIndex
		if count > len(rf.peers)/2 {
			rf.commitIndex = N
			// apply
			go rf.apply()
			break
		}
	}
}

// ============================== Persisters Logic ==============================

// 2C
// unlocked
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
func (rf *Raft) persist() {
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(rf.currentTerm)
	e.Encode(rf.votedFor)
	e.Encode(rf.log)
	data := w.Bytes()

	rf.persister.SaveRaftState(data)
}

// 2C
// restore previously persisted state.
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}

	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var currentTerm int
	var votedFor int
	var log []LogEntry

	if d.Decode(&currentTerm) != nil || d.Decode(&votedFor) != nil || d.Decode(&log) != nil {
		rf.debug("error reading states")
	} else {
		rf.currentTerm = currentTerm
		rf.votedFor = votedFor
		rf.log = log
	}
}
