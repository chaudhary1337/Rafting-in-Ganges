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
	"Rafting-in-Ganges/labrpc"
	"fmt"
	"math/rand"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

const (
	// states
	Leader    = "Leader"
	Candidate = "Candidate"
	Follower  = "Follower"
	// times
	HeartBeat    = 100 // the minimum possible amount for the tester
	ElectionBase = 350 // 3x of heartbeat
	ElectionVar  = 350 // introduces inconsistency in election timers
)

func (rf *Raft) debug(args ...interface{}) {
	counter, _, _, _ := runtime.Caller(1)
	fullName := strings.Split(runtime.FuncForPC(counter).Name(), ".")
	name := fullName[len(fullName)-1]

	// return

	indent := strings.Repeat("\t", 7*rf.me)
	fmt.Printf("%s[S%d:%s:%d]{%s}", indent, rf.me, rf.state, rf.currentTerm, name)
	for _, arg := range args {
		fmt.Printf(" (%s)", arg)
	}
	fmt.Println()
}

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
	Command      interface{}
	CommandIndex int
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
	state             string    // Follower or Candidate or Leader
	votes             int       // number of votes gotten
	detectElectionWin chan bool // receives notification on winning elections
	detectHeartbeat   chan bool // disallows other channels to be reset
	detectToFollower  chan bool // triggers/forces forces state change detection
	detectValidVote   chan bool // disallows other channels to be reset

	// persistent state
	currentTerm int // currentTerm latest term server has seen (initialized to 0 on first boot, increases monotonically)
	votedFor    int // candidateId that received vote in current term (or null if none)
}

// non blocking notify
func (rf *Raft) notify(ch chan bool) {
	select {
	case ch <- true:
	default:
	}
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	// Your code here (2A).
	rf.mu.Lock()
	defer rf.mu.Unlock()

	term := rf.currentTerm
	isLeader := rf.state == Leader

	return term, isLeader
}

// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
func (rf *Raft) persist() {
	// Your code here (2C).
	// Example:
	// w := new(bytes.Buffer)
	// e := labgob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// data := w.Bytes()
	// rf.persister.SaveRaftState(data)
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

// example RequestVote RPC arguments structure.
// field names must start with capital letters!
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term        int // candidate's term
	CandidateId int // candidate who is requesting vote
}

// example RequestVote RPC reply structure.
// field names must start with capital letters!
type RequestVoteReply struct {
	// Your data here (2A).
	Term        int  // currentTerm for the candidate to update itself
	VoteGranted bool // true means the candidate received a vote
}

type AppendEntriesArgs struct {
	Term     int
	LeaderId int
}

type AppendEntriesReply struct {
	Term    int
	Success bool
}

// receiver implementation
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		reply.VoteGranted = false
		return
	}

	// more to be added for 2B
	if rf.votedFor == -1 || rf.votedFor == args.CandidateId {
		reply.Term = rf.currentTerm
		reply.VoteGranted = true
		return
	}

	// default case
	reply.Term = rf.currentTerm
	reply.VoteGranted = false
	return
}

// receiver implementation
func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		reply.Success = false
		return
	}

	// handle logic for wrong logs here

	reply.Term = rf.currentTerm
	reply.Success = true

	if reply.Success {
		rf.notify(rf.detectHeartbeat)
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
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) {
	// its fine if a server is not reachable
	if ok := rf.peers[server].Call("Raft.RequestVote", args, reply); !ok {
		return
	}

	rf.mu.Lock()
	defer rf.mu.Unlock()
	rf.debug()

	// at wrong term
	if rf.currentTerm != args.Term || reply.Term > rf.currentTerm {
		rf.toFollower(reply.Term)
		rf.notify(rf.detectToFollower)
		return
	}

	if reply.VoteGranted {
		rf.votes++
	}

	// only changed once
	if rf.votes == len(rf.peers)/2+1 {
		rf.state = Leader
		rf.notify(rf.detectElectionWin)
	}
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) {
	// its fine if a server is not reachable
	// FOR NOW
	if ok := rf.peers[server].Call("Raft.AppendEntries", args, reply); !ok {
		return
	}

	rf.mu.Lock()
	defer rf.mu.Unlock()
	rf.debug()

	if rf.currentTerm != args.Term || reply.Term > rf.currentTerm {
		rf.toFollower(reply.Term)
		rf.notify(rf.detectToFollower)
	}

	if !reply.Success {
		// logic to handle logs
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
	index := -1
	term := -1
	isLeader := true

	// Your code here (2B).

	return index, term, isLeader
}

// gets a time.Duration as return that can be used in time.After
// uses ElectionBase and ElectionVar
func getElectionTimeout() time.Duration {
	totalTime := ElectionBase + rand.Intn(ElectionVar)
	return time.Duration(totalTime) * time.Millisecond
}

// gets a time.Duration as return that can be used in time.After
// uses Heartbeat
func getHeartbeat() time.Duration {
	return time.Duration(HeartBeat) * time.Millisecond
}

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
	// Your code here, if desired.
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

func (rf *Raft) resetChannels() {
	rf.detectElectionWin = make(chan bool)
	rf.detectHeartbeat = make(chan bool)
	rf.detectToFollower = make(chan bool)
	rf.detectValidVote = make(chan bool)
}

func (rf *Raft) toFollower(term int) {
	rf.debug()
	rf.resetChannels()

	rf.state = Follower
	rf.currentTerm = term
	rf.votedFor = -1
	rf.votes = 0 // shouldn't be needed, since candidate does its own
}

// assumes lock, broadcast votes to all servers
func (rf *Raft) broadcastVotes() {
	rf.debug()

	args := RequestVoteArgs{
		Term:        rf.currentTerm,
		CandidateId: rf.me,
	}

	for server := range rf.peers {
		if server != rf.me {
			go rf.sendRequestVote(server, &args, &RequestVoteReply{})
		}
	}
}

// converts to candidate, starts election by broadcasting votes
func (rf *Raft) toCandidate() {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	rf.debug()
	rf.resetChannels()

	rf.state = Candidate
	rf.currentTerm++
	rf.votes = 1

	rf.broadcastVotes()
}

// assumes lock
func (rf *Raft) broadcastAppendEntries() {
	rf.debug()

	args := AppendEntriesArgs{}

	for server := range rf.peers {
		if server != rf.me {
			go rf.sendAppendEntries(server, &args, &AppendEntriesReply{})
		}
	}

}

func (rf *Raft) toLeader() {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	rf.debug()
	rf.resetChannels()
	rf.broadcastAppendEntries()
}

func (rf *Raft) loop() {
	for !rf.killed() {
		rf.mu.Lock()
		state := rf.state
		rf.debug()
		rf.mu.Unlock()

		switch state {
		case Follower:
			select {
			case <-rf.detectHeartbeat:
				rf.debug("detected hearbeat")
			case <-rf.detectValidVote:
				rf.debug("detected valid vote")
			case <-time.After(getElectionTimeout()):
				rf.debug("detected election timeout")
				rf.toCandidate()
			}
		case Candidate:
			select {
			case <-rf.detectElectionWin:
				rf.debug("detected election win")
				rf.toLeader()
			case <-rf.detectHeartbeat:
				rf.debug("detected hearbeat")
			case <-rf.detectToFollower:
				rf.debug("detected to follower")
			case <-rf.detectValidVote:
				rf.debug("detected valid vote")
			case <-time.After(getElectionTimeout()):
				rf.debug("detected election timeout")
				rf.toCandidate()
			}
		case Leader:
			select {
			// add cases for other valid heartbeats, valid votes, if needed
			case <-rf.detectToFollower:
				rf.debug("detected to follower")
			case <-time.After(getHeartbeat()):
				rf.debug("detected heartbeat timeout")
				rf.toLeader()
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
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	// Your initialization code here (2A, 2B, 2C).
	rf.toFollower(0)
	rf.resetChannels()

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	rf.debug()
	go rf.loop()

	return rf
}
