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
	"bytes"
	"log"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	"context"

	"labgob"
	"labrpc"
)

// import "bytes"
// import "labgob"

//
// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in Lab 3 you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh; at that point you can add fields to
// ApplyMsg, but set CommandValid to false for these other uses.
//
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int
}

type LogEntry struct {
	Command interface{} // each entry contains command for state machine,
	Term    int         // and term when entry was received by leader(fisrt index is 1)
}

type AppendEntriesArgs struct {
	//leader term
	Term int
	//ident for leader
	LeaderId int
	//index of log entry immediately preceding new ones
	PrevLogIndex int
	//
	PrevLogTerm  int
	Entries      []LogEntry
	LeaderCommit int
}

type AppendEntriesReply struct {
	Term       int
	Success    bool
	FollowerID int
}

type Config struct {
	followTimeout   time.Duration
	electionTimeout time.Duration
	leaderLeaseTime time.Duration
}

const (
	FOLLOWER raftState = iota
	CANDIDATE
	LEADER
)

type raftState uint32

//
// A Go object implementing a single Raft peer.
//
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	stateChan chan raftState
	//logs
	logs []LogEntry
	// message queue
	shutdown chan struct{}
	//
	state raftState
	//

	// raft config
	config *Config

	lastContact   time.Time
	lastContactMu sync.RWMutex
	lastlogindex  int
	lastlogterm   int

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain
	// .
	currentTermRW sync.RWMutex
	currentTerm   int
	isLeader      bool
	// vote for candidate in this term
	votedFor int
	// log entries
	log []interface{}
	//
	commitIndex int
	//
	lastApplied int
	//
	// rem peers nextId
	nextIndex []int
	//
	matchIndex []int
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	//var term int
	//var isleader bool
	// Your code here (2A).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.currentTerm, rf.isLeader
}

//
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
//
func (rf *Raft) persist() {
	// Your code here (2C).
	// Example:
	// w := new(bytes.Buffer)
	// e := labgob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// data := w.Bytes()
	// rf.persister.SaveRaftState(data)
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(rf.currentTerm)
	e.Encode(rf.votedFor)
	e.Encode(rf.logs)
	data := w.Bytes()
	rf.persister.SaveRaftState(data)
	DPrintf("[persist]: Id %d Term %d State %s\t||\tsave persistent state\n",
		rf.me, rf.currentTerm, state2name(rf.state))

}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	DPrintf("[WARNING] I'm %d send appendEntries to %d ,resp %t", rf.me, server, ok)
	return ok
}

//
// restore previously persisted state.
//
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
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var currentTerm int
	var voteFor int
	var logs []LogEntry
	if d.Decode(&currentTerm) != nil ||
		d.Decode(&voteFor) != nil ||
		d.Decode(&logs) != nil {
		log.Fatal("[readPersist]: decode error!\n")
	} else {
		rf.currentTerm = currentTerm
		rf.votedFor = voteFor
		rf.logs = logs
	}
	DPrintf("[readPersist]: Id %d Term %d State %s\t||\trestore persistent state from Persister\n",
		rf.me, rf.currentTerm, state2name(rf.state))
}

func state2name(state raftState) (res string) {
	switch state {
	case LEADER:
		res = "LEADER"
	case FOLLOWER:
		res = "FOLLOWER"
	case CANDIDATE:
		res = "CANDIDATE"
	}
	return
}

//
// example RequestVote RPC arguments structure.
// field names must start with capital letters!
//
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	// candidate’s term
	Term int
	// candidate requesting vote
	CandidateId int
	// index of candidate’s last log entry
	LastLogIndex int
	// term of candidate’s last log entry
	LastLogTerm int
}

//
// example RequestVote RPC reply structure.
// field names must start with capital letters!
//
type RequestVoteReply struct {
	// Your data here (2A).
	// currentTerm, for candidate to update itself
	Term int
	// true means candidate received vote
	VoteGranted bool
}

//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	//for index, _ := range rf.peers {
	//	go func(index int, args *RequestVoteArgs) {
	//		rf.sendRequestVote(index, args)
	//	}(index, args)
	//}
	DPrintf("[WARING] I'm %d receive vote request from %d, lastLogTerm %d, lastLogIndex %d, Term %d", rf.me, args.CandidateId, args.LastLogTerm, args.LastLogIndex, args.Term)
	rf.mu.Lock()
	defer rf.mu.Unlock()
	reply.Term = args.Term
	reply.VoteGranted = false
	if rf.getCurrentTerm() > args.Term {
		return
	}
	//find a bigger term
	if rf.getCurrentTerm() < args.Term {
		rf.votedFor = -1
		rf.setCurrentTerm(args.Term)
		rf.atomicStoreState(FOLLOWER)
		rf.persist()
	}
	//if in the same term or otherwise
	if rf.votedFor == -1 || rf.votedFor == args.CandidateId {
		lastLogIndex := len(rf.logs) - 1
		lastEntry := rf.logs[lastLogIndex]
		if lastEntry.Term < args.LastLogTerm || (lastEntry.Term == args.LastLogTerm && lastLogIndex <= args.LastLogIndex) {
			rf.votedFor = args.CandidateId
			rf.atomicStoreState(FOLLOWER)
			rf.persist()
			reply.Term = rf.getCurrentTerm()
			reply.VoteGranted = true
			rf.setNowAsLastContact()
			DPrintf("[WARING] %d give vote to %d", rf.me, args.CandidateId)
			return
		}
	}
	return
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	DPrintf("[WARNING] I'm %d receive appendEntriesArgs from leaderId %d, term %d, preLogIndex %d, preLogTerm %d, leaderCommit %d", rf.me, args.LeaderId, args.Term, args.PrevLogIndex, args.PrevLogTerm, args.LeaderCommit)
	reply.Success = false
	reply.Term = rf.getCurrentTerm()
	if args.Term < rf.getCurrentTerm() {
		return
	}
	rf.mu.Lock()
	defer rf.mu.Unlock()
	// don't have log yet
	if len(rf.logs)-1 < args.PrevLogIndex {
		DPrintf("[WARING] %d last log index %d receive append from %d lastLog index %d", rf.me, len(rf.logs)-1, args.LeaderId, args.PrevLogIndex)
		return
	}
	// find leader
	if args.Term > rf.getCurrentTerm() {
		DPrintf("[WARNING] %d find leader %d Term %d", rf.me, args.LeaderId, args.Term)
		rf.setCurrentTerm(args.Term)
		rf.setNowAsLastContact()
		rf.atomicStoreState(FOLLOWER)
		rf.votedFor = -1
		rf.persist()
	}
	prelog := rf.logs[args.PrevLogIndex]
	if prelog.Term == args.PrevLogTerm {
		if len(args.Entries) > 0 {
			//check log conflict
			lastIndex := len(rf.logs) - 1
			appendBeginIndex := args.PrevLogIndex + 1
			newStart := 0
			for index, entry := range args.Entries {
				needToAppendIndex := appendBeginIndex + index
				if needToAppendIndex < lastIndex && rf.logs[needToAppendIndex].Term != entry.Term {
					newStart = needToAppendIndex
					args.Entries = args.Entries[index:]
				}
			}
			if newStart != 0 {
				rf.logs = rf.logs[:newStart]
			}
			rf.logs = append(rf.logs, args.Entries...)
		}
		if args.LeaderCommit > rf.commitIndex {
			lastIndex := len(rf.logs) - 1
			rf.commitIndex = min(args.LeaderCommit, lastIndex)
		}
		rf.atomicStoreState(FOLLOWER)
		rf.setNowAsLastContact()
		rf.persist()
		reply.Success = true
		reply.Term = rf.getCurrentTerm()
	}
}

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}

func (rf *Raft) setNowAsLastContact() time.Time {
	rf.lastContactMu.Lock()
	defer rf.lastContactMu.Unlock()
	rf.lastContact = time.Now()
	return rf.lastContact
}

func (rf *Raft) readLastContact() time.Time {
	rf.lastContactMu.RLock()
	defer rf.lastContactMu.RUnlock()
	return rf.lastContact
}

//
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
//
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	DPrintf("[WARNING] I'm %d send RequestVote to server %d and then resp is %t", rf.me, server, ok)
	return ok
}

func (rf *Raft) write(command interface{}) {
	rf.mu.Lock()
	logEntry := LogEntry{
		Command: command,
		Term:    rf.getCurrentTerm(),
	}
	//add to local but not persist
	rf.logs = append(rf.logs, logEntry)
	index := len(rf.logs) - 1
	rf.mu.Unlock()
	replies := rf.broadcast(index)
	agree := rf.quorumSize()

	select {
	case reply := <-replies:
		if reply.Success && reply.Term == rf.getCurrentTerm() {
			agree++
			rf.mu.Lock()
			rf.nextIndex[reply.FollowerID] = index
			rf.matchIndex[reply.FollowerID] = index
			rf.mu.Unlock()
		} else if !reply.Success && reply.Term == rf.getCurrentTerm() {
			rf.mu.Lock()
			followerNextID := rf.nextIndex[reply.FollowerID]
			followerNextID--

		}
	}
}

func (rf *Raft) appendEntriesTo(replies chan *AppendEntriesReply, follower int, fromIndex int, toIndex int) {
	if rf.atomicGetState() != LEADER {
		return
	}
	go func() {
		rf.mu.Lock()
		if toIndex+1 > len(rf.logs)-1 {
			DPrintf("[WARNING] Leader %d want to send %d, indexOutOfBound from %d to %d but len %d", rf.me, follower, fromIndex, toIndex)
			return
		}
		&AppendEntriesArgs{
			Term:         rf.getCurrentTerm(),
			LeaderId:     rf.me,
			PrevLogIndex: fromIndex,
		}
		rf.logs[fromIndex : toIndex+1]
	}()
}

func (rf *Raft) broadcast(endIndex int) chan *AppendEntriesReply {
	replies := make(chan *AppendEntriesReply, len(rf.peers)-1)
	for index := range rf.peers {
		if index == rf.me {
			continue
		}
		go func() {
			if rf.atomicGetState() != LEADER {
				return
			}
			prevLogIndex := rf.nextIndex[index]
			prelogTerm := rf.logs[prevLogIndex].Term
			appendEntriesArgs := &AppendEntriesArgs{
				Term:         rf.getCurrentTerm(),
				LeaderId:     rf.me,
				PrevLogIndex: prevLogIndex,
				PrevLogTerm:  prelogTerm,
				Entries:      rf.logs[prevLogIndex : index+1],
				LeaderCommit: rf.commitIndex,
			}
			reply := &AppendEntriesReply{
				FollowerID: index,
			}
			if rf.sendAppendEntries(index, appendEntriesArgs, reply) {
				replies <- reply
			}
		}()
	}
	return replies
}

//
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
//
func (rf *Raft) Start(command interface{}) (index int, term int, isLeader bool) {
	index = -1
	term = -1
	isLeader = false

	term, isLeader = rf.GetState()
	if !isLeader {
		return
	}

	// Your code here (2B).

	return index, term, isLeader
}

//
// the tester calls Kill() when a Raft instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (rf *Raft) Kill() {
	// Your code here, if desired.
}

//
// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
//
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me
	rf.currentTerm = 0
	rf.state = FOLLOWER
	rf.config = &Config{
		followTimeout:   time.Duration(time.Millisecond * 150),
		electionTimeout: time.Duration(time.Millisecond * 200),
		leaderLeaseTime: time.Duration(time.Millisecond * 100),
	}
	entries := make([]LogEntry, 0)
	entries = append(entries, LogEntry{Term: 0})
	rf.logs = entries
	rf.votedFor = -1
	rf.nextIndex = make([]int, len(rf.peers))
	rf.matchIndex = make([]int, len(rf.peers))
	rf.isLeader = false
	rf.stateChan = make(chan raftState, 1)

	// Your initialization code here (2A, 2B, 2C).

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())
	go func() {
		for {
			select {
			case <-rf.shutdown:
				DPrintf("server %d shutdown", rf.me)
				return
			default:
			}

			switch rf.atomicGetState() {

			case FOLLOWER:
				rf.FollowerLoop()
			case CANDIDATE:
				rf.CandidateLoop()
			case LEADER:
				rf.LeaderLoop()
			}
		}
	}()

	return rf
}

func (rf *Raft) atomicGetState() raftState {
	stateP := (*uint32)(&rf.state)
	return raftState(atomic.LoadUint32(stateP))
}

func (rf *Raft) atomicStoreState(state raftState) {
	rf.stateChan <- state
	stateP := (*uint32)(&rf.state)
	atomic.StoreUint32(stateP, uint32(state))
}

func randomTimeout(min time.Duration) <-chan time.Time {
	ex := time.Duration(rand.Int63()) % min
	return time.After(min + ex)
}

func (rf *Raft) getCurrentTerm() (term int) {
	rf.currentTermRW.RLock()
	term = rf.currentTerm
	rf.currentTermRW.RUnlock()
	return
}

func (rf *Raft) setCurrentTerm(term int) {
	rf.currentTermRW.Lock()
	defer rf.currentTermRW.Unlock()
	rf.currentTerm = term
}

func (rf *Raft) increaseTerm() (newterm int) {
	rf.currentTermRW.Lock()
	defer rf.currentTermRW.Unlock()
	newterm = rf.currentTerm + 1
	rf.currentTerm = newterm
	return
}

func (rf *Raft) rollback() {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	rf.currentTerm -= 1
}

func (rf *Raft) quorumSize() int {
	return (len(rf.peers) + 1) / 2
}

func (rf *Raft) voteSelf(ctx context.Context) <-chan *RequestVoteReply {
	voteReplies := make(chan *RequestVoteReply, len(rf.peers))
	newTerm := rf.increaseTerm()
	requestVoteArgs := &RequestVoteArgs{
		Term:         newTerm,
		CandidateId:  rf.me,
		LastLogIndex: rf.lastlogindex,
		LastLogTerm:  rf.lastlogterm,
	}
	for index := range rf.peers {
		if index == rf.me {
			continue
		}
		go func(index int, ctx context.Context) {
			select {
			case <-ctx.Done():
				DPrintf("[WARNING] I'm %d stop sending vote request to %d", rf.me, index)
				return
			default:
				reply := &RequestVoteReply{
					VoteGranted: false,
				}
				if rf.sendRequestVote(index, requestVoteArgs, reply) {
					DPrintf("[WARNING] I'm %d receive from %d reply.Term %d reply.VoteGranted %b", rf.me, index, reply.Term, reply.VoteGranted)
					voteReplies <- reply
				}
			}

		}(index, ctx)
	}
	return voteReplies
}

func timeTrigger(duration time.Duration) <-chan time.Time {
	return time.After(duration)
}

// Three Loop
func (rf *Raft) FollowerLoop() {
	checkHearBeatCK := randomTimeout(rf.config.followTimeout)
followerLoop:
	for {
		select {
		case <-checkHearBeatCK:
			DPrintf("[WARNING] I'm Follower %d check lastContact", rf.me)
			lastContact := rf.readLastContact()
			if time.Now().Sub(lastContact) < rf.config.followTimeout {
				DPrintf("[WARNING] I'm Follower %d receive heartbeat", rf.me)
				checkHearBeatCK = randomTimeout(rf.config.followTimeout)
				continue
			}
			//heart beat timeout
			rf.atomicStoreState(CANDIDATE)
			DPrintf("[WARING] I'm Follower %d -> Candidate", rf.me)
			break followerLoop
		case state := <-rf.stateChan:
			if state != FOLLOWER {
				break followerLoop
			}
		}
	}

}

func (rf *Raft) CandidateLoop() {
	votes := 0
	timeout := randomTimeout(rf.config.electionTimeout)
	ctx, cancel := context.WithCancel(context.Background())
	voteReplies := rf.voteSelf(ctx)
	needVote := rf.quorumSize()
	DPrintf("[WARING] I'm %d and want to be leader", rf.me)
candidateLoop:
	for {
		select {
		case <-timeout:
			//	rf state has been candidate already
			DPrintf("[WARNING] I'm %d Election timeout", rf.me)
			rf.rollback()
			timeout = randomTimeout(rf.config.electionTimeout)
			return
		case vote := <-voteReplies:
			if vote.Term > rf.getCurrentTerm() {

				rf.atomicStoreState(FOLLOWER)
				rf.setCurrentTerm(vote.Term)
				DPrintf("[WARING] I'm %d -> follower", rf.me)
				break candidateLoop
			}
			if vote.VoteGranted {
				DPrintf("[WARING] I'm %d receive a vote success", rf.me)
				votes++
			}
			if votes >= needVote {
				cancel()
				rf.atomicStoreState(LEADER)
				DPrintf("[WARNING] I'm %d receive %d votes", rf.me, votes)
				break candidateLoop
			}
		case state := <-rf.stateChan:
			if state != CANDIDATE {
				break candidateLoop
			} else {
				continue
			}
		}
	}
}

func (rf *Raft) sendHeartBeat() <-chan *AppendEntriesReply {
	replies := make(chan *AppendEntriesReply, len(rf.peers))
	for index := range rf.peers {
		if index == rf.me {
			continue
		}
		go func(index int) {
			logEntry, lastIndex := rf.lastLog()
			args := &AppendEntriesArgs{
				Term:         rf.getCurrentTerm(),
				LeaderId:     rf.me,
				PrevLogTerm:  logEntry.Term,
				PrevLogIndex: lastIndex,
				Entries:      nil,
				LeaderCommit: rf.commitIndex,
			}
			reply := &AppendEntriesReply{
				Success: false,
			}
			if rf.sendAppendEntries(index, args, reply) {
				replies <- reply
			}
		}(index)
	}
	return replies
}

func (rf *Raft) LeaderLoop() {
	rf.isLeader = true
	//randomTimeout(rf.config.followTimeout / 2)
	sendHeartBeat := timeTrigger(rf.config.leaderLeaseTime)
	q := rf.quorumSize()
leaderLoop:
	for {
		select {
		case <-sendHeartBeat:
			beat := rf.sendHeartBeat()
			reveiveHeartCount := 0
			select {
			case reply := <-beat:
				DPrintf("[WARNING] I'm leader %d receive appendEntriesReply %d, %t", rf.me, reply.Term, reply.Success)
				if reply.Success {
					//find a leader
					if reply.Term > rf.getCurrentTerm() {
						rf.atomicStoreState(FOLLOWER)
						rf.persist()
						rf.isLeader = false
						break leaderLoop
					} else {
						reveiveHeartCount++
						//receive quorumSize heartbeat reply
						if reveiveHeartCount > q {
							rf.isLeader = true
							continue leaderLoop
						}
					}
				} else {
					//	append request fail
					if reply.Term > rf.getCurrentTerm() {
						rf.atomicStoreState(FOLLOWER)
						rf.persist()
						rf.isLeader = false
						break leaderLoop
					}
				}
			}
			sendHeartBeat = timeTrigger(rf.config.leaderLeaseTime)
		case state := <-rf.stateChan:
			if state != LEADER {
				break leaderLoop
			}
		}
	}
}
func (rf *Raft) lastLog() (log LogEntry, lastIndex int) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	lastIndex = len(rf.logs) - 1
	log = rf.logs[lastIndex]
	return
}
