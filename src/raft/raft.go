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


import "sync"

// replace "net/rpc" with "labrpc", which simulates a network that can lose requests,
// lose replies, delay messages, and entirely disconnect particular hosts.
import "labrpc"
import "time"
import "math/rand"

// import "bytes"
// import "encoding/gob"

// state of  a server
type State string

const (
	LEADER    State = "Leader"
	CANDIDATE       = "Candidate"
	FOLLOWER        = "Follower"
)

const (
	ELECTIONTIMEMIN      = 550 * time.Millisecond
	ElectionTimeOutRange = 333
	HeartBeatInterval    = 50 * time.Millisecond
)

//
// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make().
//
type ApplyMsg struct {
	Index       int
	Command     interface{}
	UseSnapshot bool   // ignore for lab2; only used in lab3
	Snapshot    []byte // ignore for lab2; only used in lab3
}

// Log entry
type LogEntry struct {
	Term    int
	Command interface{}
}

//
// A Go object implementing a single Raft peer.
//
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

	// persistent state on all servers:
	currentTerm int
	votedFor    int
	log         []LogEntry

	// volatile state on all servers:
	commitIndex int
	lastApplied int

	// volatile state on leaders:
	nextIndex  []int
	matchIndex []int

	// state
	state State

	// count the number vote for this candidate
	voteCount int

	// channel
	heartbeatCh   chan bool
	commitLogCh   chan bool
	grantVoteCh   chan bool
	winElectionCh chan bool
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	var term int
	var isleader bool
	// Your code here (2A).
	term = rf.currentTerm
	isleader = rf.state == LEADER
	return term, isleader
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
	// e := gob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// data := w.Bytes()
	// rf.persister.SaveRaftState(data)
}

//
// restore previously persisted state.
//
func (rf *Raft) readPersist(data []byte) {
	// Your code here (2C).
	// Example:
	// r := bytes.NewBuffer(data)
	// d := gob.NewDecoder(r)
	// d.Decode(&rf.xxx)
	// d.Decode(&rf.yyy)
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
}

//
// example RequestVote RPC arguments structure.
// field names must start with capital letters!
//
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term         int
	CandidateId  int
	LastLogIndex int
	LastLogTerm  int
}

//
// example RequestVote RPC reply structure.
// field names must start with capital letters!
//
type RequestVoteReply struct {
	// Your data here (2A).
	Term        int
	VoteGranted bool
}

func (rf *Raft) getLastLogIndex() int {
	return len(rf.log) - 1
}


func (rf *Raft) getLastLogTerm() int {
	return rf.log[len(rf.log) - 1].Term
}

//
// example RequestVote RPC handler.
//

func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	// 1. Reply false if term < currentTerm($5.1)
	if args.Term < rf.currentTerm {
		reply.VoteGranted = false
		reply.Term = rf.currentTerm
		return
	}
	// Switch to follower if term > currentTerm
	if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
		rf.state = FOLLOWER
		rf.voteCount = 0
		rf.votedFor = -1
	}
	// 2. If votedFor is null or candidateId, and candidate's log is at least as
	// up-to-date as receiver's log, grant vote($5.2, $5.4)
	reply.Term = rf.currentTerm
	lastLogTerm := rf.getLastLogTerm()
	lastLogIndex := rf.getLastLogIndex()
	upToDate := (args.LastLogTerm > lastLogTerm) ||
		((args.LastLogTerm == lastLogTerm) && (args.LastLogIndex >= lastLogIndex))
	if upToDate && (rf.votedFor == -1 || rf.votedFor == args.CandidateId) {
		reply.VoteGranted = true
		rf.state = FOLLOWER
		rf.voteCount = 0
		rf.votedFor = args.CandidateId
		rf.grantVoteCh <- true
	}
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

	rf.mu.Lock()
	defer rf.mu.Unlock()
	if ok {
		if rf.state != CANDIDATE || args.Term != rf.currentTerm {
			return ok
		}

		if reply.Term > rf.currentTerm {
			rf.currentTerm = reply.Term
			rf.state = FOLLOWER
			rf.voteCount = 0
			rf.votedFor = -1
			return ok
		}

		if reply.VoteGranted {
			rf.voteCount++
			if rf.state == CANDIDATE && rf.voteCount > len(rf.peers)/2 {
				rf.state = LEADER
				rf.winElectionCh <- true
			}
		}
	}
	return ok
}

func (rf *Raft) broadcastRequestVote() {
	rf.mu.Lock()
	args := RequestVoteArgs{
		Term:         rf.currentTerm,
		CandidateId:  rf.me,
		LastLogIndex: rf.getLastLogIndex(),
		LastLogTerm:  rf.getLastLogTerm(),
	}
	rf.mu.Unlock()

	for server := range rf.peers {
		if server != rf.me && rf.state == CANDIDATE {
			go func(server int) {
				var reply RequestVoteReply
				rf.sendRequestVote(server, &args, &reply)
			}(server)
		}
	}
}

//
// example AppendEntries RPC argument structure:
//
type AppendEntriesArgs struct {
	Term         int
	LeaderId     int
	PrevLogIndex int
	PrevLogTerm  int
	Entries      []LogEntry
	LeaderCommit int
}

//
// example AppendEntries RPC reply structure
//
type AppendEntriesReply struct {
	Term    int
	Success bool
	NextIndex int
}

func (rf *Raft) PrevLogIndex(server int) int {
	return rf.nextIndex[server] - 1
}

func (rf *Raft) PrevLogTerm(server int) int {
	return rf.log[rf.PrevLogIndex(server)].Term
	//PrevLogIndex := rf.PrevLogIndex(server)
	//if PrevLogIndex >= 0 {
	//	return rf.log[PrevLogIndex].Term
	//} else {
	//	return 0
	//}
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	reply.Success = false
	// 1.Reply false if term < currentTerm($5.1)
	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		reply.NextIndex = rf.getLastLogIndex() + 1
		return
	}

	if args.LeaderId != rf.me {
		rf.heartbeatCh <- true
	}
	//rf.heartbeatCh <- true
	// if term > currenTerm, update currentTerm = Term
	if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
		rf.state = FOLLOWER
		rf.voteCount = 0
		rf.votedFor = -1
	}
	reply.Term = args.Term
	// Reply false if last log index < prevLogIndex, wait for retry
	if rf.getLastLogIndex() < args.PrevLogIndex {
		reply.NextIndex = rf.getLastLogIndex() + 1
		return
	} else if rf.log[args.PrevLogIndex].Term != args.PrevLogTerm {
		// 2. Reply false if log doesn't contain an entry at prevLogIndex
		// whose term matches preLogTerm($5.3)
		// consistency check nextIndex optimization:
		// when rejecting an AppendEntries request, a follower can include the term of conflicting entry
		// and the first index it stores for that term.
		term := rf.log[args.PrevLogTerm].Term
		for idx := args.PrevLogIndex - 1; idx >= 0; idx-- {
			if rf.log[idx].Term != term {
				reply.NextIndex = idx + 1
				break
			}
		}
		return
	}

	// 3. If an existing entry conflicts with a new one(same index but different terms),
	// delete the existing entry and all that follows it($5.3)
	rf.log = rf.log[:args.PrevLogIndex+1]
	// 4. Append any new entries not already in the log
	rf.log = append(rf.log, args.Entries...)
	reply.Success = true
	reply.NextIndex = rf.getLastLogIndex() + 1
	// 5. If leaderCommit > commiIndex, set commitIndex =
	// min(leaderCommit, index of the last netry entry)
	if args.LeaderCommit > rf.commitIndex {
		lastLogIndex := rf.getLastLogIndex()
		if args.LeaderCommit > lastLogIndex {
			rf.commitIndex = lastLogIndex
		} else {
			rf.commitIndex = args.LeaderCommit
		}
		rf.commitLogCh <- true
	}
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if ok {
		if rf.state != LEADER || args.Term != rf.currentTerm {
			return ok
		}

		if reply.Term > rf.currentTerm {
			rf.currentTerm = reply.Term
			rf.state = FOLLOWER
			rf.voteCount = 0
			rf.votedFor = -1
			return ok
		}

		if reply.Success {
			if len(args.Entries) > 0 {
				nextIndex := args.PrevLogIndex + len(args.Entries) + 1
				rf.nextIndex[server] = nextIndex
				rf.matchIndex[server] = nextIndex - 1
			}
		} else {
			rf.nextIndex[server] = reply.NextIndex
		}
	}
	return ok
}

// if there exists an N such that N > commitIndex, a majority of
// matchIndex[i] >= N, and log[N].term == currentTerm:
// set commitIndex = N($5.3, $5.4)
// 5.4.2 committing entries from previous term
func (rf *Raft) updateCommitIndex() {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	updateCommitIndex := rf.commitIndex
	// ($5.4.2) find the Max updateCommitIndex(N)
	for N := rf.commitIndex + 1; N <= rf.getLastLogIndex(); N++ {
		cnt := 1
		for server := range rf.peers {
			// ($5.4.2) Raft never commits log from previous terms by counting replicas,
			// Only log entries from the leader’s current term are committed by counting replicas;
			if rf.me != server && rf.matchIndex[server] >= N && rf.log[N].Term == rf.currentTerm {
				cnt++
			}
		}
		if cnt > len(rf.peers) / 2 {
			DPrintf("leader %d at term %d commit index from %d to %d", rf.me, rf.currentTerm, rf.commitIndex, N)
			updateCommitIndex = N
		}
	}
	if updateCommitIndex != rf.commitIndex {
		rf.commitIndex = updateCommitIndex
		rf.commitLogCh <- true
	}
}

func (rf *Raft) broadcastAppendEntries() {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	for server := range rf.peers {
		if server != rf.me && rf.state == LEADER {
			go func(server int) {
				prevLogIndex := rf.PrevLogIndex(server)
				args := AppendEntriesArgs{
					Term:         rf.currentTerm,
					LeaderId:     rf.me,
					PrevLogIndex: prevLogIndex,
					PrevLogTerm:  rf.log[prevLogIndex].Term,
					Entries:      rf.log[prevLogIndex+1:],
					LeaderCommit: rf.commitIndex,
				}
				rf.sendAppendEntries(server, &args, &AppendEntriesReply{})
			}(server)
		}
	}
}

//
// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election.
//
// the first return value is the index that the command will appear at
// if it's ever committed. the second return value is the current
// term. the third return value is true if this server believes it is
// the leader.
//
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	index := -1
	term := rf.currentTerm
	isLeader := rf.state == LEADER
	if isLeader {
		index = rf.getLastLogIndex() + 1
		rf.log = append(rf.log, LogEntry{Term:term, Command:command})
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

func (rf *Raft) init() {
	rf.state = FOLLOWER
	rf.votedFor = -1
	rf.voteCount = 0
	rf.log = append(rf.log, LogEntry{Term: 0})
	rf.currentTerm = 0
	rf.commitLogCh = make(chan bool)
	rf.heartbeatCh = make(chan bool)
	rf.grantVoteCh = make(chan bool)
	rf.winElectionCh = make(chan bool)
}

func (rf *Raft) randomElectionTimeout() time.Duration {
	return ELECTIONTIMEMIN + time.Millisecond*time.Duration(rand.Int63n(ElectionTimeOutRange))
}

func (rf *Raft) StateSwitchLoop() {
	for {
		switch rf.state {
		case FOLLOWER:
			select {
			case <-rf.heartbeatCh:
			case <-rf.grantVoteCh:
			case <-time.After(rf.randomElectionTimeout()):
				rf.mu.Lock()
				rf.state = CANDIDATE
				rf.mu.Unlock()
			}
		case LEADER:
			//fmt.Printf("Leader:%v %v\n",rf.me,"boatcastAppendEntries	")
			rf.updateCommitIndex()
			rf.broadcastAppendEntries()
			time.Sleep(HeartBeatInterval)
		case CANDIDATE:
			rf.mu.Lock()
			rf.currentTerm++
			rf.votedFor = rf.me
			rf.voteCount = 1
			rf.mu.Unlock()
			go rf.broadcastRequestVote()
			//fmt.Printf("%v become CANDIDATE %v\n",rf.me,rf.currentTerm)
			select {
			case <-rf.winElectionCh:
				rf.mu.Lock()
				rf.state = LEADER
				rf.nextIndex = make([]int, len(rf.peers))
				rf.matchIndex = make([]int, len(rf.peers))
				for i := range rf.peers {
					rf.nextIndex[i] = rf.getLastLogIndex() + 1
					rf.matchIndex[i] = 0
				}
				rf.mu.Unlock()
			case <-rf.heartbeatCh:
				rf.mu.Lock()
				rf.state = FOLLOWER
				rf.mu.Unlock()
			case <-time.After(rf.randomElectionTimeout()):
			}
		}
	}
}

// if commitIndex > lastApplied: increment latApplied,
// apply log[lastApplied] to state machine($5.3)
func (rf *Raft) applyLog(applyCh chan ApplyMsg) {
	for {
		select {
		case <-rf.commitLogCh:
			//	println(rf.me,rf.lastApplied,rf.commitIndex)
			rf.mu.Lock()
			commitIndex := rf.commitIndex
			for i := rf.lastApplied + 1; i <= commitIndex; i++ {
				msg := ApplyMsg{Index: i, Command: rf.log[i].Command}
				applyCh <- msg
				rf.lastApplied = i
			}
			rf.mu.Unlock()
		}
	}
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

	// Your initialization code here (2A, 2B, 2C).
	rf.init()

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())
	go rf.StateSwitchLoop()
	go rf.applyLog(applyCh)

	return rf
}