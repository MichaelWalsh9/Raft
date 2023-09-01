package raft

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

// Skeleton provided by Boston University CS350, John Liagouris
// Code written by Michael Walsh (mpw9@bu.edu)

import (
	"sync"
	"sync/atomic"
	"cs350/labrpc"
	"math/rand"
	"time"
	"bytes"
	"cs350/labgob"
	"fmt"
)

// import "bytes"
// import "cs350/labgob"

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
	CommandValid 	bool
	Command      	interface{}
	CommandIndex 	int

	// For 2D:
	SnapshotValid 	bool
	Snapshot      	[]byte
	SnapshotTerm  	int
	SnapshotIndex 	int
}

// A Go object implementing log entries
type Entry struct {
	Command		interface{}					// Command for state machine
	Term		int							// Term when entry was committed
}

// Struct to simplify encoding/decoding persistent state
type Persistent struct {
	CurrentTerm 	int						// Holds currentTerm for persister
	VotedFor		int						// Holds votedFor for persister
	Log				[]Entry					// Holds log for persister
}

// A Go object implementing a single Raft peer.
type Raft struct {
	mu        		sync.Mutex          	// Lock to protect shared access to this peer's state
	peers     		[]*labrpc.ClientEnd 	// RPC end points of all peers
	persister 		*Persister          	// Object to hold this peer's persisted state
	me        		int                 	// this peer's index into peers[]
	dead      		int32               	// set by Kill()

	// Persistent State (All Servers)
	currentTerm		int 					// Latest term server has seen
	votedFor		int						// candidateId that received vote in current election, -1 is null
	log				[]Entry					// Log Entries

	// Volatile State (All Servers)
	commitIndex		int 					// Index of highest log entry known to be committed 
	lastApplied		int 					// Index of highest log entry applied to state machine

	// Volatile State (Leaders Only)
	nextIndex		[]int					// List of the next log entry to send to each server
	matchIndex		[]int					// List of the highest log entry known to be replicated on each server

	// Additional
	role 			int 					// Tracks what the server believes its role is, 0 = follower, 1 = candidate, 2 = leader
	lastBeat 		time.Time				// Tracks the time of the last communication from the current leader
	ballotBox		[2]int					// Tracks votes received by server as a candidate in elections
	voteChan		chan *RPCReply			// Channel for candidates receiving replies to requestVote RPC's
	leaderChan		chan AppendResult	 	// Channel for leaders receiving replies to AppendEntries RPC's
	timeoutChan		chan int				// Channel for receiving timeout alerts from ticker
	applyChan		chan ApplyMsg

}

// A Go object implementing AppendEntries RPC requests
type AppendEntriesArgs struct {
	Term			int						// Leader's currentTerm
	LeaderId		int						// Leader's ID, used for follower to redirect clients
	PrevLogIndex	int 					// Index of log entry immediately preceding new ones
	PrevLogTerm		int						// Term of log entry immediately preceding new ones
	Entries 		[]Entry					// Log entries to store
	LeaderCommit	int						// Leader's commitIndex
}

// A Go object implementing RequestVote RPC requests
type RequestVoteArgs struct {
	Term			int						// Candidate's currentTerm
	CandidateId		int						// Candidate's ID
	LastLogIndex	int						// Index of candidate's last log entry
	LastLogTerm		int						// Term of candidate's last log entry
}

// A Go object implementing replies for both AppendEntries and RequestVote RPC's
type RPCReply struct {
	Term			int						// Responder's currentTerm, used for candidate/leader to update self
	Success			bool					// Boolean implementing Success for AppendEntries and VoteGranted for RequestVote
	LastCommit		int						// Index of last log entry a follower is known to have commited, used to speed up log realignment
}

type AppendResult struct {
	args			*AppendEntriesArgs
	reply			*RPCReply
	server			int
}

// Safe with concurrency function for getting a server's rf.peers value
func (rf *Raft) getPeers() []*labrpc.ClientEnd {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.peers
}

// Safe with concurrency function for getting a server's rf.me value
func (rf *Raft) getID() int {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.me
}

// Safe with concurrency function for getting a server's currentTerm value
func (rf *Raft) getCurrentTerm() int {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.currentTerm
}

// Safe with concurrency function for updating a server's currentTerm value
func (rf *Raft) setCurrentTerm(i int) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	rf.currentTerm = i
	rf.persist()
}

// Safe with concurrency function for getting a server's votedFor value
func (rf *Raft) getVotedFor() int {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.votedFor
}

// Safe with concurrency function for updating a server's votedFor value
func (rf *Raft) setVotedFor(i int) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	rf.votedFor = i
	rf.persist()
}

// Safe with concurrency function for getting a server's log
func (rf *Raft) getLog() []Entry {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.log
}

func (rf *Raft) getLastLogIndex() int {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return len(rf.log) - 1
}

// Safe with concurrency function for getting an entry on a server's log
func (rf *Raft) getEntry(i int) Entry {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if (i < len(rf.log)) && (i != -1) { 
		return rf.log[i]
	} else {
		faux := Entry {
			Command: nil,
			Term: -1,
		}
		return faux
	}
}

func (rf *Raft) getLogFromIndex(i int) []Entry {
	rf.mu.Lock()
	rf.mu.Unlock()
	return rf.log[i:]
}

// Safe with concurrency function for setting a server's log
func (rf *Raft) setLog(newLog []Entry) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	rf.log = newLog
	rf.persist()
}

// Safe with concurrency function for rewriting a server's log starting from a specified index
func (rf *Raft) addLogIndex(newEntries []Entry, index int) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	// I hate slices why on earth is append() a write aghhhh
	if (index == len(rf.log)) {
		newLog := make([]Entry, 0, len(rf.log) + len(newEntries))
		newLog = append(newLog, rf.log...)
		newLog = append(newLog, newEntries...)
		rf.log = newLog
	} else {
		newLog := make([]Entry, 0, len(rf.log[:index]) + len(newEntries))
		newLog = append(newLog, rf.log[:index]...)
		newLog = append(newLog, newEntries...)
		rf.log = newLog
	}
	rf.persist()
}

// Safe with concurrency function for adding to a server's log
func (rf *Raft) appendLog(newEntries []Entry) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	for _, entry := range newEntries {
		rf.log = append(rf.log, entry)
	}
	rf.persist()
}

// Safe with concurrency function for getting a server's commitIndex value
func (rf *Raft) getCommitIndex() int {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.commitIndex
}

// Safe with concurrency function for updating a server's commitIndex value
func (rf *Raft) setCommitIndex(i int) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	rf.commitIndex = i
	// fmt.Printf("Server %v (Term %v) has updated its commit index to %v \n", rf.me, rf.currentTerm, rf.commitIndex)
}

// Safe with concurrency function for getting a server's lastApplied value
func (rf *Raft) getLastApplied() int {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.lastApplied
}

// Safe with concurrency function for updating a server's lastApplied value
func (rf *Raft) setLastApplied(i int) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	rf.lastApplied = i
}

// Safe with concurrency function for getting a server's nextIndex value
func (rf *Raft) getNextIndex(i int) int {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.nextIndex[i]
}

func (rf *Raft) appendNextIndex(i int) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	rf.nextIndex = append(rf.nextIndex, i)
}

// Safe with concurrency function for updating a server's nextIndex value
func (rf *Raft) setNextIndex(server int, i int) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	rf.nextIndex[server] = i
}

// Safe with concurrency function for getting a server's matchIndex value
func (rf *Raft) getMatchIndex(i int) int {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.matchIndex[i]
}

func (rf *Raft) appendMatchIndex(i int) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	rf.matchIndex = append(rf.matchIndex, i)
}

// Safe with concurrency function for updating a server's matchIndex value
func (rf *Raft) setMatchIndex(server int, i int) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	rf.matchIndex[server] = i
}

// Safe with concurrency function for getting a server's role value
func (rf *Raft) getRole() int {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.role
}

// Safe with concurrency function for updating a server's role value
func (rf *Raft) setRole(i int) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	rf.role = i
}

// Safe with concurrency function for getting a server's lastBeat value
func (rf *Raft) getLastBeat() time.Time {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.lastBeat
}

// Safe with concurrency function for updating a server's lastBeat value
func (rf *Raft) setLastBeat() {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	rf.lastBeat = time.Now()
}

// Safe with concurrency function for getting a server's ballotBox value
func (rf *Raft) getBallotBox() [2]int {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.ballotBox
}

// Safe with concurrency function for setting the value of a server's ballotBox
func (rf *Raft) setBallotBox(newBox [2]int) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	rf.ballotBox = newBox
}

// Safe with concurrency function for adding a vote to a server's ballotBox
// false = no vote
// true = yes vote
func (rf *Raft) addVote(vote bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if vote {
		rf.ballotBox[0]++
	}
	rf.ballotBox[1]++
}

// Safe with concurrency function for resetting a server's ballot box
func (rf *Raft) resetBallotBox() {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	rf.ballotBox = [2]int{0, 0}
}

// Safe with concurrency function for resetting much of a server's state
func (rf *Raft) resetServer() {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	rf.ballotBox = [2]int{0, 0}
	rf.nextIndex = []int{}
	rf.matchIndex = []int{}
	rf.lastBeat = time.Now()
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	return rf.getCurrentTerm(), (rf.getRole() == 2)
}

// Save Raft's persistent state to stable storage
func (rf *Raft) persist() {
	buffer := new(bytes.Buffer)
	encoder := labgob.NewEncoder(buffer)
	state := Persistent {
		CurrentTerm: rf.currentTerm,
		VotedFor: rf.votedFor,
		Log: rf.log,
	}
	encoder.Encode(state)
	stateBytes := buffer.Bytes()
	rf.persister.SaveRaftState(stateBytes)
}

// Restore previously persisted state.
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	} else {
		rf.mu.Lock()
		defer rf.mu.Unlock()
		r := bytes.NewBuffer(data)
		decoder := labgob.NewDecoder(r)
		var state Persistent
		if err := decoder.Decode(&state); err != nil {
			fmt.Println(err)
		} else {
			rf.currentTerm = state.CurrentTerm
			rf.votedFor = state.VotedFor
			rf.log = state.Log
		}
	}
}

func min(i1 int, i2 int) int {
	if i1 > i2 {
		return i2
	}
	return i1
}

// A service wants to switch to snapshot.  Only do so if Raft hasn't
// have more recent info since it communicate the snapshot on applyCh.
func (rf *Raft) CondInstallSnapshot(lastIncludedTerm int, lastIncludedIndex int, snapshot []byte) bool {

	// Your code here (2D).

	return true
}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (2D).

}

func (rf *Raft) isUpToDate(args *RequestVoteArgs) bool {
	lastLog := rf.getEntry(rf.getLastLogIndex())
	if args.LastLogTerm > lastLog.Term {
		return true
	} else if args.LastLogTerm == lastLog.Term {
		return args.LastLogIndex >= rf.getLastLogIndex()
	} else {
		return false
	}
}

// RequestVote RPC Handler
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RPCReply) {
	
	// Reset VotedFor if Candidate is from a higher term	
	if (args.Term > rf.getCurrentTerm()) {
		rf.setVotedFor(-1)
	}
	
	// Convert to follower immediately if RPC is from server with higher term
	if (args.Term > rf.getCurrentTerm()) {
		if (rf.getRole() != 0) {
			rf.transitionRole(0)
		}
	}

	// Branching logic for granting votes
	switch {		
	case args.Term < rf.getCurrentTerm():
		// Candidate is out of date, do not give vote
		reply.Success = false
	case (((rf.getVotedFor() == -1) || (rf.getVotedFor() == args.CandidateId)) || (rf.getCurrentTerm() < args.Term)) && rf.isUpToDate(args):
		// Candidate is valid, give vote
		reply.Success = true
		rf.setVotedFor(args.CandidateId)
		rf.setCurrentTerm(args.Term)
		if (rf.getRole() != 0) {
			rf.transitionRole(0)
		}
		rf.setLastBeat()
	default:
		// Do not give vote as default
		reply.Success = false
	}

	reply.Term = rf.getCurrentTerm()

}

func (rf *Raft) applyEntries(oldIndex int, newIndex int) {
	// fmt.Printf("Server %v (Term %v) is preparing to submit entires to the applyChan, current log is %v \n", rf.getID(), rf.getCurrentTerm(), rf.log)
	rf.mu.Lock()
	defer rf.mu.Unlock()
	for i := oldIndex + 1; i <= newIndex; i++ {
		entry := rf.log[i]
		message := ApplyMsg {
			CommandValid: true,
			Command: entry.Command,
			CommandIndex: i,
		}
		// fmt.Printf("Server %v (Term %v) has submitted {%v, %v} to its applyChan \n", rf.me, rf.currentTerm, entry.Command, i)
		rf.applyChan <- message
		rf.lastApplied = i
	}
}

// Sends a RequestVote RPC to the specified server
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RPCReply) bool {
	// fmt.Printf("Server %v (Term %v) is sending RequestVote %v to Server %v \n", rf.getID(), rf.getCurrentTerm(), args, server)
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	rf.voteChan <- reply
	return ok
}

// AppendEntries RPC Handler
func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *RPCReply) {
	// Branching logic for AppendEntries response
	switch {
	case (args.Term < rf.getCurrentTerm()):
		// RPC came from outdated leader
		reply.Success = false
	case (args.PrevLogTerm != rf.getEntry(args.PrevLogIndex).Term):
		// RPC has wrong PrevLogTerm
		// fmt.Printf("Server %v (Term %v) is rejecting an RPC from Server %v (Term %v) due to log mismatch \n", rf.getID(), rf.getCurrentTerm(), args.LeaderId, args.Term)
		rf.setLastBeat()
		reply.Success = false
		reply.LastCommit = rf.getCommitIndex()
	default:
		// Valid AppendEntries
		rf.setLastBeat()
		reply.Success = true
		rf.setCurrentTerm(args.Term)
		if rf.getRole() != 0 {
			rf.transitionRole(0)
		}
		if (len(args.Entries) != 0) {
			// AppendEntries is not a heartbeat, contains new entries to append
			// fmt.Printf("Server %v (Term %v, Role %v, CI %v) is updating its log. Old log ending: %v \n", rf.getID(), rf.getCurrentTerm(), rf.getRole(), rf.getCommitIndex(), rf.log[len(rf.log) - 1])
			rf.addLogIndex(args.Entries, (args.PrevLogIndex + 1))
			// fmt.Printf("Server %v (Term %v, Role %v, CI %v) has updated its log. New log ending: %v \n", rf.getID(), rf.getCurrentTerm(), rf.getRole(), rf.getCommitIndex(), rf.log[len(rf.log) - 1])	
		}		
		if (args.LeaderCommit > rf.getCommitIndex()) {
			// Further entries have been commited
			oldCommitIndex := rf.getCommitIndex()
			rf.setCommitIndex(min(args.LeaderCommit, rf.getLastLogIndex()))
			go rf.applyEntries(oldCommitIndex, rf.getCommitIndex())
		}
	}
	reply.Term = rf.getCurrentTerm()
}

// Sends an AppendEntries RPC to the specified server
func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *RPCReply) bool {
	if (len(args.Entries) != 0) {
		// fmt.Printf("Server %v (Term %v) is sending log entries starting from %v and ending in %v to server %v \n", rf.getID(), rf.getCurrentTerm(), args.Entries[0], args.Entries[len(args.Entries) - 1], server)
	} else {
		// fmt.Printf("Server %v (Term %v) is sending %v to Server %v \n", rf.getID(), rf.getCurrentTerm(), args, server)
	}
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	result := AppendResult {
		args: args,
		reply: reply,
		server: server,
	}
	rf.leaderChan <- result
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
	// Your code here (2B).
	// One comprehensive lock needed here because clients can submit commands concurrently
	rf.mu.Lock()
	index := len(rf.log)
	term := rf.currentTerm
	isLeader := (rf.role == 2)

	if isLeader {
		// fmt.Printf("New command received: %v, expected at index %v \n", command, index)
		rf.nextIndex[rf.me]++
		rf.matchIndex[rf.me]++
		// fmt.Printf("Server %v (Term %v, Role %v) is updating its log. Old log ending: %v \n", rf.me, rf.currentTerm, rf.role, rf.log[len(rf.log) - 1])
		rf.log = append(rf.log, Entry{Command: command,Term: rf.currentTerm,})
		rf.persist()
		// fmt.Printf("Server %v (Term %v, Role %v) has updated its log. New log ending: %v \n", rf.me, rf.currentTerm, rf.role, rf.log[len(rf.log) - 1])
	}
	rf.mu.Unlock()
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

// The ticker go routine starts a new election if this peer hasn't received
// heartsbeats recently.
func (rf *Raft) ticker() {
	for rf.killed() == false {
		rand.Seed(time.Now().UnixNano())
		timeout := time.Millisecond * time.Duration(rand.Intn(500) + 500)
		time.Sleep(timeout)
		lastBeat := rf.getLastBeat()
		if time.Now().After(lastBeat.Add(timeout)) {
			rf.setLastBeat()
			rf.timeoutChan <- 1
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
	// Initialize all elements of the Raft struct as default values
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me
	rf.currentTerm = 0
	rf.votedFor = -1
	rf.log = []Entry{{nil, -1}}
	rf.commitIndex = 0
	rf.lastApplied = 0
	rf.nextIndex = []int{}
	rf.matchIndex = []int{}
	rf.role = 0
	rf.lastBeat = time.Now()
	rf.ballotBox = [2]int{}
	rf.voteChan = make(chan *RPCReply)
	rf.leaderChan = make(chan AppendResult)
	rf.timeoutChan = make(chan int)
	rf.applyChan = applyCh
	dummyMsg := ApplyMsg {
		CommandValid: true,
		Command: -1,
		CommandIndex: 0,
	}
	rf.applyChan <- dummyMsg

	// Initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// Start ticker goroutine to start elections
	go rf.ticker()

	// Start main loop process
	go rf.main()

	return rf
}

// Function for sending RequestVote RPC's to all servers
func (rf *Raft) holdElection() {	
	rf.setCurrentTerm(rf.getCurrentTerm() + 1)
	rf.setLastBeat()
	// Initialize RequestVoteArgs
	args := RequestVoteArgs {
		Term: rf.getCurrentTerm(),
		CandidateId: rf.getID(),
		LastLogIndex: rf.getLastLogIndex(),
		LastLogTerm: rf.getEntry(rf.getLastLogIndex()).Term,
	}

	// Iterates through all servers and sends RequestVote RPC's
	for Id, _ := range rf.getPeers() {
		if Id != rf.getID() {
			// Send RequestVote RPC to the server
			reply := RPCReply{}
			go rf.sendRequestVote(Id, &args, &reply)
		} else {
			// Always vote for yourself
			ownVote := RPCReply {
				Term: rf.getCurrentTerm(),
				Success: true,
			}
			rf.setVotedFor(rf.getID())
			rf.voteChan <- &ownVote
		}
	}
}

// Send an set of entries to a server
func (rf *Raft) sendEntries(server int, entryList []Entry) {
	// Reset heartbeat
	rf.lastBeat = time.Now()

	// Set args values
	args := AppendEntriesArgs {
		Term: rf.currentTerm,
		LeaderId: rf.me,
		PrevLogIndex: rf.nextIndex[server] - 1,
		PrevLogTerm: rf.log[rf.nextIndex[server] - 1].Term,
		Entries: entryList,
		LeaderCommit: rf.commitIndex,
	}

	// Initialize reply
	reply := RPCReply{}

	// Send
	go rf.sendAppendEntries(server, &args, &reply)
}

// Function that sends empty AppendEntries RPC's to all servers to maintain leadership
func (rf *Raft) sendHeartbeats() {
	// Reset heartbeat
	rf.setLastBeat()

	// Iterates through all servers and sends AppendEntries RPC's
	for Id, _ := range rf.getPeers() {
		if (Id != rf.getID()) && (rf.getLastLogIndex() < rf.getNextIndex(Id)) {
			// Send RequestVote RPC to the server
			// Initialize AppendEntriesArgs
			args := AppendEntriesArgs {
				Term: rf.getCurrentTerm(),
				LeaderId: rf.getID(),
				PrevLogIndex: (rf.getNextIndex(Id) - 1),
				PrevLogTerm: (rf.getEntry(rf.getNextIndex(Id) - 1).Term),
				Entries: []Entry{},
				LeaderCommit: rf.getCommitIndex(),
			}
			reply := RPCReply{}
			go rf.sendAppendEntries(Id, &args, &reply)
		}
	}
}

// Increments the commit index if valid
func (rf *Raft) updateCommit() {
	// Start with highest possible N, decrementing from there
	for N := rf.getLastLogIndex(); N > rf.getCommitIndex(); N-- {
		numAdvanced := 0
		for Id, _ := range rf.getPeers() {
			if rf.getMatchIndex(Id) >= N {
				numAdvanced++
			}
		}
		if ((rf.getEntry(N).Term != rf.getCurrentTerm()) && numAdvanced > (len(rf.getPeers()))/2) {
			// fmt.Printf("Server %v: Higher majority matchIndex entry found (%v), but it did not make it to a majority in this term \n", rf.getID(), N)
		}
		if ((rf.getEntry(N).Term == rf.getCurrentTerm()) && numAdvanced > (len(rf.getPeers()))/2) {
			for i := rf.getCommitIndex() + 1; i <= N; i++ {
				rf.setCommitIndex(N)
				entry := rf.getLog()[i]
				message := ApplyMsg {
					CommandValid: true,
					Command: entry.Command,
					CommandIndex: i,
				}
				rf.applyChan <- message
				// fmt.Printf("Server %v (Term %v) has submitted {%v, %v} to its applyChan \n", rf.getID(), rf.getCurrentTerm(), message, i)
				rf.setLastApplied(i)
			}
		}
	}
	
}

// Function for processing a RequestVote reply and determining election results
func (rf *Raft) processVote(reply *RPCReply) {
	// fmt.Printf("Server %v (Term %v) is processing vote %v \n", rf.getID(), rf.getCurrentTerm(), reply)
	if reply.Term > rf.getCurrentTerm() {
		// Server is outdated, retry as up-to-date
		rf.setCurrentTerm(reply.Term)
		rf.transitionRole(1)
	}
	
	// Prevents leftover votes from past elections from getting counted
	if (reply.Term >= rf.getCurrentTerm()) && (reply.Success) {
		rf.addVote(reply.Success) // Adds the vote to the ballot box
	}

	// Values to simplify election arithmetic
	numYes := rf.getBallotBox()[0]
	numVotes := rf.getBallotBox()[1]
	numNo := numVotes - numYes
	numServers := len(rf.getPeers())

	// Logic for determining if election is won, lost, or undecided
	switch {
	case (numYes > (numServers/2)):
		// Majority received
		rf.transitionRole(2)
		rf.sendHeartbeats()
	case (numVotes == numServers) || (numNo > (numServers/2)):
		// Election is over, server has lost
		if numYes == 0 {
			time.Sleep(time.Millisecond * 250)
			rf.setLastBeat()
		}
		rf.transitionRole(0)
	}
}

// Function for processing an AppendReesult and adjusting nextIndex/matchIndex accordingly
func (rf *Raft) processAppendResult(result AppendResult) {
	// Unpack AppendResult struct for convenience/readability 
	args := result.args
	reply := result.reply
	server := result.server
	if (len(args.Entries) != 0) {
		// fmt.Printf("Server %v (Term %v) is processing %v, %v, %v \n", rf.getID(), rf.getCurrentTerm(), args.Entries[len(args.Entries) - 1], reply, server)
	}

	if (reply.Term > rf.getCurrentTerm()) {
		// Leader is outdated, convert to follower
		rf.setCurrentTerm(reply.Term)
		rf.transitionRole(0)
	} else if (reply.Success) {
		// Update nextIndex and matchIndex
		if (len(args.Entries) != 0) {
			// fmt.Printf("Server %v's nextIndex updating, currently %v \n", rf.getID(), rf.nextIndex)
			newIndex := (args.PrevLogIndex + 1) + len(args.Entries)
			rf.setNextIndex(server, newIndex)
			// fmt.Printf("Server %v's nextIndex updated, currently %v \n", rf.getID(), rf.nextIndex)
			rf.setMatchIndex(server, newIndex - 1)
			// fmt.Printf("Server %v's matchIndex updated, currently %v \n", rf.getID(), rf.matchIndex)
			rf.updateCommit()
		}
	} else {
		// If reply.Term == 0, the server in question is likely just offline
		if (reply.Term != 0) {
			// Failure due to log inconsistency
			rf.setNextIndex(server, reply.LastCommit + 1)
		}

	}
}

// Loop for leaders to run, constantly checking if followers are up to date 
func (rf *Raft) updateFollowers() {
	for (rf.killed() == false) && (rf.getRole() == 2) {
		rf.mu.Lock()
		waitLong := false
		for Id, _ := range rf.peers {
			// Iterate through all servers
			// fmt.Printf("Server %v (Term %v) is fetching a nextIndex value at index %v, current nextIndex is %v\n", rf.me, rf.currentTerm, Id, rf.nextIndex)
			if (len(rf.nextIndex) != 0) {
				if (len(rf.log) - 1) >= rf.nextIndex[Id] {
					// If the leader's log is more up-to-date, send missing log entries
					rf.sendEntries(Id, rf.log[rf.nextIndex[Id]:])
					waitLong = true
				}	
			}
		}
		rf.mu.Unlock()
		if waitLong {
			// Wait longer if you sent out an RPC to avoid bombarding followers
			time.Sleep(time.Millisecond * 100)
		} else {
			time.Sleep(time.Millisecond * 15)
		}
	}
}

// Function to call when transitioning the state of a server
func (rf *Raft) transitionRole(newRole int) {
	// Reset role-specific values to defaults and empty channels
	rf.resetServer()

	// Transition to desired role
	switch newRole {
	case 0:
		// newRole is follower
		// fmt.Printf("Server %v (Term %v) is transition to follower with log ending %v \n", rf.getID(), rf.getCurrentTerm(), rf.log[len(rf.log) - 1])
		rf.setRole(0)
	case 1:
		// newRole is candidate
		// fmt.Printf("Server %v (Term %v) is transition to candidate with log ending %v \n", rf.getID(), rf.getCurrentTerm(), rf.log[len(rf.log) - 1])
		rf.setRole(1)
		rf.setBallotBox([2]int{0, 0})
		go rf.holdElection()
	case 2:
		// newRole is leader
		// fmt.Printf("Server %v (Term %v) is transition to leader with log ending %v \n", rf.getID(), rf.getCurrentTerm(), rf.log[len(rf.log) - 1])
		rf.setRole(2)
		// Initialize leader state values
		for i := 0; i < len(rf.getPeers()); i++ {
			rf.appendNextIndex(rf.getLastLogIndex() + 1)
			rf.appendMatchIndex(0)
		}
		rf.setMatchIndex(rf.getID(), rf.getLastLogIndex())
		// Start goroutine for keepUp loop
		go rf.updateFollowers()
	}
}

// Main logic loop for Raft
func (rf *Raft) main() {
	for (rf.killed() == false) {
		switch rf.getRole() {
		case 0:
			// Server is follower
			select {
			case <- rf.timeoutChan:
				// ticker() has expired, become candidate
				rf.transitionRole(1)
			}
		case 1:  
			// Server is candidate
			select {
			case vote := <- rf.voteChan:
				// RequestVote Reply received
				rf.processVote(vote)
			case <- rf.timeoutChan:
				// ticker() has expired, restart election
				rf.transitionRole(1)
			}
		case 2:
			// Server is leader
			select {
			case result := <- rf.leaderChan:
				// Received AppendEntry reply
				rf.processAppendResult(result)				
			case <- rf.timeoutChan:
				// Leaders do not care about timeout, just empty the channel
			default:
				// No new commands, send heartbeats
				rf.sendHeartbeats()
				time.Sleep(time.Millisecond * 125)
			}
		}
	}
}

// Current issue: log entries being appended by leader at term 1, election triggering, RPC's sent out but now 
// cannot be committed because the entries are not sent out in the terms they are applied
