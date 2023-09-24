// var mu sync.Mutex
// cond := sync.NewCond(&mu)
// votes := 0
// total := 0

// for server := 0; server < len(rf.peers); server++ {

// 	go func (server int) {
// 		rf.peers[server].Call("Raft.RequestVote", args, reply)

// 		mu.Lock()
// 		defer mu.Unlock()

// 		if reply.VoteGranted {
// 			votes++
// 		}
// 		total++
// 		cond.Broadcast()
// 	}(server)
// }

// mu.Lock()
// for votes < len(rf.peers) / 2 && total != len(rf.peers) {
// 	cond.Wait()
// }

// if votes >= len(rf.peers) {
// 	rf.state = LEADER
// } else {
// 	rf.state =
// }

// lock -> broadcast -> unlock

// lock -> conditional wait -> unlock
