package vraft;

import vfd.IP;
import vraft.msg.Log;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

// https://www.sofastack.tech/en/projects/sofa-jraft/raft-introduction/
/*
 * All Servers:
 * If commitIndex > lastApplied, increment lastApplied, and apply log[lastApplied] to state machine.
 * If the RPC request or response contains term T > currentTerm, set currentTerm to T and transit into a follower.
 * Follower
 * Responds to RPCs from candidates and the leader.
 * If the election timeout elapses, and the follower fails to receive any AppendEntries RPCs from the current leader or any RequestVote RPCs from any candidate, the follower transits into a candidate.
 * Candidate
 * Starts election after transiting into a candidate:
 * Increment currentTerm > Reset the election timer > Vote for itself > Send RequestVote RPCs to all other servers.
 * If the candidate receives votes from a majority of servers, it becomes the leader.
 * If a candidate receives an AppendEntries RPC from the new leader, it transits into a follower.
 * If the election timeout elapses, it starts a new election.
 * Leader
 * Upon election, the leader sends empty AppendEntries RPCs (heartbeat) to each server, and repeats the step during idle periods to prevent the election from timeing out.
 * If the leader receives a command from a client, it appends an entry to the local log and sends AppendEntries RPCs to all servers. After receiving responses from a majority of the servers, it applies the entry to the state machine and replies responses to the clients.
 * If last log index >= nextIndex for a follower, the leader sends an AppendEntries RPC with log entries starting from the nextIndex. If it is successful, the leader updates the follower’s nextIndex and matcheIndex. If AppendEntries fails because of log inconsistency, the leader decrements the nextIndex and resends the AppendEntries RPC to the follower.
 * If there is an N that N > commitIndex, a majority of matchIndex[i] >= N, and log]N[.term == currentTerm, the leader sets commitIndex to N.
 */

public class Storage {
    private final String id;
    private ServerRole role;

    // Persistent state on all servers (updated on stable storage before responding to RPCs)
    private int currentTerm = 0; // The latest term that the server gets (initialized to 0 on initial boot, increasing monotonically)
    private ServerRef votedFor = null; // The candidateId that has received votes in the current term (or null if none).
    private final List<Log> logs = new ArrayList<>(1024); // Log entries. Each entry contains a command for the state machine, and the term when the entry was received by the leader.

    // Volatile state on all servers
    private int commitIndex = -1; // The index of the highest log entry known to be committed.
    private int lastApplied = -1; // The index of the highest log entry applied to the state machine.

    private final LeaderFields leaderFields = new LeaderFields();

    static class LeaderFields {
        // Volatile state on leaders
        final Map<ServerRef, Integer> nextIndexes = new HashMap<>(); // The index of the next log entry to be sent to each follower.
        final Map<ServerRef, Integer> matchIndexes = new HashMap<>(); // The index of the highest log entry known to have been replicated on each follower.
    }

    public Storage(IP ip, ServerRole initialRole) {
        this.id = ip.formatToIPString();
        this.role = initialRole;
    }
}
