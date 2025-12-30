package main

import (
	"fmt"

	gocql "github.com/apache/cassandra-gocql-driver/v2"
)

var (
	voteQuery   *gocql.Query
	resultQuery *gocql.Query
	clearQuery  *gocql.Query
)

func initCassandraSession() (*gocql.Session, error) {
	cluster := gocql.NewCluster("172.28.0.10", "172.28.0.11", "172.28.0.12", "172.28.0.13")
	cluster.Keyspace = "elections"
	return cluster.CreateSession()
}

func initQuery(session *gocql.Session) {
	voteQuery = session.Query(`UPDATE votes SET votes = votes + 1 WHERE district_id = ? AND party_id = ? AND candidate_id = ?`)
	resultQuery = session.Query(`SELECT district_id, party_id, candidate_id, votes FROM votes WHERE district_id = ?`)
	clearQuery = session.Query(`TRUNCATE votes`).Consistency(gocql.All)
}

func castVote(districtID, partyID, candidateID int) error {
	err := voteQuery.Bind(districtID, partyID, candidateID).Consistency(gocql.Three).Exec()
	if err != nil {
		return fmt.Errorf("failed to execute vote query: %w", err)
	}
	// TODO: check network partition fault here
	return nil
}

func clearVotes() error {
	return clearQuery.Exec()
}
