package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"strconv"

	gocql "github.com/apache/cassandra-gocql-driver/v2"
)

const (
	voteThreshold = 5
)

var (
	voteQuery   *gocql.Query
	resultQuery *gocql.Query
)

type Candidate struct {
	CandidateID int    `json:"candidate_id"`
	Name        string `json:"name"`
}

type Party struct {
	PartyID    int         `json:"party_id"`
	PartyName  string      `json:"party_name"`
	Candidates []Candidate `json:"candidates"`
}

type District struct {
	DistrictID int     `json:"district_id"`
	Name       string  `json:"name"`
	Mandates   int     `json:"mandates"`
	Parties    []Party `json:"parties"`
}

type Election struct {
	ElectionID string     `json:"election_id"`
	Districts  []District `json:"districts"`
}

type Identifiable interface {
	GetID() int
	GetName() string
}

func (d District) GetID() int      { return d.DistrictID }
func (d District) GetName() string { return d.Name }

func (p Party) GetID() int      { return p.PartyID }
func (p Party) GetName() string { return p.PartyName }

func (c Candidate) GetID() int      { return c.CandidateID }
func (c Candidate) GetName() string { return c.Name }

func parseArgToItem[T Identifiable](arg string, items []T) (T, int, bool) {
	var zero T
	id, err := strconv.Atoi(arg)
	if err == nil {
		for _, item := range items {
			if item.GetID() == id {
				return item, id, true
			}
		}
	} else {
		for _, item := range items {
			if item.GetName() == arg {
				return item, item.GetID(), true
			}
		}
	}

	return zero, -1, false
}

func initCassandraSession() (*gocql.Session, error) {
	cluster := gocql.NewCluster("172.28.0.10", "172.28.0.11", "172.28.0.12", "172.28.0.13")
	cluster.Keyspace = "elections"
	return cluster.CreateSession()
}

func vote(election Election) error {
	if flag.NArg() < 5 {
		return fmt.Errorf("not enough arguments for voting")
	}

	district, districtID, ok := parseArgToItem(flag.Arg(2), election.Districts)
	if !ok {
		fmt.Printf("Invalid district: %s\n", flag.Arg(2))
		os.Exit(1)
	}

	party, partyID, ok := parseArgToItem(flag.Arg(3), district.Parties)
	if !ok {
		fmt.Printf("Invalid party: %s\n", flag.Arg(3))
		os.Exit(1)
	}
	candidate, candidateID, ok := parseArgToItem(flag.Arg(4), party.Candidates)
	if !ok {
		fmt.Printf("Invalid candidate: %s\n", flag.Arg(4))
		os.Exit(1)
	}

	fmt.Printf("Voting for candidate %s (ID: %d) from party %s (ID: %d) in district %s (ID: %d)\n",
		candidate.Name, candidateID, party.PartyName, partyID, district.Name, districtID)

	session, err := initCassandraSession()
	if err != nil {
		fmt.Printf("Failed to connect to Cassandra: %v\n", err)
		os.Exit(1)
	}

	defer session.Close()

	initQuery(session)

	err = voteQuery.Bind(districtID, partyID, candidateID).Exec()
	if err != nil {
		return fmt.Errorf("failed to execute vote query: %w", err)
	}
	// TODO: check network partition fault here
	return nil
}

func initQuery(session *gocql.Session) {
	voteQuery = session.Query(`UPDATE votes SET votes = votes + 1 WHERE district_id = ? AND party_id = ? AND candidate_id = ?`)
	resultQuery = session.Query(`SELECT district_id, party_id, candidate_id, votes FROM votes WHERE district_id = ?`)
}

func main() {
	flag.Usage = func() {
		fmt.Fprintf(os.Stderr, "Usage: %s <election file> <cmd>\n", os.Args[0])
		fmt.Fprintf(os.Stderr, "Commands:\n")
		fmt.Fprintf(os.Stderr, "  vote <district> <party> <candidate>   Cast a vote\n")
		flag.PrintDefaults()
	}
	flag.Parse()
	filePath := flag.Arg(0)

	if filePath == "" || flag.NArg() < 3 {
		flag.Usage()
		os.Exit(1)
	}

	data, err := os.ReadFile(filePath)
	if err != nil {
		fmt.Printf("Error reading file: %v\n", err)
		os.Exit(1)
	}

	var election Election
	if err := json.Unmarshal(data, &election); err != nil {
		fmt.Printf("Invalid JSON: %v\n", err)
		os.Exit(1)
	}
	if len(election.Districts) == 0 {
		fmt.Println("Validation error: no districts found")
		os.Exit(1)
	}

	for _, d := range election.Districts {
		if len(d.Parties) == 0 {
			fmt.Printf("Validation error: district %d has no parties\n", d.DistrictID)
			os.Exit(1)
		}
		for _, p := range d.Parties {
			if len(p.Candidates) == 0 {
				fmt.Printf("Validation error: party %s in district %d has no candidates\n", p.PartyName, d.DistrictID)
				os.Exit(1)
			}
		}
	}

	cmd := flag.Arg(1)
	switch cmd {
	case "vote":
		if err := vote(election); err != nil {
			fmt.Printf("Error voting: %v\n", err)
			os.Exit(1)
		}
	default:
		flag.Usage()
	}
}
