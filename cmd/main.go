package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"strconv"

	gocql "github.com/apache/cassandra-gocql-driver/v2"
)

var voteQuery *gocql.Query

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
	cluster := gocql.NewCluster("127.0.0.1")
	cluster.Keyspace = "elections"
	return cluster.CreateSession()
}

func vote(districtID string, partyID int, candidateID int) error {
	err := voteQuery.Bind(districtID, partyID, candidateID).Exec()
	if err != nil {
		return fmt.Errorf("failed to execute vote query: %w", err)
	}
	// TODO: check network partition fault here
	return nil
}

func initialQuery(session *gocql.Session) {
	voteQuery = session.Query(`UPDATE votes SET vote_count = vote_count + 1 WHERE district_id = ? AND party_id = ? AND candidate_id = ?`)
}

func main() {
	filePath := flag.String("f", "", "Path to election lists json file")
	flag.Usage = func() {
		fmt.Fprintf(os.Stderr, "Usage: %s [options] <district id|string> <party id|string> <candidate id|string>\n", os.Args[0])
		flag.PrintDefaults()
	}
	flag.Parse()

	if *filePath == "" || flag.NArg() != 3 {
		flag.Usage()
		os.Exit(1)
	}

	data, err := os.ReadFile(*filePath)
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
			fmt.Printf("Validation error: district %s has no parties\n", d.DistrictID)
			os.Exit(1)
		}
		for _, p := range d.Parties {
			if len(p.Candidates) == 0 {
				fmt.Printf("Validation error: party %s in district %s has no candidates\n", p.PartyName, d.DistrictID)
				os.Exit(1)
			}
		}
	}

	district, districtID, ok := parseArgToItem(flag.Arg(0), election.Districts)
	if !ok {
		fmt.Printf("Invalid district: %s\n", flag.Arg(0))
		os.Exit(1)
	}

	party, partyID, ok := parseArgToItem(flag.Arg(1), district.Parties)
	if !ok {
		fmt.Printf("Invalid party: %s\n", flag.Arg(1))
		os.Exit(1)
	}
	candidate, candidateID, ok := parseArgToItem(flag.Arg(2), party.Candidates)
	if !ok {
		fmt.Printf("Invalid candidate: %s\n", flag.Arg(2))
		os.Exit(1)
	}

	fmt.Printf("Voting for candidate %s (ID: %d) from party %s (ID: %d) in district %s (ID: %d)\n",
		candidate.Name, candidateID, party.PartyName, partyID, district.Name, districtID)
}
