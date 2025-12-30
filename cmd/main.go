package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"os"
)

func main() {
	flag.Usage = func() {
		fmt.Fprintf(os.Stderr, "Usage: %s <election file> <cmd>\n", os.Args[0])
		fmt.Fprintf(os.Stderr, "Commands:\n")
		fmt.Fprintf(os.Stderr, "  vote <district> <party> <candidate>   	Cast a vote\n")
		fmt.Fprintf(os.Stderr, "  vote-load <time in seconds> <votes count> 	Generate vote load\n")
		flag.PrintDefaults()
	}
	flag.Parse()
	filePath := flag.Arg(0)

	if filePath == "" || flag.NArg() < 2 {
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

	session, err := initCassandraSession()
	if err != nil {
		fmt.Printf("Failed to connect to Cassandra: %v\n", err)
		os.Exit(1)
	}

	defer session.Close()

	initQuery(session)

	cmd := flag.Arg(1)
	switch cmd {
	case "vote":
		if err := vote(election); err != nil {
			fmt.Printf("Error voting: %v\n", err)
			os.Exit(1)
		}
	case "vote-load":
		if err := voteLoad(election); err != nil {
			fmt.Printf("Error generating vote load: %v\n", err)
			os.Exit(1)
		}
	case "clear":
		if err := clearVotes(); err != nil {
			fmt.Printf("Error clearing votes: %v\n", err)
		}
	case "results":
		if err := results(election); err != nil {
			fmt.Printf("Error calculating results: %v\n", err)
		}
	default:
		flag.Usage()
	}
}
