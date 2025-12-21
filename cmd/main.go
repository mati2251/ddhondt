package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"os"
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
	DistrictID string  `json:"district_id"`
	Name       string  `json:"name"`
	Mandates   int     `json:"mandates"`
	Parties    []Party `json:"parties"`
}

type Election struct {
	ElectionID string     `json:"election_id"`
	Districts  []District `json:"districts"`
}

func main() {
	filePath := flag.String("f", "", "Path to election lists json file")
	flag.Parse()

	if *filePath == "" {
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
}
