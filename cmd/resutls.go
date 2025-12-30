package main

import "fmt"

func results(e Election) error {
	districtIdToIndex := make(map[int]int)
	partyIdToIndex := make(map[int]int)
	for i, d := range e.Districts {
		districtIdToIndex[d.DistrictID] = i
		if i == 0 {
			for j, p := range d.Parties {
				partyIdToIndex[p.PartyID] = j
			}
		}
	}

	for _, d := range e.Districts {
		result := resultQuery.Bind(d.DistrictID).Iter()
		candidateIdToIndex := make(map[int]map[int]int)
		for _, p := range d.Parties {
			candidateIdToIndex[p.PartyID] = make(map[int]int)
			for j, c := range p.Candidates {
				candidateIdToIndex[p.PartyID][c.CandidateID] = j
			}
		}
		var districtID, partyID, candidateID, votes int
		for result.Scan(&districtID, &partyID, &candidateID, &votes) {
			pIndex := partyIdToIndex[partyID]
			cIndex := candidateIdToIndex[partyID][candidateID]
			e.Districts[districtIdToIndex[districtID]].Parties[pIndex].Candidates[cIndex].Votes = votes
		}
	}

	perPartyTotals := make(map[int]int)
	for _, d := range e.Districts {
		for _, p := range d.Parties {
			for _, c := range p.Candidates {
				perPartyTotals[p.PartyID] += c.Votes
			}
		}
	}

	totalVotes := 0
	for _, v := range perPartyTotals {
		totalVotes += v
	}

	passThresholdVotes := totalVotes * voteThreshold / 100
	isPassed := make(map[int]bool)

	fmt.Printf("Total Votes: %d\n", totalVotes)
	fmt.Println("Party Results:")
	for partyID, votes := range perPartyTotals {
		percentage := (float64(votes) / float64(totalVotes)) * 100
		fmt.Printf("Party: %d, Votes: %d, Percentage: %.2f%%\n", partyID, votes, percentage)
		if votes >= passThresholdVotes {
			isPassed[partyID] = true
		} else {
			isPassed[partyID] = false
			fmt.Printf("Party ID %d did not pass the threshold with %d votes\n", partyID, votes)
		}
	}

	return nil
}

