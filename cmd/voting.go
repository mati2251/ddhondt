package main

import (
	"flag"
	"fmt"
	"math/rand"
	"os"
	"strconv"
	"sync"
	"time"
)

const (
	voteThreshold = 5
	workers       = 10
)

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

	err := castVote(districtID, partyID, candidateID)
	if err != nil {
		return fmt.Errorf("failed to cast vote: %w", err)
	}

	return nil
}

func voteLoad(election Election) error {

	if flag.NArg() < 4 {
		return fmt.Errorf("not enough arguments for vote load")
	}

	timeArg := flag.Arg(2)
	votesArg := flag.Arg(3)

	durationSeconds, err := strconv.Atoi(timeArg)
	if err != nil {
		return fmt.Errorf("invalid duration: %w", err)
	}

	votesCount, err := strconv.Atoi(votesArg)
	if err != nil {
		return fmt.Errorf("invalid votes count: %w", err)
	}

	if durationSeconds <= 0 || votesCount <= 0 {
		return fmt.Errorf("duration and votes count must be positive integers")
	}

	votesChan := make(chan struct{})

	wg := sync.WaitGroup{}

	for range workers {
		go heavyVoter(election, votesChan, &wg)
	}

	interval := time.Duration(durationSeconds) * time.Second / time.Duration(votesCount)
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	voutesCount := 0
	for range votesCount {
		<-ticker.C
		voutesCount += 1
		votesChan <- struct{}{}
	}

	fmt.Printf("Submitted %d votes in %d seconds\n", voutesCount, durationSeconds)
	close(votesChan)
	wg.Wait()
	return nil
}

func heavyVoter(election Election, votesChan <-chan struct{}, wg *sync.WaitGroup) {
	wg.Add(1)
	source := rand.NewSource(time.Now().UnixNano())
	rng := rand.New(source)

	for range votesChan {
		d := election.Districts[rng.Intn(len(election.Districts))]
		p := d.Parties[rng.Intn(len(d.Parties))]
		c := p.Candidates[rng.Intn(len(p.Candidates))]

		err := voteQuery.Bind(
			d.DistrictID,
			p.PartyID,
			c.CandidateID,
		).Exec()

		if err != nil {
			fmt.Printf("vote failed (district=%d party=%d candidate=%d): %v\n",
				d.DistrictID, p.PartyID, c.CandidateID, err)
			break
		}
	}
	wg.Done()
}

