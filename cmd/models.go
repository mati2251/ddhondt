package main

import "strconv"

type Candidate struct {
	CandidateID int    `json:"candidate_id"`
	Name        string `json:"name"`
	Votes       int    `json:",omitempty"`
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
