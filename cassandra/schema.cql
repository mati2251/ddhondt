CREATE KEYSPACE elections
WITH replication = {
  'class': 'SimpleStrategy',
  'replication_factor': 3
};

CREATE TABLE elections.votes (
    district_id int,
    party_id int,
    candidate_id int,
    votes counter,
    PRIMARY KEY ((district_id), party_id, candidate_id)
);

