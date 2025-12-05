CREATE SCHEMA Sports_DW;


CREATE TABLE IF NOT EXISTS Sports_DW.dim_nba (
	LeagueID INT PRIMARY KEY,
	Nickname TEXT NOT NULL,
	Conference TEXT NOT NULL,
	Division TEXT NOT NULL
);

SELECT * FROM Sports_DW.dim_geography

CREATE TABLE IF NOT EXISTS Sports_DW.dim_nfl (
	LeagueID INT PRIMARY KEY,
	Coach TEXT NOT NULL,
	Owner TEXT NOT NULL,
	Stadium TEXT NOT NULL,
	Established INT NOT NULL
);


CREATE TABLE IF NOT EXISTS Sports_DW.dim_mlb (
	LeagueID INT PRIMARY KEY,
	Conference TEXT NOT NULL,
	Division TEXT NOT NULL
);


CREATE TABLE IF NOT EXISTS Sports_DW.dim_geography (
	GeographyID INT PRIMARY KEY,
	Country TEXT NOT NULL,
	City TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS Sports_DW.fact_table (
	TeamID INT NOT NULL,
	League TEXT NOT NULL CHECK (League IN ('NBA', 'NFL', 'MLB')),
	LeagueID INT NOT NULL,
	GeographyID INT NOT NULL,
	Name TEXT NOT NULL,
	Code TEXT NOT NULL,
	Logo TEXT NOT NULL,
	PRIMARY KEY(League, LeagueID)
) PARTITION BY LIST (League);

ALTER TABLE Sports_DW.fact_table
  ADD CONSTRAINT fk_fact_geography
  FOREIGN KEY (GeographyID) REFERENCES Sports_DW.dim_geography(GeographyID);

CREATE TABLE IF NOT EXISTS Sports_DW.fact_table_nba
	PARTITION OF Sports_DW.fact_table FOR VALUES IN ('NBA')
ALTER TABLE Sports_DW.fact_table_nba
	ADD CONSTRAINT fk_fact_table_nba
	FOREIGN KEY (LeagueID) REFERENCES Sports_DW.dim_nba(LeagueID)
	
CREATE TABLE IF NOT EXISTS Sports_DW.fact_table_nfl
  	PARTITION OF Sports_DW.fact_table FOR VALUES IN ('NFL');
ALTER TABLE Sports_DW.fact_table_nfl
  	ADD CONSTRAINT fk_fact_table_nfl
  	FOREIGN KEY (LeagueID) REFERENCES Sports_DW.dim_nfl(LeagueID);


CREATE TABLE IF NOT EXISTS Sports_DW.fact_table_mlb
  	PARTITION OF Sports_DW.fact_table FOR VALUES IN ('MLB');
ALTER TABLE Sports_DW.fact_table_mlb
  	ADD CONSTRAINT fk_fact_table_mlb
  	FOREIGN KEY (LeagueID) REFERENCES Sports_DW.dim_mlb(LeagueID);

CREATE INDEX IF NOT EXISTS idx_fact_nba_leagueid ON Sports_DW.fact_table_nba (LeagueID);
CREATE INDEX IF NOT EXISTS idx_fact_nfl_leagueid ON Sports_DW.fact_table_nfl (LeagueID);
CREATE INDEX IF NOT EXISTS idx_fact_mlb_leagueid ON Sports_DW.fact_table_mlb (LeagueID);
CREATE INDEX IF NOT EXISTS idx_fact_geo         ON Sports_DW.fact_table     (GeographyID);

