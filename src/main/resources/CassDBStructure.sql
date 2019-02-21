drop TABLE mts_bars.lastbars;

-------------------------------------------------------------------------------
--
-- Small table, contains partitions count equal distinct tickers_id.
-- and for 1 year there is about 365*5 ~ 1500 rows (1 year, 5 distinct widths)
--
-------------------------------------------------------------------------------
drop TABLE mts_bars.last_bars;

CREATE TABLE mts_bars.last_bars (
	ticker_id int,
	bar_width_sec int,
	ddate date,
	ts_end bigint,
	PRIMARY KEY ((ticker_id), bar_width_sec, ddate)
) WITH  CLUSTERING ORDER BY ( bar_width_sec ASC, ddate DESC)
and comment = 'Contains only last bars (ts_end) for each key; ticker_id,bar_width_sec,ddate ';

insert into mts_bars.last_bars(ticker_id,bar_width_sec,ddate,ts_end) values(1,30,'2018-09-19',1234567);
insert into mts_bars.last_bars(ticker_id,bar_width_sec,ddate,ts_end) values(1,30,'2018-09-19',1234678);

select * from mts_bars.last_bars -- contains ts_end 1234678 because it's "upsert" and key: 1,30,'2018-09-19' already exists.