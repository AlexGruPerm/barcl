drop TABLE mts_bars.lastbars;



----------------------------------------------------------------------------------------------------------------------
--
-- Small table, which widths in seconds we need calculate for each tickers.
--
----------------------------------------------------------------------------------------------------------------------
drop table mts_meta.bars_property;

CREATE TABLE mts_meta.bars_property(
	ticker_id int,
	bar_width_sec int,
	is_enabled int,
	PRIMARY KEY (ticker_id, bar_width_sec, is_enabled)
) WITH  comment = 'All bar property for calculations - bar_width_sec';

insert into  mts_meta.bars_property(ticker_id,bar_width_sec,is_enabled) values(1,300,1);
insert into  mts_meta.bars_property(ticker_id,bar_width_sec,is_enabled) values(2,300,1);
insert into  mts_meta.bars_property(ticker_id,bar_width_sec,is_enabled) values(3,300,1)
insert into  mts_meta.bars_property(ticker_id,bar_width_sec,is_enabled) values(1,30,0);--

----------------------------------------------------------------------------------------------------------------------
--
-- Small table, contains partitions count equal distinct tickers_id (~20) * (10) diff bars widths =200
-- and for 1 year there is about 365 rows in each partition
--
----------------------------------------------------------------------------------------------------------------------
drop TABLE mts_bars.last_bars;

CREATE TABLE mts_bars.last_bars (
	ticker_id int,
	bar_width_sec int,
	ddate date,
	ts_end bigint,
	PRIMARY KEY ((ticker_id,bar_width_sec), ddate)
) WITH  CLUSTERING ORDER BY (ddate DESC)
and comment = 'Contains only last bars (ts_end) for each key; ticker_id,bar_width_sec,ddate';

insert into mts_bars.last_bars(ticker_id,bar_width_sec,ddate,ts_end) values(1,300,'2019-02-08',1549662931724);
insert into mts_bars.last_bars(ticker_id,bar_width_sec,ddate,ts_end) values(1,300,'2019-02-08',1549662931725);
insert into mts_bars.last_bars(ticker_id,bar_width_sec,ddate,ts_end) values(2,300,'2019-02-19',1550613300701);

-- contains
-- ts_end 1549662848806 (2018-09-19)
-- ts_end 1549662848808 (2018-09-20)
--because it's "upsert" and key: (1,30,'2018-09-19') already exists.

-- Only real last bar, because CLUSTERING ORDER BY (ddate DESC)
select * from mts_bars.last_bars where ticker_id=1 and bar_width_sec=300 limit 1;
select * from mts_bars.last_bars where ticker_id=2 and bar_width_sec=300 limit 1;

-- eliminate no necessary columns
select ddate,ts_end from mts_bars.last_bars where ticker_id=1 and bar_width_sec=300 limit 1;

----------------------------------------------------------------------------------------------------------------------