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
insert into  mts_meta.bars_property(ticker_id,bar_width_sec,is_enabled) values(1,30,0);

----------------------------------------------------------------------------------------------------------------------
--
-- Small table, contains partitions count equal distinct tickers_id (~20) * (10) diff bars widths =200
-- and for 1 year there is about 365 rows in each partition
--
----------------------------------------------------------------------------------------------------------------------
drop TABLE mts_bars.last_bars;

-- orig
CREATE TABLE mts_bars.last_bars (
	ticker_id int,
	bar_width_sec int,
	ddate date,
	ts_end bigint,
	PRIMARY KEY ((ticker_id,bar_width_sec), ddate)
) WITH  CLUSTERING ORDER BY (ddate DESC)
and comment = 'Contains only last bars (ts_end) for each key; ticker_id,bar_width_sec,ddate';

--new for multiple empty bars inside one ddate
CREATE TABLE mts_bars.last_bars (
	ticker_id int,
	bar_width_sec int,
	ddate date,
	ts_end bigint,
	PRIMARY KEY ((ticker_id,bar_width_sec), ddate, ts_end)
) WITH  CLUSTERING ORDER BY (ddate DESC, ts_end DESC)
and comment = 'Contains only last bars (ts_end) for each key; ticker_id,bar_width_sec,ddate';



--new 2
CREATE TABLE mts_bars.last_bars (
	ticker_id int,
	bar_width_sec int,
	ddate_begin date,
	ddate_end   date,
	ts_begin bigint,
	ts_end bigint,
	PRIMARY KEY ((ticker_id,bar_width_sec), ddate_begin, ddate_end, ts_begin, ts_end)
) WITH  CLUSTERING ORDER BY (ddate_begin DESC, ddate_end DESC, ts_begin DESC, ts_end DESC)
and comment = 'Contains only last bars (ts_end) for each key; ticker_id,bar_width_sec,ddate';

--new 3
CREATE TABLE mts_bars.last_bars (
	ticker_id int,
	bar_width_sec int,
	ts_end bigint,
	PRIMARY KEY ((ticker_id,bar_width_sec) ,ts_end)
) WITH comment = 'Contains only last bars (ts_end) for each key; ticker_id,bar_width_sec';


--new 4
CREATE TABLE mts_bars.last_bars (
	ticker_id int,
	bar_width_sec int,
	ts_end bigint,
	PRIMARY KEY ((ticker_id,bar_width_sec ,ts_end))
) WITH comment = 'Contains only last bars (ts_end) for each key; ticker_id,bar_width_sec';


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


CREATE TABLE mts_src.ticks_count_total(
	ticker_id int,
	ticks_count counter,
	PRIMARY KEY (ticker_id)
);

CREATE TABLE mts_src.ticks_count_days(
	ticker_id   int,
	ddate       date,
	ticks_count counter,
	PRIMARY KEY ((ticker_id),ddate)
) WITH CLUSTERING ORDER BY (ddate DESC);

----------------------------------------------------------------------------------------------------------------------


CREATE TABLE mts_bars.bars (
	ticker_id int,
	ddate date,
	bar_width_sec int,
	ts_end bigint,
	btype text,
	c double,
	disp double,
	h double,
	h_body double,
	h_shad double,
	l double,
	log_co double,
	o double,
	ticks_cnt int,
	ts_begin bigint,
	PRIMARY KEY (( ticker_id, ddate, bar_width_sec ), ts_end)
) WITH CLUSTERING ORDER BY ( ts_end DESC );



select sum(ticks_count) from mts_src.ticks_count_total;
-- 13/02/2019  16 063 762
--             16 474 172
-- 14/02/2019  17 957 615
--
-- 18/02       22 971 918
-- 19/02       25 020 342
--             25 704 408
-- 20.02       27 682 500
--             30 054 245
-- 21.02       30 280 321
--             31 197 306
-- 25.02       35 366 895
-- 25.02       36 573 479
-- 26.02       37 680 108
--             38 546 802
-- 27.02       40 817 904
--             41 592 727





CREATE TABLE mts_bars.td_bars_3600(
  ticker_id int,
  ddate     date,
  bar_1 map<text,text>,
  bar_2 map<text,text>,
  bar_3 map<text,text>,
  bar_4 map<text,text>,
  bar_5 map<text,text>,
  bar_6 map<text,text>,
  bar_7 map<text,text>,
  bar_8 map<text,text>,
  bar_9 map<text,text>,
  bar_10 map<text,text>,
  bar_11 map<text,text>,
  bar_12 map<text,text>,
  bar_13 map<text,text>,
  bar_14 map<text,text>,
  bar_15 map<text,text>,
  bar_16 map<text,text>,
  bar_17 map<text,text>,
  bar_18 map<text,text>,
  bar_19 map<text,text>,
  bar_20 map<text,text>,
  bar_21 map<text,text>,
  bar_22 map<text,text>,
  bar_23 map<text,text>,
  bar_24 map<text,text>,
	PRIMARY KEY ((ticker_id), ddate)
) WITH CLUSTERING ORDER BY (ddate DESC);


insert into mts_bars.td_bars_3600(ticker_id,ddate,bar_1) values(1,'2019-01-25',
{
  'bar_width_sec' : '3659',
  'btype' : 'd',
  'c' : '1.67173',
  'disp' : '0.0017',
  'h' : '1.67184',
  'h_body' : '0.0013',
  'h_shad' : '0.0011',
  'l' : '1.67141',
  'log_co' : '0.023',
  'o' : '1.67156',
  'ticks_cnt' : '35746',
  'ts_begin' : '1548839178248',
  'ts_end' : '1548839188248'
}
);


insert into mts_bars.td_bars_3600(ticker_id,ddate,bar_1,bar_2) values(1,'2019-01-25',
{
  'bar_width_sec' : '3659',
  'btype' : 'd',
  'c' : '1.67173',
  'disp' : '0.0017',
  'h' : '1.67184',
  'h_body' : '0.0013',
  'h_shad' : '0.0011',
  'l' : '1.67141',
  'log_co' : '0.023',
  'o' : '1.67156',
  'ticks_cnt' : '35746',
  'ts_begin' : '1548839178248',
  'ts_end' : '1548839188248'
},
{
  'bar_width_sec' : '3659',
  'btype' : 'u',
  'c' : '1.67173',
  'disp' : '0.0017',
  'h' : '1.67184',
  'h_body' : '0.0013',
  'h_shad' : '0.0011',
  'l' : '1.67141',
  'log_co' : '0.023',
  'o' : '1.67156',
  'ticks_cnt' : '35746',
  'ts_begin' : '1548839178248',
  'ts_end' : '1548839188248'
}
);

insert into mts_bars.td_bars_3600(ticker_id,ddate,bar_1,bar_2) values(2,'2019-01-25',
{
  'bar_width_sec' : '3659',
  'btype' : 'd',
  'c' : '1.45',
  'disp' : '0.0017',
  'h' : '1.67184',
  'h_body' : '0.0013',
  'h_shad' : '0.0011',
  'l' : '1.67141',
  'log_co' : '0.023',
  'o' : '1.67156',
  'ticks_cnt' : '35746',
  'ts_begin' : '1548839178248',
  'ts_end' : '1548839188248'
},
{
  'bar_width_sec' : '3659',
  'btype' : 'd',
  'c' : '1.56',
  'disp' : '0.0017',
  'h' : '1.67184',
  'h_body' : '0.0013',
  'h_shad' : '0.0011',
  'l' : '1.67141',
  'log_co' : '0.023',
  'o' : '1.67156',
  'ticks_cnt' : '35746',
  'ts_begin' : '1548839178248',
  'ts_end' : '1548839188248'
}
);

insert into mts_bars.td_bars_3600(ticker_id,ddate,bar_1,bar_2,bar_3) values(3,'2019-01-25',
{
  'bar_width_sec' : '3659',
  'btype' : 'd',
  'c' : '1.45',
  'disp' : '0.0017',
  'h' : '1.67184',
  'h_body' : '0.0013',
  'h_shad' : '0.0011',
  'l' : '1.67141',
  'log_co' : '0.023',
  'o' : '1.67156',
  'ticks_cnt' : '35746',
  'ts_begin' : '1548839178248',
  'ts_end' : '1548839188248'
},
{
  'bar_width_sec' : '3659',
  'btype' : 'd',
  'c' : '1.56',
  'disp' : '0.0017',
  'h' : '1.67184',
  'h_body' : '0.0013',
  'h_shad' : '0.0011',
  'l' : '1.67141',
  'log_co' : '0.023',
  'o' : '1.67156',
  'ticks_cnt' : '35746',
  'ts_begin' : '1548839178248',
  'ts_end' : '1548839188248'
},
{
  'bar_width_sec' : '3659',
  'btype' : 'd',
  'c' : '1.56',
  'disp' : '0.0017',
  'h' : '1.67184',
  'h_body' : '0.0013',
  'h_shad' : '0.0011',
  'l' : '1.67141',
  'log_co' : '0.023',
  'o' : '1.67156',
  'ticks_cnt' : '35746',
  'ts_begin' : '1548839178248',
  'ts_end' : '1548839188248'
}
);

--==============================================================

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
insert into  mts_meta.bars_property(ticker_id,bar_width_sec,is_enabled) values(1,30,0);

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


--12.03.2019
DROP TABLE mts_bars.bars;

CREATE TABLE mts_bars.bars(
	ticker_id int,
	bar_width_sec int,
	ddate_begin date,
	ddate_end   date,
	ts_end bigint,
	btype text,
	c double,
	disp double,
	h double,
	h_body double,
	h_shad double,
	l double,
	log_co double,
	o double,
	ticks_cnt int,
	ts_begin bigint,
	PRIMARY KEY ((ticker_id, bar_width_sec), ddate_end, ts_end)
) WITH CLUSTERING ORDER BY (ddate_end DESC, ts_end DESC );














