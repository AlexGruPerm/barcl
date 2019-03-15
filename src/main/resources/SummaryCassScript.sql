---------------------------------------------------
--
-- MTS_SRC only 3 tables: Source ticks data.
--
---------------------------------------------------

DROP TABLE mts_src.ticks;
DROP TABLE mts_src.ticks_count_days;
DROP TABLE mts_src.ticks_count_total;

CREATE TABLE mts_src.ticks(
	ticker_id int,
	ddate date,
	ts bigint,
	db_tsunx bigint,
	ask double,
	bid double,
	PRIMARY KEY (( ticker_id, ddate ), ts, db_tsunx)
) WITH CLUSTERING ORDER BY ( ts DESC, db_tsunx DESC );

CREATE TABLE mts_src.ticks_count_days(
	ticker_id int,
	ddate date,
	ticks_count counter,
	PRIMARY KEY (ticker_id, ddate)
) WITH CLUSTERING ORDER BY ( ddate DESC );

CREATE TABLE mts_src.ticks_count_total(
	ticker_id int,
	ticks_count counter,
	PRIMARY KEY (ticker_id)
);

---------------------------------------------------
--
-- MTS_BARS: Contains calculated bars.
--
---------------------------------------------------

DROP TABLE mts_bars.bars;

CREATE TABLE mts_bars.bars(
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