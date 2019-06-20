
--drop old CF

drop table mts_bars.bars;
drop table mts_bars.last_bars;
drop table mts_bars.bars_future;
drop table mts_bars.bars_test;
drop table mts_bars.pattern_search_results;
drop table mts_bars.td_bars_3600;
drop table mts_bars.trade_advisers_results_bars;

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

CREATE TABLE mts_bars.bars_fa(
	ticker_id int,
	ddate date,
	bar_width_sec int,
	log_oe double,
	ts_end bigint,
	c double,
	c_res double,
	ddate_res date,
	dursec_res int,
	res_type text,
	ts_end_res bigint,
	PRIMARY KEY (( ticker_id, ddate, bar_width_sec ), log_oe, ts_end)
) WITH CLUSTERING ORDER BY ( log_oe ASC, ts_end DESC );

CREATE TABLE mts_bars.bars_forms(
	ticker_id int,
	ddate date,
	bar_width_sec int,
	ts_begin bigint,
	ts_end bigint,
	log_oe double,
	res_type text,
	formdeepkoef int,
	formprops MAP<text, text>,
	PRIMARY KEY (( ticker_id, ddate, bar_width_sec ), ts_begin, ts_end, log_oe, res_type)
) WITH CLUSTERING ORDER BY ( ts_begin DESC, ts_end DESC, log_oe ASC, res_type ASC );