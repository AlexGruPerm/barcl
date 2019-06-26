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

---------------------------------------------------
--
-- MTS_META: Contains all tickers as dictionary.
--
---------------------------------------------------
CREATE TABLE mts_meta.tickers(
	ticker_id int,
	ticker_code text,
	ticker_first text,
	ticker_seconds text,
	PRIMARY KEY (( ticker_id, ticker_code, ticker_first ))
);

INSERT INTO tickers (ticker_id,ticker_code,ticker_first,ticker_seconds) VALUES (1,'EURUSD','EUR','USD');
INSERT INTO tickers (ticker_id,ticker_code,ticker_first,ticker_seconds) VALUES (2,'AUDUSD','AUD','USD');
INSERT INTO tickers (ticker_id,ticker_code,ticker_first,ticker_seconds) VALUES (3,'GBPUSD','GBP','USD');
INSERT INTO tickers (ticker_id,ticker_code,ticker_first,ticker_seconds) VALUES (4,'NZDUSD','NZD','USD');
INSERT INTO tickers (ticker_id,ticker_code,ticker_first,ticker_seconds) VALUES (5,'EURCHF','EUR','CHF');
INSERT INTO tickers (ticker_id,ticker_code,ticker_first,ticker_seconds) VALUES (6,'USDCAD','USD','CAD');
INSERT INTO tickers (ticker_id,ticker_code,ticker_first,ticker_seconds) VALUES (7,'USDCHF','USD','CHF');
INSERT INTO tickers (ticker_id,ticker_code,ticker_first,ticker_seconds) VALUES (8,'EURCAD','EUR','CAD');
INSERT INTO tickers (ticker_id,ticker_code,ticker_first,ticker_seconds) VALUES (9,'GBPAUD','GBP','AUD');
INSERT INTO tickers (ticker_id,ticker_code,ticker_first,ticker_seconds) VALUES (10,'GBPCAD','GBP','CAD');
INSERT INTO tickers (ticker_id,ticker_code,ticker_first,ticker_seconds) VALUES (11,'GBPCHF','GBP','CHF');
INSERT INTO tickers (ticker_id,ticker_code,ticker_first,ticker_seconds) VALUES (12,'EURGBP','EUR','GBP');
INSERT INTO tickers (ticker_id,ticker_code,ticker_first,ticker_seconds) VALUES (13,'GBPNZD','GBP','NZD');
INSERT INTO tickers (ticker_id,ticker_code,ticker_first,ticker_seconds) VALUES (14,'NZDCAD','NZD','CAD');
INSERT INTO tickers (ticker_id,ticker_code,ticker_first,ticker_seconds) VALUES (15,'AUDCAD','AUD','CAD');
INSERT INTO tickers (ticker_id,ticker_code,ticker_first,ticker_seconds) VALUES (16,'AUDCHF','AUD','CHF');
INSERT INTO tickers (ticker_id,ticker_code,ticker_first,ticker_seconds) VALUES (17,'AUDJPY','AUD','JPY');
INSERT INTO tickers (ticker_id,ticker_code,ticker_first,ticker_seconds) VALUES (18,'CADJPY','CAD','JPY');
INSERT INTO tickers (ticker_id,ticker_code,ticker_first,ticker_seconds) VALUES (19,'CHFJPY','CHF','JPY');
INSERT INTO tickers (ticker_id,ticker_code,ticker_first,ticker_seconds) VALUES (20,'EURNZD','EUR','NZD');
INSERT INTO tickers (ticker_id,ticker_code,ticker_first,ticker_seconds) VALUES (21,'EURJPY','EUR','JPY');
INSERT INTO tickers (ticker_id,ticker_code,ticker_first,ticker_seconds) VALUES (22,'USDJPY','USD','JPY');
INSERT INTO tickers (ticker_id,ticker_code,ticker_first,ticker_seconds) VALUES (23,'USDRUB','USD','RUB');
INSERT INTO tickers (ticker_id,ticker_code,ticker_first,ticker_seconds) VALUES (24,'GBPSGD','GBP','SGD');
INSERT INTO tickers (ticker_id,ticker_code,ticker_first,ticker_seconds) VALUES (25,'USDSGD','USD','SGD');
INSERT INTO tickers (ticker_id,ticker_code,ticker_first,ticker_seconds) VALUES (26,'XAUUSD','XAU','USD');
INSERT INTO tickers (ticker_id,ticker_code,ticker_first,ticker_seconds) VALUES (27,'BRN','BRN','XXX');
INSERT INTO tickers (ticker_id,ticker_code,ticker_first,ticker_seconds) VALUES (28,'_DXY','_DXY','XXX');

INSERT INTO mts_meta.tickers (ticker_id,ticker_code,ticker_first,ticker_seconds) VALUES (30,'RTS-6.19','RTS-6.19','RUB');
INSERT INTO mts_meta.tickers(ticker_id,ticker_code,ticker_first,ticker_seconds) VALUES (31,'SBRF-6.19','SBRF-6.19','RUB');
INSERT INTO mts_meta.tickers (ticker_id,ticker_code,ticker_first,ticker_seconds) VALUES (32,'LKOH-6.19','LKOH-6.19','RUB');
INSERT INTO mts_meta.tickers (ticker_id,ticker_code,ticker_first,ticker_seconds) VALUES (33,'VTBR-6.19','VTBR-6.19','RUB');
INSERT INTO mts_meta.tickers (ticker_id,ticker_code,ticker_first,ticker_seconds) VALUES (34,'GAZR-6.19','GAZR-6.19','RUB');
INSERT INTO mts_meta.tickers (ticker_id,ticker_code,ticker_first,ticker_seconds) VALUES (35,'Eu-6.19','Eu-6.19','RUB');
INSERT INTO mts_meta.tickers (ticker_id,ticker_code,ticker_first,ticker_seconds) VALUES (36,'BR-6.19','BR-6.19','RUB');
INSERT INTO mts_meta.tickers (ticker_id,ticker_code,ticker_first,ticker_seconds) VALUES (37,'ROSN-6.19','ROSN-6.19','RUB');
INSERT INTO mts_meta.tickers (ticker_id,ticker_code,ticker_first,ticker_seconds) VALUES (38,'Si-6.19','Si-6.19','RUB');

---------------------------------------------------
--
--Results of future analyze for each bar from mts_bars.bars
--
---------------------------------------------------

CREATE TABLE mts_bars.bars_fa(
	ticker_id      int,
	ddate          date,
	bar_width_sec  int,
    ts_end         bigint,
    c              double,
	log_oe         double,
	ts_end_res     bigint,
	dursec_res     int,
    ddate_res      date,
    c_res          double,
	res_type       text,
	PRIMARY KEY (( ticker_id, ddate, bar_width_sec ), log_oe, ts_end)
) WITH CLUSTERING ORDER BY (log_oe ASC, ts_end DESC);


---------------------------------------------------
--
--
--
---------------------------------------------------
--v1
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


---------------------------------------------------
--
--
--
---------------------------------------------------


INSERT INTO bars_property (ticker_id,bar_width_sec,is_enabled) VALUES (1,30,1);
INSERT INTO bars_property (ticker_id,bar_width_sec,is_enabled) VALUES (1,60,1);
INSERT INTO bars_property (ticker_id,bar_width_sec,is_enabled) VALUES (1,300,1);
INSERT INTO bars_property (ticker_id,bar_width_sec,is_enabled) VALUES (1,600,1);
INSERT INTO bars_property (ticker_id,bar_width_sec,is_enabled) VALUES (1,1800,1);
INSERT INTO bars_property (ticker_id,bar_width_sec,is_enabled) VALUES (1,3600,1);
INSERT INTO bars_property (ticker_id,bar_width_sec,is_enabled) VALUES (2,30,1);
INSERT INTO bars_property (ticker_id,bar_width_sec,is_enabled) VALUES (2,60,1);
INSERT INTO bars_property (ticker_id,bar_width_sec,is_enabled) VALUES (2,300,1);
INSERT INTO bars_property (ticker_id,bar_width_sec,is_enabled) VALUES (2,600,1);
INSERT INTO bars_property (ticker_id,bar_width_sec,is_enabled) VALUES (2,1800,1);
INSERT INTO bars_property (ticker_id,bar_width_sec,is_enabled) VALUES (2,3600,1);
INSERT INTO bars_property (ticker_id,bar_width_sec,is_enabled) VALUES (3,30,1);
INSERT INTO bars_property (ticker_id,bar_width_sec,is_enabled) VALUES (3,60,1);
INSERT INTO bars_property (ticker_id,bar_width_sec,is_enabled) VALUES (3,300,1);
INSERT INTO bars_property (ticker_id,bar_width_sec,is_enabled) VALUES (3,600,1);
INSERT INTO bars_property (ticker_id,bar_width_sec,is_enabled) VALUES (3,1800,1);
INSERT INTO bars_property (ticker_id,bar_width_sec,is_enabled) VALUES (3,3600,1);
INSERT INTO bars_property (ticker_id,bar_width_sec,is_enabled) VALUES (4,30,1);
INSERT INTO bars_property (ticker_id,bar_width_sec,is_enabled) VALUES (4,60,1);
INSERT INTO bars_property (ticker_id,bar_width_sec,is_enabled) VALUES (4,300,1);
INSERT INTO bars_property (ticker_id,bar_width_sec,is_enabled) VALUES (4,600,1);
INSERT INTO bars_property (ticker_id,bar_width_sec,is_enabled) VALUES (4,1800,1);
INSERT INTO bars_property (ticker_id,bar_width_sec,is_enabled) VALUES (4,3600,1);
INSERT INTO bars_property (ticker_id,bar_width_sec,is_enabled) VALUES (5,30,1);
INSERT INTO bars_property (ticker_id,bar_width_sec,is_enabled) VALUES (5,60,1);
INSERT INTO bars_property (ticker_id,bar_width_sec,is_enabled) VALUES (5,300,1);
INSERT INTO bars_property (ticker_id,bar_width_sec,is_enabled) VALUES (5,600,1);
INSERT INTO bars_property (ticker_id,bar_width_sec,is_enabled) VALUES (5,1800,1);
INSERT INTO bars_property (ticker_id,bar_width_sec,is_enabled) VALUES (5,3600,1);
INSERT INTO bars_property (ticker_id,bar_width_sec,is_enabled) VALUES (6,30,1);
INSERT INTO bars_property (ticker_id,bar_width_sec,is_enabled) VALUES (6,60,1);
INSERT INTO bars_property (ticker_id,bar_width_sec,is_enabled) VALUES (6,300,1);
INSERT INTO bars_property (ticker_id,bar_width_sec,is_enabled) VALUES (6,600,1);
INSERT INTO bars_property (ticker_id,bar_width_sec,is_enabled) VALUES (6,1800,1);
INSERT INTO bars_property (ticker_id,bar_width_sec,is_enabled) VALUES (6,3600,1);
INSERT INTO bars_property (ticker_id,bar_width_sec,is_enabled) VALUES (7,30,1);
INSERT INTO bars_property (ticker_id,bar_width_sec,is_enabled) VALUES (7,60,1);
INSERT INTO bars_property (ticker_id,bar_width_sec,is_enabled) VALUES (7,300,1);
INSERT INTO bars_property (ticker_id,bar_width_sec,is_enabled) VALUES (7,600,1);
INSERT INTO bars_property (ticker_id,bar_width_sec,is_enabled) VALUES (7,1800,1);
INSERT INTO bars_property (ticker_id,bar_width_sec,is_enabled) VALUES (7,3600,1);
INSERT INTO bars_property (ticker_id,bar_width_sec,is_enabled) VALUES (8,30,1);
INSERT INTO bars_property (ticker_id,bar_width_sec,is_enabled) VALUES (8,60,1);
INSERT INTO bars_property (ticker_id,bar_width_sec,is_enabled) VALUES (8,300,1);
INSERT INTO bars_property (ticker_id,bar_width_sec,is_enabled) VALUES (8,600,1);
INSERT INTO bars_property (ticker_id,bar_width_sec,is_enabled) VALUES (8,1800,1);
INSERT INTO bars_property (ticker_id,bar_width_sec,is_enabled) VALUES (8,3600,1);
INSERT INTO bars_property (ticker_id,bar_width_sec,is_enabled) VALUES (9,30,1);
INSERT INTO bars_property (ticker_id,bar_width_sec,is_enabled) VALUES (9,60,1);
INSERT INTO bars_property (ticker_id,bar_width_sec,is_enabled) VALUES (9,300,1);
INSERT INTO bars_property (ticker_id,bar_width_sec,is_enabled) VALUES (9,600,1);
INSERT INTO bars_property (ticker_id,bar_width_sec,is_enabled) VALUES (9,1800,1);
INSERT INTO bars_property (ticker_id,bar_width_sec,is_enabled) VALUES (9,3600,1);
INSERT INTO bars_property (ticker_id,bar_width_sec,is_enabled) VALUES (10,30,1);
INSERT INTO bars_property (ticker_id,bar_width_sec,is_enabled) VALUES (10,60,1);
INSERT INTO bars_property (ticker_id,bar_width_sec,is_enabled) VALUES (10,300,1);
INSERT INTO bars_property (ticker_id,bar_width_sec,is_enabled) VALUES (10,600,1);
INSERT INTO bars_property (ticker_id,bar_width_sec,is_enabled) VALUES (10,1800,1);
INSERT INTO bars_property (ticker_id,bar_width_sec,is_enabled) VALUES (10,3600,1);
INSERT INTO bars_property (ticker_id,bar_width_sec,is_enabled) VALUES (11,30,1);
INSERT INTO bars_property (ticker_id,bar_width_sec,is_enabled) VALUES (11,60,1);
INSERT INTO bars_property (ticker_id,bar_width_sec,is_enabled) VALUES (11,300,1);
INSERT INTO bars_property (ticker_id,bar_width_sec,is_enabled) VALUES (11,600,1);
INSERT INTO bars_property (ticker_id,bar_width_sec,is_enabled) VALUES (11,1800,1);
INSERT INTO bars_property (ticker_id,bar_width_sec,is_enabled) VALUES (11,3600,1);
INSERT INTO bars_property (ticker_id,bar_width_sec,is_enabled) VALUES (12,30,1);
INSERT INTO bars_property (ticker_id,bar_width_sec,is_enabled) VALUES (12,60,1);
INSERT INTO bars_property (ticker_id,bar_width_sec,is_enabled) VALUES (12,300,1);
INSERT INTO bars_property (ticker_id,bar_width_sec,is_enabled) VALUES (12,600,1);
INSERT INTO bars_property (ticker_id,bar_width_sec,is_enabled) VALUES (12,1800,1);
INSERT INTO bars_property (ticker_id,bar_width_sec,is_enabled) VALUES (12,3600,1);
INSERT INTO bars_property (ticker_id,bar_width_sec,is_enabled) VALUES (13,30,1);
INSERT INTO bars_property (ticker_id,bar_width_sec,is_enabled) VALUES (13,60,1);
INSERT INTO bars_property (ticker_id,bar_width_sec,is_enabled) VALUES (13,300,1);
INSERT INTO bars_property (ticker_id,bar_width_sec,is_enabled) VALUES (13,600,1);
INSERT INTO bars_property (ticker_id,bar_width_sec,is_enabled) VALUES (13,1800,1);
INSERT INTO bars_property (ticker_id,bar_width_sec,is_enabled) VALUES (13,3600,1);
INSERT INTO bars_property (ticker_id,bar_width_sec,is_enabled) VALUES (14,30,1);
INSERT INTO bars_property (ticker_id,bar_width_sec,is_enabled) VALUES (14,60,1);
INSERT INTO bars_property (ticker_id,bar_width_sec,is_enabled) VALUES (14,300,1);
INSERT INTO bars_property (ticker_id,bar_width_sec,is_enabled) VALUES (14,600,1);
INSERT INTO bars_property (ticker_id,bar_width_sec,is_enabled) VALUES (14,1800,1);
INSERT INTO bars_property (ticker_id,bar_width_sec,is_enabled) VALUES (14,3600,1);
INSERT INTO bars_property (ticker_id,bar_width_sec,is_enabled) VALUES (15,30,1);
INSERT INTO bars_property (ticker_id,bar_width_sec,is_enabled) VALUES (15,60,1);
INSERT INTO bars_property (ticker_id,bar_width_sec,is_enabled) VALUES (15,300,1);
INSERT INTO bars_property (ticker_id,bar_width_sec,is_enabled) VALUES (15,600,1);
INSERT INTO bars_property (ticker_id,bar_width_sec,is_enabled) VALUES (15,1800,1);
INSERT INTO bars_property (ticker_id,bar_width_sec,is_enabled) VALUES (15,3600,1);
INSERT INTO bars_property (ticker_id,bar_width_sec,is_enabled) VALUES (16,30,1);
INSERT INTO bars_property (ticker_id,bar_width_sec,is_enabled) VALUES (16,60,1);
INSERT INTO bars_property (ticker_id,bar_width_sec,is_enabled) VALUES (16,300,1);
INSERT INTO bars_property (ticker_id,bar_width_sec,is_enabled) VALUES (16,600,1);
INSERT INTO bars_property (ticker_id,bar_width_sec,is_enabled) VALUES (16,1800,1);
INSERT INTO bars_property (ticker_id,bar_width_sec,is_enabled) VALUES (16,3600,1);
INSERT INTO bars_property (ticker_id,bar_width_sec,is_enabled) VALUES (17,30,1);
INSERT INTO bars_property (ticker_id,bar_width_sec,is_enabled) VALUES (17,60,1);
INSERT INTO bars_property (ticker_id,bar_width_sec,is_enabled) VALUES (17,300,1);
INSERT INTO bars_property (ticker_id,bar_width_sec,is_enabled) VALUES (17,600,1);
INSERT INTO bars_property (ticker_id,bar_width_sec,is_enabled) VALUES (17,1800,1);
INSERT INTO bars_property (ticker_id,bar_width_sec,is_enabled) VALUES (17,3600,1);
INSERT INTO bars_property (ticker_id,bar_width_sec,is_enabled) VALUES (18,30,1);
INSERT INTO bars_property (ticker_id,bar_width_sec,is_enabled) VALUES (18,60,1);
INSERT INTO bars_property (ticker_id,bar_width_sec,is_enabled) VALUES (18,300,1);
INSERT INTO bars_property (ticker_id,bar_width_sec,is_enabled) VALUES (18,600,1);
INSERT INTO bars_property (ticker_id,bar_width_sec,is_enabled) VALUES (18,1800,1);
INSERT INTO bars_property (ticker_id,bar_width_sec,is_enabled) VALUES (18,3600,1);
INSERT INTO bars_property (ticker_id,bar_width_sec,is_enabled) VALUES (19,30,1);
INSERT INTO bars_property (ticker_id,bar_width_sec,is_enabled) VALUES (19,60,1);
INSERT INTO bars_property (ticker_id,bar_width_sec,is_enabled) VALUES (19,300,1);
INSERT INTO bars_property (ticker_id,bar_width_sec,is_enabled) VALUES (19,600,1);
INSERT INTO bars_property (ticker_id,bar_width_sec,is_enabled) VALUES (19,1800,1);
INSERT INTO bars_property (ticker_id,bar_width_sec,is_enabled) VALUES (19,3600,1);
INSERT INTO bars_property (ticker_id,bar_width_sec,is_enabled) VALUES (20,30,1);
INSERT INTO bars_property (ticker_id,bar_width_sec,is_enabled) VALUES (20,60,1);
INSERT INTO bars_property (ticker_id,bar_width_sec,is_enabled) VALUES (20,300,1);
INSERT INTO bars_property (ticker_id,bar_width_sec,is_enabled) VALUES (20,600,1);
INSERT INTO bars_property (ticker_id,bar_width_sec,is_enabled) VALUES (20,1800,1);
INSERT INTO bars_property (ticker_id,bar_width_sec,is_enabled) VALUES (20,3600,1);
INSERT INTO bars_property (ticker_id,bar_width_sec,is_enabled) VALUES (21,30,1);
INSERT INTO bars_property (ticker_id,bar_width_sec,is_enabled) VALUES (21,60,1);
INSERT INTO bars_property (ticker_id,bar_width_sec,is_enabled) VALUES (21,300,1);
INSERT INTO bars_property (ticker_id,bar_width_sec,is_enabled) VALUES (21,600,1);
INSERT INTO bars_property (ticker_id,bar_width_sec,is_enabled) VALUES (21,1800,1);
INSERT INTO bars_property (ticker_id,bar_width_sec,is_enabled) VALUES (21,3600,1);
INSERT INTO bars_property (ticker_id,bar_width_sec,is_enabled) VALUES (22,30,1);
INSERT INTO bars_property (ticker_id,bar_width_sec,is_enabled) VALUES (22,60,1);
INSERT INTO bars_property (ticker_id,bar_width_sec,is_enabled) VALUES (22,300,1);
INSERT INTO bars_property (ticker_id,bar_width_sec,is_enabled) VALUES (22,600,1);
INSERT INTO bars_property (ticker_id,bar_width_sec,is_enabled) VALUES (22,1800,1);
INSERT INTO bars_property (ticker_id,bar_width_sec,is_enabled) VALUES (22,3600,1);
INSERT INTO bars_property (ticker_id,bar_width_sec,is_enabled) VALUES (23,30,1);
INSERT INTO bars_property (ticker_id,bar_width_sec,is_enabled) VALUES (23,60,1);
INSERT INTO bars_property (ticker_id,bar_width_sec,is_enabled) VALUES (23,300,1);
INSERT INTO bars_property (ticker_id,bar_width_sec,is_enabled) VALUES (23,600,1);
INSERT INTO bars_property (ticker_id,bar_width_sec,is_enabled) VALUES (23,1800,1);
INSERT INTO bars_property (ticker_id,bar_width_sec,is_enabled) VALUES (23,3600,1);
INSERT INTO bars_property (ticker_id,bar_width_sec,is_enabled) VALUES (24,30,1);
INSERT INTO bars_property (ticker_id,bar_width_sec,is_enabled) VALUES (24,60,1);
INSERT INTO bars_property (ticker_id,bar_width_sec,is_enabled) VALUES (24,300,1);
INSERT INTO bars_property (ticker_id,bar_width_sec,is_enabled) VALUES (24,600,1);
INSERT INTO bars_property (ticker_id,bar_width_sec,is_enabled) VALUES (24,1800,1);
INSERT INTO bars_property (ticker_id,bar_width_sec,is_enabled) VALUES (24,3600,1);
INSERT INTO bars_property (ticker_id,bar_width_sec,is_enabled) VALUES (25,30,1);
INSERT INTO bars_property (ticker_id,bar_width_sec,is_enabled) VALUES (25,60,1);
INSERT INTO bars_property (ticker_id,bar_width_sec,is_enabled) VALUES (25,300,1);
INSERT INTO bars_property (ticker_id,bar_width_sec,is_enabled) VALUES (25,600,1);
INSERT INTO bars_property (ticker_id,bar_width_sec,is_enabled) VALUES (25,1800,1);
INSERT INTO bars_property (ticker_id,bar_width_sec,is_enabled) VALUES (25,3600,1);
INSERT INTO bars_property (ticker_id,bar_width_sec,is_enabled) VALUES (26,30,1);
INSERT INTO bars_property (ticker_id,bar_width_sec,is_enabled) VALUES (26,60,1);
INSERT INTO bars_property (ticker_id,bar_width_sec,is_enabled) VALUES (26,300,1);
INSERT INTO bars_property (ticker_id,bar_width_sec,is_enabled) VALUES (26,600,1);
INSERT INTO bars_property (ticker_id,bar_width_sec,is_enabled) VALUES (26,1800,1);
INSERT INTO bars_property (ticker_id,bar_width_sec,is_enabled) VALUES (26,3600,1);
INSERT INTO bars_property (ticker_id,bar_width_sec,is_enabled) VALUES (27,30,1);
INSERT INTO bars_property (ticker_id,bar_width_sec,is_enabled) VALUES (27,60,1);
INSERT INTO bars_property (ticker_id,bar_width_sec,is_enabled) VALUES (27,300,1);
INSERT INTO bars_property (ticker_id,bar_width_sec,is_enabled) VALUES (27,600,1);
INSERT INTO bars_property (ticker_id,bar_width_sec,is_enabled) VALUES (27,1800,1);
INSERT INTO bars_property (ticker_id,bar_width_sec,is_enabled) VALUES (27,3600,1);
INSERT INTO bars_property (ticker_id,bar_width_sec,is_enabled) VALUES (28,30,1);
INSERT INTO bars_property (ticker_id,bar_width_sec,is_enabled) VALUES (28,60,1);
INSERT INTO bars_property (ticker_id,bar_width_sec,is_enabled) VALUES (28,300,1);
INSERT INTO bars_property (ticker_id,bar_width_sec,is_enabled) VALUES (28,600,1);
INSERT INTO bars_property (ticker_id,bar_width_sec,is_enabled) VALUES (28,1800,1);
INSERT INTO bars_property (ticker_id,bar_width_sec,is_enabled) VALUES (28,3600,1);
INSERT INTO bars_property (ticker_id,bar_width_sec,is_enabled) VALUES (30,30,1);
INSERT INTO bars_property (ticker_id,bar_width_sec,is_enabled) VALUES (30,60,1);
INSERT INTO bars_property (ticker_id,bar_width_sec,is_enabled) VALUES (30,300,1);
INSERT INTO bars_property (ticker_id,bar_width_sec,is_enabled) VALUES (30,600,1);
INSERT INTO bars_property (ticker_id,bar_width_sec,is_enabled) VALUES (30,1800,1);
INSERT INTO bars_property (ticker_id,bar_width_sec,is_enabled) VALUES (30,3600,1);
INSERT INTO bars_property (ticker_id,bar_width_sec,is_enabled) VALUES (31,30,1);
INSERT INTO bars_property (ticker_id,bar_width_sec,is_enabled) VALUES (31,60,1);
INSERT INTO bars_property (ticker_id,bar_width_sec,is_enabled) VALUES (31,300,1);
INSERT INTO bars_property (ticker_id,bar_width_sec,is_enabled) VALUES (31,600,1);
INSERT INTO bars_property (ticker_id,bar_width_sec,is_enabled) VALUES (31,1800,1);
INSERT INTO bars_property (ticker_id,bar_width_sec,is_enabled) VALUES (31,3600,1);
INSERT INTO bars_property (ticker_id,bar_width_sec,is_enabled) VALUES (32,30,1);
INSERT INTO bars_property (ticker_id,bar_width_sec,is_enabled) VALUES (32,60,1);
INSERT INTO bars_property (ticker_id,bar_width_sec,is_enabled) VALUES (32,300,1);
INSERT INTO bars_property (ticker_id,bar_width_sec,is_enabled) VALUES (32,600,1);
INSERT INTO bars_property (ticker_id,bar_width_sec,is_enabled) VALUES (32,1800,1);
INSERT INTO bars_property (ticker_id,bar_width_sec,is_enabled) VALUES (32,3600,1);
INSERT INTO bars_property (ticker_id,bar_width_sec,is_enabled) VALUES (33,30,1);
INSERT INTO bars_property (ticker_id,bar_width_sec,is_enabled) VALUES (33,60,1);
INSERT INTO bars_property (ticker_id,bar_width_sec,is_enabled) VALUES (33,300,1);
INSERT INTO bars_property (ticker_id,bar_width_sec,is_enabled) VALUES (33,600,1);
INSERT INTO bars_property (ticker_id,bar_width_sec,is_enabled) VALUES (33,1800,1);
INSERT INTO bars_property (ticker_id,bar_width_sec,is_enabled) VALUES (33,3600,1);
INSERT INTO bars_property (ticker_id,bar_width_sec,is_enabled) VALUES (34,30,1);
INSERT INTO bars_property (ticker_id,bar_width_sec,is_enabled) VALUES (34,60,1);
INSERT INTO bars_property (ticker_id,bar_width_sec,is_enabled) VALUES (34,300,1);
INSERT INTO bars_property (ticker_id,bar_width_sec,is_enabled) VALUES (34,600,1);
INSERT INTO bars_property (ticker_id,bar_width_sec,is_enabled) VALUES (34,1800,1);
INSERT INTO bars_property (ticker_id,bar_width_sec,is_enabled) VALUES (34,3600,1);
INSERT INTO bars_property (ticker_id,bar_width_sec,is_enabled) VALUES (35,30,1);
INSERT INTO bars_property (ticker_id,bar_width_sec,is_enabled) VALUES (35,60,1);
INSERT INTO bars_property (ticker_id,bar_width_sec,is_enabled) VALUES (35,300,1);
INSERT INTO bars_property (ticker_id,bar_width_sec,is_enabled) VALUES (35,600,1);
INSERT INTO bars_property (ticker_id,bar_width_sec,is_enabled) VALUES (35,1800,1);
INSERT INTO bars_property (ticker_id,bar_width_sec,is_enabled) VALUES (35,3600,1);
INSERT INTO bars_property (ticker_id,bar_width_sec,is_enabled) VALUES (36,30,1);
INSERT INTO bars_property (ticker_id,bar_width_sec,is_enabled) VALUES (36,60,1);
INSERT INTO bars_property (ticker_id,bar_width_sec,is_enabled) VALUES (36,300,1);
INSERT INTO bars_property (ticker_id,bar_width_sec,is_enabled) VALUES (36,600,1);
INSERT INTO bars_property (ticker_id,bar_width_sec,is_enabled) VALUES (36,1800,1);
INSERT INTO bars_property (ticker_id,bar_width_sec,is_enabled) VALUES (36,3600,1);
INSERT INTO bars_property (ticker_id,bar_width_sec,is_enabled) VALUES (37,30,1);
INSERT INTO bars_property (ticker_id,bar_width_sec,is_enabled) VALUES (37,60,1);
INSERT INTO bars_property (ticker_id,bar_width_sec,is_enabled) VALUES (37,300,1);
INSERT INTO bars_property (ticker_id,bar_width_sec,is_enabled) VALUES (37,600,1);
INSERT INTO bars_property (ticker_id,bar_width_sec,is_enabled) VALUES (37,1800,1);
INSERT INTO bars_property (ticker_id,bar_width_sec,is_enabled) VALUES (37,3600,1);
INSERT INTO bars_property (ticker_id,bar_width_sec,is_enabled) VALUES (38,30,1);
INSERT INTO bars_property (ticker_id,bar_width_sec,is_enabled) VALUES (38,60,1);
INSERT INTO bars_property (ticker_id,bar_width_sec,is_enabled) VALUES (38,300,1);
INSERT INTO bars_property (ticker_id,bar_width_sec,is_enabled) VALUES (38,600,1);
INSERT INTO bars_property (ticker_id,bar_width_sec,is_enabled) VALUES (38,1800,1);
INSERT INTO bars_property (ticker_id,bar_width_sec,is_enabled) VALUES (38,3600,1);


CREATE KEYSPACE mts_ml
WITH durable_writes = true
AND replication = {
	'class' : 'SimpleStrategy',
	'replication_factor' : 3
}

create table mts_ml.result(
 label       double,
 prediction  double,
 p0          double,
 p1          double,
 formprops   int,
 PRIMARY KEY (label, prediction, p0, p1, formprops)
)

create table mts_ml.result(
 label       double,
 frmconfpeak int,
 prediction  double,
 p0    double,
 p1    double,
 PRIMARY KEY (label, frmconfpeak, prediction, p0,p1)
);


drop table mts_bars.bars_bws_dates;

/*
 Contain only last ddate for each key (tickerID + bar_width_sec)
 added to wide optimization of read ,ts_bars.bars.
 For removing queries like this:
 select distinct ticker_id,bar_width_sec,ddate from mts_bars.bars allow filtering
 For removing "allow filtering"
*/
CREATE TABLE mts_bars.bars_bws_dates(
	ticker_id     int,
	bar_width_sec int,
	ddate         date,
	PRIMARY KEY (ticker_id ,bar_width_sec)
);
