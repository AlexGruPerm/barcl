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
--
--
---------------------------------------------------


---------------------------------------------------
--
--
--
---------------------------------------------------