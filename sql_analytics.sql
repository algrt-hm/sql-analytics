--
-- sql-analytics
-- =============
--
-- 1. A view which provides the following information for each individual apprentice as of the last day of each month during their apprenticeship
--  > As view: one_final
--  > As table (faster to access): staging_one
--
-- Note: I have done LTM rather than over all time periods.
--       On first run takes about 1.5 mins end to end; could be sped up w/ staging tables.
--       Creating the view for the month ends (the first query) only needs to be run once and this view will persist, so
--        this query should not be run in subsequent sessions.
--       All other views are temp views so will only exist in the session -- this is for dev to avoid littering with views
--        that then need to be deleted and re-done during development.
--
-- a. Cumulative live days ON programme as of that day
--  > cum_days
--  > calculated in view one_final based ON view staging_one_a
--  > Flow:
--   > one_a_cols view are in columnar and not wrapped
--   > one_a_cols_cum view are in columnar and not wrapped and cumulative
--   > one_a_cum view is transposed and cumulative
--   > staging_one_a is a staging view
-- b. Cumulative targeted training hours as of that day
--  > cum_target
--  > calculated in view one_final based ON view staging_one_b
--  > Flow:
--   > one_b_log view does the joining necessary
--   > one_b_cols view is columnar
--   > one_b_cols_cum is columnar and cumulative
--   > one_b_cum view is transposed and cumulative
--   > staging_one_b is the staging view
-- c. Cumulative logged training hours as of that day
--  > cum_logged
--  > calculated in view one_final based ON view staging_one_c
--  > Flow:
--   > one_c_log view does the joining necessary
--   > one_c_cols view is columnar
--   > one_c_cols_cum is columnar and cumulative
--   > one_c_cum view is transposed and cumulative
--   > staging_one_c is the staging view
-- d. % of cumulative targeted training hours that have been logged as of that day
--  > pct_tgt_lgd
--  > calculated in view one_final based ON sub-query
-- e. Most recent feedback score as of that day
--  > response
--  > calculated in one_e_final based ON one_e_long_ages

-- 2. A view which provides the following information for each programme (i.e. group of apprentices enrolled in the same programme) as of 31st December 2020
--  > As view: two_final
--  > As table (faster to access): staging_two
-- a. Total number of 'live' apprentices that were ON programme as of that day
--  > n_live
--  > calculated in view two_combo_a from view two_a_b
-- b. Number of live apprentices that had 80%+ of their cumulative targeted training hours logged as of that day
--  > n_eighty_pct_plus_logged
--  > calculated in view two_combo_b from view two_d
-- c. % of total live apprentices that had 80%+ of their cumulative targeted training hours logged as of that day
--  > pct_eighty_pct_plus_logged
--  > calculated in view two_final from join of views two_combo_a and two_combo_b
-- d. Number of live apprentices that had an all-time average feedback score of 8+ as of that day
--  > n_avg_eight_plus
--  > calculated in view two_combo_b from view two_d
-- e. % of total live apprentices that had an all-time average feedback score of 8+ as of that day
--  > pct_avg_eight_plus
--  > calculated in view two_final from join of views two_combo_a and two_combo_b

-- create_monthend_views

CREATE OR REPLACE VIEW monthend
    AS
    SELECT
    (date_trunc('month', '2020-1-01'::date) + interval '1 month' - interval '1 day')::date AS "me202001",
(date_trunc('month', '2020-2-01'::date) + interval '1 month' - interval '1 day')::date AS "me202002",
(date_trunc('month', '2020-3-01'::date) + interval '1 month' - interval '1 day')::date AS "me202003",
(date_trunc('month', '2020-4-01'::date) + interval '1 month' - interval '1 day')::date AS "me202004",
(date_trunc('month', '2020-5-01'::date) + interval '1 month' - interval '1 day')::date AS "me202005",
(date_trunc('month', '2020-6-01'::date) + interval '1 month' - interval '1 day')::date AS "me202006",
(date_trunc('month', '2020-7-01'::date) + interval '1 month' - interval '1 day')::date AS "me202007",
(date_trunc('month', '2020-8-01'::date) + interval '1 month' - interval '1 day')::date AS "me202008",
(date_trunc('month', '2020-9-01'::date) + interval '1 month' - interval '1 day')::date AS "me202009",
(date_trunc('month', '2020-10-01'::date) + interval '1 month' - interval '1 day')::date AS "me202010",
(date_trunc('month', '2020-11-01'::date) + interval '1 month' - interval '1 day')::date AS "me202011",
(date_trunc('month', '2020-12-01'::date) + interval '1 month' - interval '1 day')::date AS "me202012",
(date_trunc('month', '2021-1-01'::date) + interval '1 month' - interval '1 day')::date AS "me202101"
;

-- create_views_for_days

CREATE OR REPLACE TEMPORARY VIEW m202101_v_a
AS
SELECT apprentice_id,
       me202012                                              AS period_start,
       me202101                                              AS period_end,
       COALESCE(apprenticeship_start_date, CURRENT_DATE)       AS start_date,
       COALESCE(
               LEAST(apprenticeship_withdrawal_date,
                     apprenticeship_completion_date)
           , CURRENT_DATE)                                     AS end_date,
       COALESCE(apprenticeship_break_start_date, CURRENT_DATE) AS break_start,
       COALESCE(apprenticeship_break_end_date, '2000-01-01')   AS break_end,
       -- days in the month
       (me202101 - me202012)               AS days_in_period
       -- bool for stated before end of period
FROM apprentice_dates,
     monthend
;

CREATE OR REPLACE TEMPORARY VIEW m202101_v_b
AS
SELECT apprentice_id,
       period_start,
       period_end,
       start_date,
       end_date,
       break_start,
       break_end,
       days_in_period,
       -- break over whole period
       CASE WHEN break_start < period_start AND break_end > period_end THEN 0 ELSE 1 END AS break_mult,
       -- start date after period
       CASE WHEN start_date > period_end THEN 0 ELSE 1 END                               AS not_started_mult,
       CASE WHEN end_date < period_start THEN 0 ELSE 1 END                               AS ended_mult
FROM m202101_v_a
;

CREATE OR REPLACE TEMPORARY VIEW m202101_v_c
AS
SELECT apprentice_id,
       period_start,
       period_end,
       start_date,
       end_date,
       break_start,
       break_end,
       break_mult,
       not_started_mult,
       ended_mult,
       days_in_period * not_started_mult * break_mult * ended_mult      AS dip, -- base days in period
       CASE
           WHEN not_started_mult = 1 AND break_mult = 1 AND ended_mult = 1
                     AND start_date > period_start THEN (period_start - start_date) END AS start_adj,
       CASE
           WHEN not_started_mult = 1 AND break_mult = 1 AND ended_mult = 1
                    AND end_date < period_end THEN (end_date - period_end) END     AS end_adj, -- X
       CASE
           WHEN not_started_mult = 1 AND break_mult = 1 AND ended_mult = 1
                    AND break_start < period_end
                   THEN -(period_end - greatest(period_start, break_start)) END  AS break_adj
FROM m202101_v_b
;

CREATE OR REPLACE TEMPORARY VIEW m202101_v_d
AS
SELECT
       apprentice_id,
       dip + COALESCE(start_adj, 0) + COALESCE(end_adj, 0) + COALESCE(break_adj, 0) AS m202101
FROM m202101_v_c
;
    

CREATE OR REPLACE TEMPORARY VIEW m202012_v_a
AS
SELECT apprentice_id,
       me202011                                              AS period_start,
       me202012                                              AS period_end,
       COALESCE(apprenticeship_start_date, CURRENT_DATE)       AS start_date,
       COALESCE(
               LEAST(apprenticeship_withdrawal_date,
                     apprenticeship_completion_date)
           , CURRENT_DATE)                                     AS end_date,
       COALESCE(apprenticeship_break_start_date, CURRENT_DATE) AS break_start,
       COALESCE(apprenticeship_break_end_date, '2000-01-01')   AS break_end,
       -- days in the month
       (me202012 - me202011)               AS days_in_period
       -- bool for stated before end of period
FROM apprentice_dates,
     monthend
;

CREATE OR REPLACE TEMPORARY VIEW m202012_v_b
AS
SELECT apprentice_id,
       period_start,
       period_end,
       start_date,
       end_date,
       break_start,
       break_end,
       days_in_period,
       -- break over whole period
       CASE WHEN break_start < period_start AND break_end > period_end THEN 0 ELSE 1 END AS break_mult,
       -- start date after period
       CASE WHEN start_date > period_end THEN 0 ELSE 1 END                               AS not_started_mult,
       CASE WHEN end_date < period_start THEN 0 ELSE 1 END                               AS ended_mult
FROM m202012_v_a
;

CREATE OR REPLACE TEMPORARY VIEW m202012_v_c
AS
SELECT apprentice_id,
       period_start,
       period_end,
       start_date,
       end_date,
       break_start,
       break_end,
       break_mult,
       not_started_mult,
       ended_mult,
       days_in_period * not_started_mult * break_mult * ended_mult      AS dip, -- base days in period
       CASE
           WHEN not_started_mult = 1 AND break_mult = 1 AND ended_mult = 1
                     AND start_date > period_start THEN (period_start - start_date) END AS start_adj,
       CASE
           WHEN not_started_mult = 1 AND break_mult = 1 AND ended_mult = 1
                    AND end_date < period_end THEN (end_date - period_end) END     AS end_adj, -- X
       CASE
           WHEN not_started_mult = 1 AND break_mult = 1 AND ended_mult = 1
                    AND break_start < period_end
                   THEN -(period_end - greatest(period_start, break_start)) END  AS break_adj
FROM m202012_v_b
;

CREATE OR REPLACE TEMPORARY VIEW m202012_v_d
AS
SELECT
       apprentice_id,
       dip + COALESCE(start_adj, 0) + COALESCE(end_adj, 0) + COALESCE(break_adj, 0) AS m202012
FROM m202012_v_c
;
    

CREATE OR REPLACE TEMPORARY VIEW m202011_v_a
AS
SELECT apprentice_id,
       me202010                                              AS period_start,
       me202011                                              AS period_end,
       COALESCE(apprenticeship_start_date, CURRENT_DATE)       AS start_date,
       COALESCE(
               LEAST(apprenticeship_withdrawal_date,
                     apprenticeship_completion_date)
           , CURRENT_DATE)                                     AS end_date,
       COALESCE(apprenticeship_break_start_date, CURRENT_DATE) AS break_start,
       COALESCE(apprenticeship_break_end_date, '2000-01-01')   AS break_end,
       -- days in the month
       (me202011 - me202010)               AS days_in_period
       -- bool for stated before end of period
FROM apprentice_dates,
     monthend
;

CREATE OR REPLACE TEMPORARY VIEW m202011_v_b
AS
SELECT apprentice_id,
       period_start,
       period_end,
       start_date,
       end_date,
       break_start,
       break_end,
       days_in_period,
       -- break over whole period
       CASE WHEN break_start < period_start AND break_end > period_end THEN 0 ELSE 1 END AS break_mult,
       -- start date after period
       CASE WHEN start_date > period_end THEN 0 ELSE 1 END                               AS not_started_mult,
       CASE WHEN end_date < period_start THEN 0 ELSE 1 END                               AS ended_mult
FROM m202011_v_a
;

CREATE OR REPLACE TEMPORARY VIEW m202011_v_c
AS
SELECT apprentice_id,
       period_start,
       period_end,
       start_date,
       end_date,
       break_start,
       break_end,
       break_mult,
       not_started_mult,
       ended_mult,
       days_in_period * not_started_mult * break_mult * ended_mult      AS dip, -- base days in period
       CASE
           WHEN not_started_mult = 1 AND break_mult = 1 AND ended_mult = 1
                     AND start_date > period_start THEN (period_start - start_date) END AS start_adj,
       CASE
           WHEN not_started_mult = 1 AND break_mult = 1 AND ended_mult = 1
                    AND end_date < period_end THEN (end_date - period_end) END     AS end_adj, -- X
       CASE
           WHEN not_started_mult = 1 AND break_mult = 1 AND ended_mult = 1
                    AND break_start < period_end
                   THEN -(period_end - greatest(period_start, break_start)) END  AS break_adj
FROM m202011_v_b
;

CREATE OR REPLACE TEMPORARY VIEW m202011_v_d
AS
SELECT
       apprentice_id,
       dip + COALESCE(start_adj, 0) + COALESCE(end_adj, 0) + COALESCE(break_adj, 0) AS m202011
FROM m202011_v_c
;
    

CREATE OR REPLACE TEMPORARY VIEW m202010_v_a
AS
SELECT apprentice_id,
       me202009                                              AS period_start,
       me202010                                              AS period_end,
       COALESCE(apprenticeship_start_date, CURRENT_DATE)       AS start_date,
       COALESCE(
               LEAST(apprenticeship_withdrawal_date,
                     apprenticeship_completion_date)
           , CURRENT_DATE)                                     AS end_date,
       COALESCE(apprenticeship_break_start_date, CURRENT_DATE) AS break_start,
       COALESCE(apprenticeship_break_end_date, '2000-01-01')   AS break_end,
       -- days in the month
       (me202010 - me202009)               AS days_in_period
       -- bool for stated before end of period
FROM apprentice_dates,
     monthend
;

CREATE OR REPLACE TEMPORARY VIEW m202010_v_b
AS
SELECT apprentice_id,
       period_start,
       period_end,
       start_date,
       end_date,
       break_start,
       break_end,
       days_in_period,
       -- break over whole period
       CASE WHEN break_start < period_start AND break_end > period_end THEN 0 ELSE 1 END AS break_mult,
       -- start date after period
       CASE WHEN start_date > period_end THEN 0 ELSE 1 END                               AS not_started_mult,
       CASE WHEN end_date < period_start THEN 0 ELSE 1 END                               AS ended_mult
FROM m202010_v_a
;

CREATE OR REPLACE TEMPORARY VIEW m202010_v_c
AS
SELECT apprentice_id,
       period_start,
       period_end,
       start_date,
       end_date,
       break_start,
       break_end,
       break_mult,
       not_started_mult,
       ended_mult,
       days_in_period * not_started_mult * break_mult * ended_mult      AS dip, -- base days in period
       CASE
           WHEN not_started_mult = 1 AND break_mult = 1 AND ended_mult = 1
                     AND start_date > period_start THEN (period_start - start_date) END AS start_adj,
       CASE
           WHEN not_started_mult = 1 AND break_mult = 1 AND ended_mult = 1
                    AND end_date < period_end THEN (end_date - period_end) END     AS end_adj, -- X
       CASE
           WHEN not_started_mult = 1 AND break_mult = 1 AND ended_mult = 1
                    AND break_start < period_end
                   THEN -(period_end - greatest(period_start, break_start)) END  AS break_adj
FROM m202010_v_b
;

CREATE OR REPLACE TEMPORARY VIEW m202010_v_d
AS
SELECT
       apprentice_id,
       dip + COALESCE(start_adj, 0) + COALESCE(end_adj, 0) + COALESCE(break_adj, 0) AS m202010
FROM m202010_v_c
;
    

CREATE OR REPLACE TEMPORARY VIEW m202009_v_a
AS
SELECT apprentice_id,
       me202008                                              AS period_start,
       me202009                                              AS period_end,
       COALESCE(apprenticeship_start_date, CURRENT_DATE)       AS start_date,
       COALESCE(
               LEAST(apprenticeship_withdrawal_date,
                     apprenticeship_completion_date)
           , CURRENT_DATE)                                     AS end_date,
       COALESCE(apprenticeship_break_start_date, CURRENT_DATE) AS break_start,
       COALESCE(apprenticeship_break_end_date, '2000-01-01')   AS break_end,
       -- days in the month
       (me202009 - me202008)               AS days_in_period
       -- bool for stated before end of period
FROM apprentice_dates,
     monthend
;

CREATE OR REPLACE TEMPORARY VIEW m202009_v_b
AS
SELECT apprentice_id,
       period_start,
       period_end,
       start_date,
       end_date,
       break_start,
       break_end,
       days_in_period,
       -- break over whole period
       CASE WHEN break_start < period_start AND break_end > period_end THEN 0 ELSE 1 END AS break_mult,
       -- start date after period
       CASE WHEN start_date > period_end THEN 0 ELSE 1 END                               AS not_started_mult,
       CASE WHEN end_date < period_start THEN 0 ELSE 1 END                               AS ended_mult
FROM m202009_v_a
;

CREATE OR REPLACE TEMPORARY VIEW m202009_v_c
AS
SELECT apprentice_id,
       period_start,
       period_end,
       start_date,
       end_date,
       break_start,
       break_end,
       break_mult,
       not_started_mult,
       ended_mult,
       days_in_period * not_started_mult * break_mult * ended_mult      AS dip, -- base days in period
       CASE
           WHEN not_started_mult = 1 AND break_mult = 1 AND ended_mult = 1
                     AND start_date > period_start THEN (period_start - start_date) END AS start_adj,
       CASE
           WHEN not_started_mult = 1 AND break_mult = 1 AND ended_mult = 1
                    AND end_date < period_end THEN (end_date - period_end) END     AS end_adj, -- X
       CASE
           WHEN not_started_mult = 1 AND break_mult = 1 AND ended_mult = 1
                    AND break_start < period_end
                   THEN -(period_end - greatest(period_start, break_start)) END  AS break_adj
FROM m202009_v_b
;

CREATE OR REPLACE TEMPORARY VIEW m202009_v_d
AS
SELECT
       apprentice_id,
       dip + COALESCE(start_adj, 0) + COALESCE(end_adj, 0) + COALESCE(break_adj, 0) AS m202009
FROM m202009_v_c
;
    

CREATE OR REPLACE TEMPORARY VIEW m202008_v_a
AS
SELECT apprentice_id,
       me202007                                              AS period_start,
       me202008                                              AS period_end,
       COALESCE(apprenticeship_start_date, CURRENT_DATE)       AS start_date,
       COALESCE(
               LEAST(apprenticeship_withdrawal_date,
                     apprenticeship_completion_date)
           , CURRENT_DATE)                                     AS end_date,
       COALESCE(apprenticeship_break_start_date, CURRENT_DATE) AS break_start,
       COALESCE(apprenticeship_break_end_date, '2000-01-01')   AS break_end,
       -- days in the month
       (me202008 - me202007)               AS days_in_period
       -- bool for stated before end of period
FROM apprentice_dates,
     monthend
;

CREATE OR REPLACE TEMPORARY VIEW m202008_v_b
AS
SELECT apprentice_id,
       period_start,
       period_end,
       start_date,
       end_date,
       break_start,
       break_end,
       days_in_period,
       -- break over whole period
       CASE WHEN break_start < period_start AND break_end > period_end THEN 0 ELSE 1 END AS break_mult,
       -- start date after period
       CASE WHEN start_date > period_end THEN 0 ELSE 1 END                               AS not_started_mult,
       CASE WHEN end_date < period_start THEN 0 ELSE 1 END                               AS ended_mult
FROM m202008_v_a
;

CREATE OR REPLACE TEMPORARY VIEW m202008_v_c
AS
SELECT apprentice_id,
       period_start,
       period_end,
       start_date,
       end_date,
       break_start,
       break_end,
       break_mult,
       not_started_mult,
       ended_mult,
       days_in_period * not_started_mult * break_mult * ended_mult      AS dip, -- base days in period
       CASE
           WHEN not_started_mult = 1 AND break_mult = 1 AND ended_mult = 1
                     AND start_date > period_start THEN (period_start - start_date) END AS start_adj,
       CASE
           WHEN not_started_mult = 1 AND break_mult = 1 AND ended_mult = 1
                    AND end_date < period_end THEN (end_date - period_end) END     AS end_adj, -- X
       CASE
           WHEN not_started_mult = 1 AND break_mult = 1 AND ended_mult = 1
                    AND break_start < period_end
                   THEN -(period_end - greatest(period_start, break_start)) END  AS break_adj
FROM m202008_v_b
;

CREATE OR REPLACE TEMPORARY VIEW m202008_v_d
AS
SELECT
       apprentice_id,
       dip + COALESCE(start_adj, 0) + COALESCE(end_adj, 0) + COALESCE(break_adj, 0) AS m202008
FROM m202008_v_c
;
    

CREATE OR REPLACE TEMPORARY VIEW m202007_v_a
AS
SELECT apprentice_id,
       me202006                                              AS period_start,
       me202007                                              AS period_end,
       COALESCE(apprenticeship_start_date, CURRENT_DATE)       AS start_date,
       COALESCE(
               LEAST(apprenticeship_withdrawal_date,
                     apprenticeship_completion_date)
           , CURRENT_DATE)                                     AS end_date,
       COALESCE(apprenticeship_break_start_date, CURRENT_DATE) AS break_start,
       COALESCE(apprenticeship_break_end_date, '2000-01-01')   AS break_end,
       -- days in the month
       (me202007 - me202006)               AS days_in_period
       -- bool for stated before end of period
FROM apprentice_dates,
     monthend
;

CREATE OR REPLACE TEMPORARY VIEW m202007_v_b
AS
SELECT apprentice_id,
       period_start,
       period_end,
       start_date,
       end_date,
       break_start,
       break_end,
       days_in_period,
       -- break over whole period
       CASE WHEN break_start < period_start AND break_end > period_end THEN 0 ELSE 1 END AS break_mult,
       -- start date after period
       CASE WHEN start_date > period_end THEN 0 ELSE 1 END                               AS not_started_mult,
       CASE WHEN end_date < period_start THEN 0 ELSE 1 END                               AS ended_mult
FROM m202007_v_a
;

CREATE OR REPLACE TEMPORARY VIEW m202007_v_c
AS
SELECT apprentice_id,
       period_start,
       period_end,
       start_date,
       end_date,
       break_start,
       break_end,
       break_mult,
       not_started_mult,
       ended_mult,
       days_in_period * not_started_mult * break_mult * ended_mult      AS dip, -- base days in period
       CASE
           WHEN not_started_mult = 1 AND break_mult = 1 AND ended_mult = 1
                     AND start_date > period_start THEN (period_start - start_date) END AS start_adj,
       CASE
           WHEN not_started_mult = 1 AND break_mult = 1 AND ended_mult = 1
                    AND end_date < period_end THEN (end_date - period_end) END     AS end_adj, -- X
       CASE
           WHEN not_started_mult = 1 AND break_mult = 1 AND ended_mult = 1
                    AND break_start < period_end
                   THEN -(period_end - greatest(period_start, break_start)) END  AS break_adj
FROM m202007_v_b
;

CREATE OR REPLACE TEMPORARY VIEW m202007_v_d
AS
SELECT
       apprentice_id,
       dip + COALESCE(start_adj, 0) + COALESCE(end_adj, 0) + COALESCE(break_adj, 0) AS m202007
FROM m202007_v_c
;
    

CREATE OR REPLACE TEMPORARY VIEW m202006_v_a
AS
SELECT apprentice_id,
       me202005                                              AS period_start,
       me202006                                              AS period_end,
       COALESCE(apprenticeship_start_date, CURRENT_DATE)       AS start_date,
       COALESCE(
               LEAST(apprenticeship_withdrawal_date,
                     apprenticeship_completion_date)
           , CURRENT_DATE)                                     AS end_date,
       COALESCE(apprenticeship_break_start_date, CURRENT_DATE) AS break_start,
       COALESCE(apprenticeship_break_end_date, '2000-01-01')   AS break_end,
       -- days in the month
       (me202006 - me202005)               AS days_in_period
       -- bool for stated before end of period
FROM apprentice_dates,
     monthend
;

CREATE OR REPLACE TEMPORARY VIEW m202006_v_b
AS
SELECT apprentice_id,
       period_start,
       period_end,
       start_date,
       end_date,
       break_start,
       break_end,
       days_in_period,
       -- break over whole period
       CASE WHEN break_start < period_start AND break_end > period_end THEN 0 ELSE 1 END AS break_mult,
       -- start date after period
       CASE WHEN start_date > period_end THEN 0 ELSE 1 END                               AS not_started_mult,
       CASE WHEN end_date < period_start THEN 0 ELSE 1 END                               AS ended_mult
FROM m202006_v_a
;

CREATE OR REPLACE TEMPORARY VIEW m202006_v_c
AS
SELECT apprentice_id,
       period_start,
       period_end,
       start_date,
       end_date,
       break_start,
       break_end,
       break_mult,
       not_started_mult,
       ended_mult,
       days_in_period * not_started_mult * break_mult * ended_mult      AS dip, -- base days in period
       CASE
           WHEN not_started_mult = 1 AND break_mult = 1 AND ended_mult = 1
                     AND start_date > period_start THEN (period_start - start_date) END AS start_adj,
       CASE
           WHEN not_started_mult = 1 AND break_mult = 1 AND ended_mult = 1
                    AND end_date < period_end THEN (end_date - period_end) END     AS end_adj, -- X
       CASE
           WHEN not_started_mult = 1 AND break_mult = 1 AND ended_mult = 1
                    AND break_start < period_end
                   THEN -(period_end - greatest(period_start, break_start)) END  AS break_adj
FROM m202006_v_b
;

CREATE OR REPLACE TEMPORARY VIEW m202006_v_d
AS
SELECT
       apprentice_id,
       dip + COALESCE(start_adj, 0) + COALESCE(end_adj, 0) + COALESCE(break_adj, 0) AS m202006
FROM m202006_v_c
;
    

CREATE OR REPLACE TEMPORARY VIEW m202005_v_a
AS
SELECT apprentice_id,
       me202004                                              AS period_start,
       me202005                                              AS period_end,
       COALESCE(apprenticeship_start_date, CURRENT_DATE)       AS start_date,
       COALESCE(
               LEAST(apprenticeship_withdrawal_date,
                     apprenticeship_completion_date)
           , CURRENT_DATE)                                     AS end_date,
       COALESCE(apprenticeship_break_start_date, CURRENT_DATE) AS break_start,
       COALESCE(apprenticeship_break_end_date, '2000-01-01')   AS break_end,
       -- days in the month
       (me202005 - me202004)               AS days_in_period
       -- bool for stated before end of period
FROM apprentice_dates,
     monthend
;

CREATE OR REPLACE TEMPORARY VIEW m202005_v_b
AS
SELECT apprentice_id,
       period_start,
       period_end,
       start_date,
       end_date,
       break_start,
       break_end,
       days_in_period,
       -- break over whole period
       CASE WHEN break_start < period_start AND break_end > period_end THEN 0 ELSE 1 END AS break_mult,
       -- start date after period
       CASE WHEN start_date > period_end THEN 0 ELSE 1 END                               AS not_started_mult,
       CASE WHEN end_date < period_start THEN 0 ELSE 1 END                               AS ended_mult
FROM m202005_v_a
;

CREATE OR REPLACE TEMPORARY VIEW m202005_v_c
AS
SELECT apprentice_id,
       period_start,
       period_end,
       start_date,
       end_date,
       break_start,
       break_end,
       break_mult,
       not_started_mult,
       ended_mult,
       days_in_period * not_started_mult * break_mult * ended_mult      AS dip, -- base days in period
       CASE
           WHEN not_started_mult = 1 AND break_mult = 1 AND ended_mult = 1
                     AND start_date > period_start THEN (period_start - start_date) END AS start_adj,
       CASE
           WHEN not_started_mult = 1 AND break_mult = 1 AND ended_mult = 1
                    AND end_date < period_end THEN (end_date - period_end) END     AS end_adj, -- X
       CASE
           WHEN not_started_mult = 1 AND break_mult = 1 AND ended_mult = 1
                    AND break_start < period_end
                   THEN -(period_end - greatest(period_start, break_start)) END  AS break_adj
FROM m202005_v_b
;

CREATE OR REPLACE TEMPORARY VIEW m202005_v_d
AS
SELECT
       apprentice_id,
       dip + COALESCE(start_adj, 0) + COALESCE(end_adj, 0) + COALESCE(break_adj, 0) AS m202005
FROM m202005_v_c
;
    

CREATE OR REPLACE TEMPORARY VIEW m202004_v_a
AS
SELECT apprentice_id,
       me202003                                              AS period_start,
       me202004                                              AS period_end,
       COALESCE(apprenticeship_start_date, CURRENT_DATE)       AS start_date,
       COALESCE(
               LEAST(apprenticeship_withdrawal_date,
                     apprenticeship_completion_date)
           , CURRENT_DATE)                                     AS end_date,
       COALESCE(apprenticeship_break_start_date, CURRENT_DATE) AS break_start,
       COALESCE(apprenticeship_break_end_date, '2000-01-01')   AS break_end,
       -- days in the month
       (me202004 - me202003)               AS days_in_period
       -- bool for stated before end of period
FROM apprentice_dates,
     monthend
;

CREATE OR REPLACE TEMPORARY VIEW m202004_v_b
AS
SELECT apprentice_id,
       period_start,
       period_end,
       start_date,
       end_date,
       break_start,
       break_end,
       days_in_period,
       -- break over whole period
       CASE WHEN break_start < period_start AND break_end > period_end THEN 0 ELSE 1 END AS break_mult,
       -- start date after period
       CASE WHEN start_date > period_end THEN 0 ELSE 1 END                               AS not_started_mult,
       CASE WHEN end_date < period_start THEN 0 ELSE 1 END                               AS ended_mult
FROM m202004_v_a
;

CREATE OR REPLACE TEMPORARY VIEW m202004_v_c
AS
SELECT apprentice_id,
       period_start,
       period_end,
       start_date,
       end_date,
       break_start,
       break_end,
       break_mult,
       not_started_mult,
       ended_mult,
       days_in_period * not_started_mult * break_mult * ended_mult      AS dip, -- base days in period
       CASE
           WHEN not_started_mult = 1 AND break_mult = 1 AND ended_mult = 1
                     AND start_date > period_start THEN (period_start - start_date) END AS start_adj,
       CASE
           WHEN not_started_mult = 1 AND break_mult = 1 AND ended_mult = 1
                    AND end_date < period_end THEN (end_date - period_end) END     AS end_adj, -- X
       CASE
           WHEN not_started_mult = 1 AND break_mult = 1 AND ended_mult = 1
                    AND break_start < period_end
                   THEN -(period_end - greatest(period_start, break_start)) END  AS break_adj
FROM m202004_v_b
;

CREATE OR REPLACE TEMPORARY VIEW m202004_v_d
AS
SELECT
       apprentice_id,
       dip + COALESCE(start_adj, 0) + COALESCE(end_adj, 0) + COALESCE(break_adj, 0) AS m202004
FROM m202004_v_c
;
    

CREATE OR REPLACE TEMPORARY VIEW m202003_v_a
AS
SELECT apprentice_id,
       me202002                                              AS period_start,
       me202003                                              AS period_end,
       COALESCE(apprenticeship_start_date, CURRENT_DATE)       AS start_date,
       COALESCE(
               LEAST(apprenticeship_withdrawal_date,
                     apprenticeship_completion_date)
           , CURRENT_DATE)                                     AS end_date,
       COALESCE(apprenticeship_break_start_date, CURRENT_DATE) AS break_start,
       COALESCE(apprenticeship_break_end_date, '2000-01-01')   AS break_end,
       -- days in the month
       (me202003 - me202002)               AS days_in_period
       -- bool for stated before end of period
FROM apprentice_dates,
     monthend
;

CREATE OR REPLACE TEMPORARY VIEW m202003_v_b
AS
SELECT apprentice_id,
       period_start,
       period_end,
       start_date,
       end_date,
       break_start,
       break_end,
       days_in_period,
       -- break over whole period
       CASE WHEN break_start < period_start AND break_end > period_end THEN 0 ELSE 1 END AS break_mult,
       -- start date after period
       CASE WHEN start_date > period_end THEN 0 ELSE 1 END                               AS not_started_mult,
       CASE WHEN end_date < period_start THEN 0 ELSE 1 END                               AS ended_mult
FROM m202003_v_a
;

CREATE OR REPLACE TEMPORARY VIEW m202003_v_c
AS
SELECT apprentice_id,
       period_start,
       period_end,
       start_date,
       end_date,
       break_start,
       break_end,
       break_mult,
       not_started_mult,
       ended_mult,
       days_in_period * not_started_mult * break_mult * ended_mult      AS dip, -- base days in period
       CASE
           WHEN not_started_mult = 1 AND break_mult = 1 AND ended_mult = 1
                     AND start_date > period_start THEN (period_start - start_date) END AS start_adj,
       CASE
           WHEN not_started_mult = 1 AND break_mult = 1 AND ended_mult = 1
                    AND end_date < period_end THEN (end_date - period_end) END     AS end_adj, -- X
       CASE
           WHEN not_started_mult = 1 AND break_mult = 1 AND ended_mult = 1
                    AND break_start < period_end
                   THEN -(period_end - greatest(period_start, break_start)) END  AS break_adj
FROM m202003_v_b
;

CREATE OR REPLACE TEMPORARY VIEW m202003_v_d
AS
SELECT
       apprentice_id,
       dip + COALESCE(start_adj, 0) + COALESCE(end_adj, 0) + COALESCE(break_adj, 0) AS m202003
FROM m202003_v_c
;
    

CREATE OR REPLACE TEMPORARY VIEW m202002_v_a
AS
SELECT apprentice_id,
       me202001                                              AS period_start,
       me202002                                              AS period_end,
       COALESCE(apprenticeship_start_date, CURRENT_DATE)       AS start_date,
       COALESCE(
               LEAST(apprenticeship_withdrawal_date,
                     apprenticeship_completion_date)
           , CURRENT_DATE)                                     AS end_date,
       COALESCE(apprenticeship_break_start_date, CURRENT_DATE) AS break_start,
       COALESCE(apprenticeship_break_end_date, '2000-01-01')   AS break_end,
       -- days in the month
       (me202002 - me202001)               AS days_in_period
       -- bool for stated before end of period
FROM apprentice_dates,
     monthend
;

CREATE OR REPLACE TEMPORARY VIEW m202002_v_b
AS
SELECT apprentice_id,
       period_start,
       period_end,
       start_date,
       end_date,
       break_start,
       break_end,
       days_in_period,
       -- break over whole period
       CASE WHEN break_start < period_start AND break_end > period_end THEN 0 ELSE 1 END AS break_mult,
       -- start date after period
       CASE WHEN start_date > period_end THEN 0 ELSE 1 END                               AS not_started_mult,
       CASE WHEN end_date < period_start THEN 0 ELSE 1 END                               AS ended_mult
FROM m202002_v_a
;

CREATE OR REPLACE TEMPORARY VIEW m202002_v_c
AS
SELECT apprentice_id,
       period_start,
       period_end,
       start_date,
       end_date,
       break_start,
       break_end,
       break_mult,
       not_started_mult,
       ended_mult,
       days_in_period * not_started_mult * break_mult * ended_mult      AS dip, -- base days in period
       CASE
           WHEN not_started_mult = 1 AND break_mult = 1 AND ended_mult = 1
                     AND start_date > period_start THEN (period_start - start_date) END AS start_adj,
       CASE
           WHEN not_started_mult = 1 AND break_mult = 1 AND ended_mult = 1
                    AND end_date < period_end THEN (end_date - period_end) END     AS end_adj, -- X
       CASE
           WHEN not_started_mult = 1 AND break_mult = 1 AND ended_mult = 1
                    AND break_start < period_end
                   THEN -(period_end - greatest(period_start, break_start)) END  AS break_adj
FROM m202002_v_b
;

CREATE OR REPLACE TEMPORARY VIEW m202002_v_d
AS
SELECT
       apprentice_id,
       dip + COALESCE(start_adj, 0) + COALESCE(end_adj, 0) + COALESCE(break_adj, 0) AS m202002
FROM m202002_v_c
;
    
-- join_for_days
CREATE OR REPLACE TEMPORARY VIEW one_a_cols
AS
SELECT m202101_v_d.apprentice_id, m202002,m202003,m202004,m202005,m202006,m202007,m202008,m202009,m202010,m202011,m202012,m202101
FROM m202101_v_d
INNER JOIN m202012_v_d ON m202012_v_d.apprentice_id = m202101_v_d.apprentice_id
INNER JOIN m202011_v_d ON m202011_v_d.apprentice_id = m202101_v_d.apprentice_id
INNER JOIN m202010_v_d ON m202010_v_d.apprentice_id = m202101_v_d.apprentice_id
INNER JOIN m202009_v_d ON m202009_v_d.apprentice_id = m202101_v_d.apprentice_id
INNER JOIN m202008_v_d ON m202008_v_d.apprentice_id = m202101_v_d.apprentice_id
INNER JOIN m202007_v_d ON m202007_v_d.apprentice_id = m202101_v_d.apprentice_id
INNER JOIN m202006_v_d ON m202006_v_d.apprentice_id = m202101_v_d.apprentice_id
INNER JOIN m202005_v_d ON m202005_v_d.apprentice_id = m202101_v_d.apprentice_id
INNER JOIN m202004_v_d ON m202004_v_d.apprentice_id = m202101_v_d.apprentice_id
INNER JOIN m202003_v_d ON m202003_v_d.apprentice_id = m202101_v_d.apprentice_id
INNER JOIN m202002_v_d ON m202002_v_d.apprentice_id = m202101_v_d.apprentice_id
;

-- cum_days

CREATE OR REPLACE TEMPORARY VIEW one_a_cols_cum
AS
SELECT
apprentice_id,
m202002 AS cm202002 ,
m202002+m202003 AS cm202003 ,
m202002+m202003+m202004 AS cm202004 ,
m202002+m202003+m202004+m202005 AS cm202005 ,
m202002+m202003+m202004+m202005+m202006 AS cm202006 ,
m202002+m202003+m202004+m202005+m202006+m202007 AS cm202007 ,
m202002+m202003+m202004+m202005+m202006+m202007+m202008 AS cm202008 ,
m202002+m202003+m202004+m202005+m202006+m202007+m202008+m202009 AS cm202009 ,
m202002+m202003+m202004+m202005+m202006+m202007+m202008+m202009+m202010 AS cm202010 ,
m202002+m202003+m202004+m202005+m202006+m202007+m202008+m202009+m202010+m202011 AS cm202011 ,
m202002+m202003+m202004+m202005+m202006+m202007+m202008+m202009+m202010+m202011+m202012 AS cm202012 ,
m202002+m202003+m202004+m202005+m202006+m202007+m202008+m202009+m202010+m202011+m202012+m202101 AS cm202101 
FROM one_a_cols;
-- cum_days_transposed

CREATE OR REPLACE TEMPORARY VIEW one_a_cum
AS
SELECT c.apprentice_id, t.month, t.cum_days
FROM one_a_cols_cum c
  CROSS JOIN LATERAL (
     VALUES
        (c.cm202101, '202101'),
(c.cm202012, '202012'),
(c.cm202011, '202011'),
(c.cm202010, '202010'),
(c.cm202009, '202009'),
(c.cm202008, '202008'),
(c.cm202007, '202007'),
(c.cm202006, '202006'),
(c.cm202005, '202005'),
(c.cm202004, '202004'),
(c.cm202003, '202003'),
(c.cm202002, '202002')
  ) AS t(cum_days, month)
ORDER BY apprentice_id, month;
-- staging_one_a

CREATE OR REPLACE TEMPORARY VIEW staging_one_a
AS
SELECT *
FROM one_a_cum
;

-- one_b_various

CREATE OR REPLACE TEMPORARY VIEW one_b_tt
AS
    SELECT apprentice_id,
           cohort_id,
           lms_user_detail_field_id,
           name,
           data
    FROM apprentice_spine
             JOIN lms_user_detail_data ON lms_user_detail_data.lms_user_id = apprentice_spine.lms_user_id
             JOIN lms_detail_fields ON lms_user_detail_data.lms_user_detail_field_id = lms_detail_fields.id::bigint
    ORDER BY apprentice_id, lms_user_detail_field_id
;

CREATE OR REPLACE TEMPORARY VIEW one_b_roles
AS
SELECT *
FROM  crosstab(
   'SELECT apprentice_id, name, data
    FROM   one_b_tt
    ORDER  BY 1,2'  -- needs to be "ORDER BY 1,2" here
   ) AS ct ("apprentice_id" bigint, "Role type" text, "Targeted weekly training hours" text)
ORDER BY apprentice_id
;

CREATE OR REPLACE TEMPORARY VIEW one_b_targeted_log
AS
WITH sub AS (
    SELECT apprentice_id,
           "Role type"                      AS role,
           "Targeted weekly training hours"::float AS targeted,
           case
               WHEN "Role type" = 'Career Starter' THEN 4
               WHEN "Role type" = 'Career Builder' THEN 5
               ELSE null END AS weeks_hols
    FROM one_b_roles
)
SELECT apprentice_id, role, targeted, weeks_hols,
       (((52-weeks_hols)*targeted)/52)*4 AS adj_targeted_pm
FROM sub;

-- take the live days / 30.4375 to get pro rata and apply this to the adj_targeted_pm for the apprentice_id
CREATE OR REPLACE TEMPORARY VIEW one_b_cols
AS
SELECT
one_a_cols.apprentice_id,
(m202002/30.4375) * adj_targeted_pm AS m202002,
(m202003/30.4375) * adj_targeted_pm AS m202003,
(m202004/30.4375) * adj_targeted_pm AS m202004,
(m202005/30.4375) * adj_targeted_pm AS m202005,
(m202006/30.4375) * adj_targeted_pm AS m202006,
(m202007/30.4375) * adj_targeted_pm AS m202007,
(m202008/30.4375) * adj_targeted_pm AS m202008,
(m202009/30.4375) * adj_targeted_pm AS m202009,
(m202010/30.4375) * adj_targeted_pm AS m202010,
(m202011/30.4375) * adj_targeted_pm AS m202011,
(m202012/30.4375) * adj_targeted_pm AS m202012,
(m202101/30.4375) * adj_targeted_pm AS m202101
FROM one_a_cols
JOIN one_b_targeted_log ON one_b_targeted_log.apprentice_id = one_a_cols.apprentice_id
;

-- add_table_func
CREATE EXTENSION IF NOT EXISTS tablefunc;
-- one_b_cols_cum

CREATE OR REPLACE TEMPORARY VIEW one_b_cols_cum
AS
SELECT
apprentice_id,
m202002 AS cm202002 ,
m202002+m202003 AS cm202003 ,
m202002+m202003+m202004 AS cm202004 ,
m202002+m202003+m202004+m202005 AS cm202005 ,
m202002+m202003+m202004+m202005+m202006 AS cm202006 ,
m202002+m202003+m202004+m202005+m202006+m202007 AS cm202007 ,
m202002+m202003+m202004+m202005+m202006+m202007+m202008 AS cm202008 ,
m202002+m202003+m202004+m202005+m202006+m202007+m202008+m202009 AS cm202009 ,
m202002+m202003+m202004+m202005+m202006+m202007+m202008+m202009+m202010 AS cm202010 ,
m202002+m202003+m202004+m202005+m202006+m202007+m202008+m202009+m202010+m202011 AS cm202011 ,
m202002+m202003+m202004+m202005+m202006+m202007+m202008+m202009+m202010+m202011+m202012 AS cm202012 ,
m202002+m202003+m202004+m202005+m202006+m202007+m202008+m202009+m202010+m202011+m202012+m202101 AS cm202101
FROM one_b_cols;

-- one_b_cum

CREATE OR REPLACE TEMPORARY VIEW one_b_cum
AS
SELECT c.apprentice_id, t.month, t.cum_target
FROM one_b_cols_cum c
  CROSS JOIN LATERAL (
     VALUES
        (c.cm202101, '202101'),
(c.cm202012, '202012'),
(c.cm202011, '202011'),
(c.cm202010, '202010'),
(c.cm202009, '202009'),
(c.cm202008, '202008'),
(c.cm202007, '202007'),
(c.cm202006, '202006'),
(c.cm202005, '202005'),
(c.cm202004, '202004'),
(c.cm202003, '202003'),
(c.cm202002, '202002')
  ) AS t(cum_target, month)
ORDER BY apprentice_id, month;

-- staging_one_b

CREATE OR REPLACE TEMPORARY VIEW staging_one_b AS
SELECT *
FROM one_b_cum
;

-- one_c_log

CREATE OR REPLACE TEMPORARY VIEW one_c_log
AS
WITH sub AS (
    SELECT apprentice_id,
           to_timestamp(lms_training_epoch_time)::date             AS date,
           lms_training_type_id,
           sum(((lms_training_duration_seconds::float / 60) / 60)) AS training_hours
    FROM lms_training_log
             INNER JOIN apprentice_spine ON apprentice_spine.lms_user_id = lms_training_log.lms_user_id
    GROUP BY apprentice_id, date, lms_training_type_id
    ORDER BY apprentice_id, date
)
SELECT
    apprentice_id,
    to_char(date, 'YYYYMM') AS sdate,
    sum(training_hours) AS lgd_train_hrs
FROM sub
GROUP BY apprentice_id, sdate
;
;

-- one_c_cols

CREATE OR REPLACE TEMPORARY VIEW one_c_cols
AS
SELECT
       apprentice_id,
coalesce(sum(CASE WHEN sdate = '202002' THEN lgd_train_hrs ELSE null END),0) AS "m202002" ,
coalesce(sum(CASE WHEN sdate = '202003' THEN lgd_train_hrs ELSE null END),0) AS "m202003" ,
coalesce(sum(CASE WHEN sdate = '202004' THEN lgd_train_hrs ELSE null END),0) AS "m202004" ,
coalesce(sum(CASE WHEN sdate = '202005' THEN lgd_train_hrs ELSE null END),0) AS "m202005" ,
coalesce(sum(CASE WHEN sdate = '202006' THEN lgd_train_hrs ELSE null END),0) AS "m202006" ,
coalesce(sum(CASE WHEN sdate = '202007' THEN lgd_train_hrs ELSE null END),0) AS "m202007" ,
coalesce(sum(CASE WHEN sdate = '202008' THEN lgd_train_hrs ELSE null END),0) AS "m202008" ,
coalesce(sum(CASE WHEN sdate = '202009' THEN lgd_train_hrs ELSE null END),0) AS "m202009" ,
coalesce(sum(CASE WHEN sdate = '202010' THEN lgd_train_hrs ELSE null END),0) AS "m202010" ,
coalesce(sum(CASE WHEN sdate = '202011' THEN lgd_train_hrs ELSE null END),0) AS "m202011" ,
coalesce(sum(CASE WHEN sdate = '202012' THEN lgd_train_hrs ELSE null END),0) AS "m202012" ,
coalesce(sum(CASE WHEN sdate = '202101' THEN lgd_train_hrs ELSE null END),0) AS "m202101"
FROM one_c_log
GROUP BY apprentice_id
;

-- one_c_cols_cum

CREATE OR REPLACE TEMPORARY VIEW one_c_cols_cum
AS
SELECT
apprentice_id,
m202002 AS cm202002 ,
m202002+m202003 AS cm202003 ,
m202002+m202003+m202004 AS cm202004 ,
m202002+m202003+m202004+m202005 AS cm202005 ,
m202002+m202003+m202004+m202005+m202006 AS cm202006 ,
m202002+m202003+m202004+m202005+m202006+m202007 AS cm202007 ,
m202002+m202003+m202004+m202005+m202006+m202007+m202008 AS cm202008 ,
m202002+m202003+m202004+m202005+m202006+m202007+m202008+m202009 AS cm202009 ,
m202002+m202003+m202004+m202005+m202006+m202007+m202008+m202009+m202010 AS cm202010 ,
m202002+m202003+m202004+m202005+m202006+m202007+m202008+m202009+m202010+m202011 AS cm202011 ,
m202002+m202003+m202004+m202005+m202006+m202007+m202008+m202009+m202010+m202011+m202012 AS cm202012 ,
m202002+m202003+m202004+m202005+m202006+m202007+m202008+m202009+m202010+m202011+m202012+m202101 AS cm202101
FROM one_c_cols;

-- one_c_cum

CREATE OR REPLACE TEMPORARY VIEW one_c_cum
AS
SELECT c.apprentice_id, t.month, t.cum_logged
FROM one_c_cols_cum c
  CROSS JOIN LATERAL (
     VALUES
        (c.cm202101, '202101'),
(c.cm202012, '202012'),
(c.cm202011, '202011'),
(c.cm202010, '202010'),
(c.cm202009, '202009'),
(c.cm202008, '202008'),
(c.cm202007, '202007'),
(c.cm202006, '202006'),
(c.cm202005, '202005'),
(c.cm202004, '202004'),
(c.cm202003, '202003'),
(c.cm202002, '202002')
  ) AS t(cum_logged, month)
;

-- staging_one_c

CREATE OR REPLACE TEMPORARY VIEW staging_one_c AS
SELECT *
FROM one_c_cum;

-- e_etc

CREATE OR REPLACE TEMPORARY VIEW one_e_log
AS
WITH sub AS (
    SELECT lms_user_id::text           AS lms_user_id,
           lms_feedback_date::date     AS date,
           lms_feedback_response::text AS response
    FROM lms_user_fdbk_new_form
    union
    SELECT user_id::text       AS lms_user_id,
           feedback_date::date AS date,
           response::text
    FROM lms_user_fdbk_prev_form
    ORDER BY lms_user_id, date
)
SELECT apprentice_id, date, response
FROM sub
left JOIN apprentice_spine ON sub.lms_user_id::int = apprentice_spine.lms_user_id
ORDER BY apprentice_id, date
;

CREATE OR REPLACE TEMPORARY VIEW one_e_long_ages
AS
WITH sub_b AS (
    WITH sub_a AS (
        SELECT apprentice_id,
               date,
               response,
               (date_trunc('month', date) + interval '1 month' - interval '1 day')::date
                   AS end_of_month
        FROM one_e_log
    )
    SELECT apprentice_id,
           date,
           response,
           end_of_month,
           end_of_month - date AS age
    FROM sub_a
)
SELECT apprentice_id, date, response, end_of_month, age,
       min(age) over (partition by apprentice_id, end_of_month) AS min_age
FROM sub_b;

CREATE OR REPLACE TEMPORARY VIEW one_e_final
AS
SELECT
       apprentice_id,
       to_char(end_of_month, 'YYYYMM') AS month,
       replace(replace(response, ' - Extremely Likely', ''), ' - Extremely Unlikely', '')::int AS response
FROM one_e_long_ages
WHERE age = min_age
;

CREATE OR REPLACE TEMPORARY VIEW one_final
AS
    WITH sub_a AS (
        -- join everything up
        SELECT a.apprentice_id           AS id,
               a.month,
               coalesce(a.cum_days, 0)   AS cum_days,
               coalesce(b.cum_target, 0) AS cum_target,
               coalesce(c.cum_logged, 0) AS cum_logged,
               response
        FROM staging_one_a AS a
                 left OUTER JOIN staging_one_b AS b ON a.apprentice_id = b.apprentice_id and a.month = b.month
                 left OUTER JOIN staging_one_c AS c ON a.apprentice_id = c.apprentice_id and a.month = c.month
                 left OUTER JOIN one_e_final AS e ON a.apprentice_id = e.apprentice_id and a.month = e.month
        WHERE cum_days <> 0
        ORDER BY id, month
    )
-- this gets all our cols ready
    SELECT id,
           month,
           cum_days,
           cum_target,
           cum_logged,
           CASE WHEN cum_target <> 0 THEN round(((cum_logged / cum_target) * 100))::int ELSE null END AS pct_tgt_lgd,
           response
    FROM sub_a
;

SELECT *
INTO TEMPORARY TABLE staging_one
FROM one_final;

-- Covers off 2) a) and b) when used w/ sum() later
CREATE OR REPLACE TEMPORARY VIEW two_a_b
AS
WITH sub_b AS (
    WITH sub_a AS (
        SELECT id, month, programme_name, cum_days, cum_target, cum_logged, pct_tgt_lgd, response,
               '2020-12-31'::date AS ye,
               COALESCE(apprenticeship_start_date, CURRENT_DATE)       AS start_date,
               COALESCE(
                       LEAST(apprenticeship_withdrawal_date,
                             apprenticeship_completion_date)
                   , CURRENT_DATE)                                     AS end_date,
               COALESCE(apprenticeship_break_start_date, CURRENT_DATE) AS break_start,
               COALESCE(apprenticeship_break_end_date, '2000-01-01')   AS break_end
        FROM one_final
                 JOIN apprentice_spine ON one_final.id = apprentice_spine.apprentice_id
                 JOIN apprentice_dates ON one_final.id = apprentice_dates.apprentice_id
                JOIN cohorts ON apprentice_spine.cohort_id = cohorts.cohort_id
                JOIN programmes ON cohorts.programme_id = programmes.programme_id
        WHERE month = '202012'

    )
    SELECT
        -- break over whole period
        CASE WHEN break_start < ye AND break_end > ye THEN 0 ELSE 1 END AS break_mult,
        -- start date after period
        CASE WHEN start_date > ye THEN 0 ELSE 1 END                               AS not_started_mult,
        CASE WHEN end_date < ye THEN 0 ELSE 1 END                               AS ended_mult,
        id, month, programme_name, cum_days, cum_target, cum_logged, pct_tgt_lgd, response
    FROM sub_a
)
SELECT id, month, programme_name, cum_days, cum_target, cum_logged, pct_tgt_lgd, response,
    break_mult * not_started_mult * ended_mult AS live,
       CASE WHEN cum_logged >= 80 THEN 1 ELSE 0 END AS eighty_pct_plus_logged
FROM sub_b
;

-- Gives us average score to join in

CREATE OR REPLACE TEMPORARY VIEW two_d_avg
AS
SELECT
id, avg(response)
FROM one_final
GROUP BY id
ORDER BY id
;

CREATE OR REPLACE TEMPORARY VIEW two_d
AS
SELECT
       two_a_b.id, month, programme_name, pct_tgt_lgd, response, live, eighty_pct_plus_logged, avg,
       CASE WHEN avg >= 8 THEN 1 ELSE 0 END AS avg_eight_plus
FROM two_a_b
FULL OUTER JOIN two_d_avg
ON two_d_avg.id = two_a_b.id
;

CREATE OR REPLACE TEMPORARY VIEW
two_combo_a
AS
SELECT
       programme_name AS idx,
       sum(live) AS n_live
FROM two_a_b
GROUP BY programme_name
;

CREATE OR REPLACE TEMPORARY VIEW
two_combo_b
AS
WITH live_only AS (
    SELECT *
    FROM two_d
    WHERE live = 1
)
SELECT
    programme_name AS idx,
    sum(eighty_pct_plus_logged) AS n_eighty_pct_plus_logged,
    sum(avg_eight_plus) AS n_avg_eight_plus
FROM live_only
GROUP BY programme_name
;

CREATE OR REPLACE TEMPORARY VIEW two_final
AS
SELECT
       two_combo_a.idx as programme_name,
       '2020-12-31'::date AS date,
       n_live,
       n_eighty_pct_plus_logged,
       round(((n_eighty_pct_plus_logged::float / n_live::float)*100)) AS pct_eighty_pct_plus_logged,
       n_avg_eight_plus,
       round(((n_avg_eight_plus::float / n_live::float)*100)) AS pct_avg_eight_plus
FROM two_combo_a JOIN two_combo_b ON two_combo_a.idx = two_combo_b.idx
;

SELECT *
INTO TEMPORARY TABLE staging_two
FROM two_final;

-- show final outputs
SELECT * FROM staging_one;
SELECT * FROM staging_two;
