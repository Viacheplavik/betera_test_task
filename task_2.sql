WITH general_tab AS (
 SELECT bet_id, player_id, accepted_odd
   FROM bets b
   JOIN events e ON e.event_id = b.event_id
 WHERE sport = 'E-Sports'
 AND event_stage = 'Prematch'
 AND create_time >= '2022-03-14 12:00:00.000'
 AND amount >= 10
 AND settlement_time <= '2022-03-15 12:00:00.000'
 AND settlement_time NOT IN ('')
 AND settlement_time IS NOT NULL
 AND bet_type NOT IN ('System')
 AND is_free_bet = FALSE
 AND result IN ('Lose', 'Win')
)
SELECT DISTINCT (als_tab.player_id )
FROM (
  SELECT bet_id, player_id
  FROM general_tab
  WHERE accepted_odd >= 1.5
  EXCEPT
  SELECT bet_id, player_id
  FROM general_tab
  WHERE accepted_odd < 1.5) als_tab