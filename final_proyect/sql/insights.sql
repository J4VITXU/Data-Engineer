
-- 1. Number of races and DNFs for Fernando Alonso
SELECT
  COUNT(*) AS total_races,
  SUM(CASE WHEN did_finish = 0 THEN 1 ELSE 0 END) AS dnfs
FROM fact_alonso_race_results;

-- 2. Average finishing position per season (only finished races)
SELECT
  s.year,
  AVG(a.race_position) AS avg_finish_position
FROM fact_alonso_race_results a
JOIN dim_season s ON s.season_id = a.season_id
WHERE a.did_finish = 1
GROUP BY s.year
ORDER BY s.year;

-- 3. Top 10 drivers by total wins
SELECT
  d.driver_name,
  COUNT(*) AS wins
FROM fact_race_winners w
JOIN dim_driver d ON d.driver_id = w.driver_id
GROUP BY d.driver_name
ORDER BY wins DESC
LIMIT 10;

-- 4. Total wins by team
SELECT
  t.team_name,
  COUNT(*) AS wins
FROM fact_race_winners w
JOIN dim_team t ON t.team_id = w.team_id
GROUP BY t.team_name
ORDER BY wins DESC;

-- 5. Fernando Alonso total wins
SELECT
  COUNT(*) AS alonso_wins
FROM fact_race_winners w
JOIN dim_driver d ON d.driver_id = w.driver_id
WHERE d.driver_name = 'Fernando Alonso';

-- 6. Alonso podium finishes
SELECT
  race_position,
  COUNT(*) AS times
FROM fact_alonso_race_results
WHERE did_finish = 1
  AND race_position <= 3
GROUP BY race_position
ORDER BY race_position;

-- 7. Races per continent
SELECT
  continent,
  COUNT(*) AS races
FROM dim_race
GROUP BY continent;

-- 8. Alonso grid vs finish performance
SELECT
  AVG(race_position - grid_position) AS avg_position_change
FROM fact_alonso_race_results
WHERE did_finish = 1;
