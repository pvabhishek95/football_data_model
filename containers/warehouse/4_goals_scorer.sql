WITH cte AS (
    SELECT
        home_scorer_id AS scorer_id,
        COUNT(*) AS goals_scored
    FROM
        fact_matches
    WHERE
        home_scorer_id != 0
    GROUP BY
        home_scorer_id
    UNION
    SELECT
        away_scorer_id AS scorer_id,
        COUNT(*) AS goals_scored
    FROM
        fact_matches
    WHERE
        away_scorer_id != 0
    GROUP BY
        away_scorer_id
)
SELECT
    a.scorer_id,
    b.player_name,
    c.team_name,
    SUM(goals_scored) AS goals
FROM
    cte AS a
LEFT JOIN
    dim_players AS b ON a.scorer_id = b.player_id
LEFT JOIN
    dim_teams AS c ON c.team_id = b.team_id
GROUP BY
    scorer_id,
    player_name,
    team_name
ORDER BY
    goals DESC
LIMIT 3;
