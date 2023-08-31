SELECT
    b.team_name,
    SUM(a.match_awayteam_score) AS goals
FROM
    fact_statistics AS a
LEFT JOIN
    dim_teams AS b ON a.match_awayteam_id = b.team_id
WHERE
    a.isfirsthalf = 0
    AND a.match_awayteam_id IS NOT NULL
GROUP BY
    b.team_name
ORDER BY
    goals DESC,
    team_name ASC;
