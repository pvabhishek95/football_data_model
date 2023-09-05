WITH statistics AS (
    SELECT
        match_hometeam_id AS team_id,
        COUNT(*) AS matches_won,
        0 AS matches_drawn,
        0 AS match_defeated,
        COUNT(*) * 3 AS points,
        SUM(match_hometeam_score) AS goals_scored,
        SUM(match_hometeam_score_conceded) AS goals_conceded
    FROM
        fact_statistics
    WHERE
        isfirsthalf = 0 AND outcome = 'Home Win'
    GROUP BY
        match_hometeam_id
    UNION
    SELECT
        match_awayteam_id AS team_id,
        COUNT(*) AS matches_won,
        0 AS matches_drawn,
        0 AS match_defeated,
        COUNT(*) * 3 AS points,
        SUM(match_awayteam_score) AS goals_scored,
        SUM(match_awayteam_score_conceded) AS goals_conceded
    FROM
        fact_statistics
    WHERE
        isfirsthalf = 0 AND outcome = 'Away Win'
    GROUP BY
        match_awayteam_id
    UNION
    SELECT
        match_awayteam_id AS team_id,
        0 AS matches_won,
        COUNT(*) AS matches_drawn,
        0 AS match_defeated,
        COUNT(*) * 1 AS points,
        SUM(match_awayteam_score) AS goals_scored,
        SUM(match_awayteam_score_conceded) AS goals_conceded
    FROM
        fact_statistics
    WHERE
        isfirsthalf = 0 AND outcome = 'Draw'
    GROUP BY
        match_awayteam_id
    UNION
    SELECT
        match_hometeam_id AS team_id,
        0 AS matches_won,
        COUNT(*) AS matches_drawn,
        0 AS match_defeated,
        COUNT(*) * 1 AS points,
        SUM(match_hometeam_score) AS goals_scored,
        SUM(match_hometeam_score_conceded) AS goals_conceded
    FROM
        fact_statistics
    WHERE
        isfirsthalf = 0 AND outcome = 'Draw'
    GROUP BY
        match_hometeam_id
    UNION
    SELECT
        match_awayteam_id AS team_id,
        0 AS matches_won,
        0 AS matches_drawn,
        COUNT(*) AS match_defeated,
        COUNT(*) * 0 AS points,
        0 AS goals_scored,
        0 AS goals_conceded
    FROM
        fact_statistics
    WHERE
        isfirsthalf = 0 AND outcome = 'Home Win'
    GROUP BY
        match_awayteam_id
    UNION
    SELECT
        match_hometeam_id AS team_id,
        0 AS matches_won,
        0 AS matches_drawn,
        COUNT(*) AS match_defeated,
        COUNT(*) * 0 AS points,
        0 AS goals_scored,
        0 AS goals_conceded
    FROM
        fact_statistics
    WHERE
        isfirsthalf = 0 AND outcome = 'Away Win'
    GROUP BY
        match_hometeam_id
),
agg AS (
    SELECT
        a.team_id,
        b.team_name,
        SUM(matches_won) AS matches_won,
        SUM(matches_drawn) AS matches_drawn,
        SUM(match_defeated) AS matches_defeated,
        SUM(points) AS points,
        SUM(goals_scored) AS goals_scored,
        SUM(goals_conceded) AS goals_conceded
    FROM
        statistics AS a
    LEFT JOIN
        dim_teams AS b ON a.team_id = b.team_id
    GROUP BY
        a.team_id,
        b.team_name
),
agg_1 AS (
    SELECT
        team_name,
        (matches_won + matches_drawn + matches_defeated) AS matches_played,
        matches_won AS won,
        matches_drawn AS drawn,
        matches_defeated AS defeated,
        goals_scored,
        goals_conceded,
        (goals_scored - goals_conceded) AS goal_difference,
        points
    FROM
        agg
)
SELECT
    DENSE_RANK() OVER (ORDER BY points DESC, goal_difference DESC, goals_scored DESC, won DESC) AS position,
    *
FROM
    agg_1;