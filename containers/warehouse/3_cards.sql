WITH cards AS (
    SELECT
        match_referee,
        card,
        COUNT(*) AS total_cards
    FROM
        fact_cards
    WHERE
        card IS NOT NULL
    GROUP BY
        match_referee,
        card
    ORDER BY
        match_referee
)
SELECT
    match_referee,
    SUM(total_cards) AS cards_issued
FROM
    cards
GROUP BY
    match_referee
ORDER BY
    cards_issued DESC,
    match_referee ASC;
