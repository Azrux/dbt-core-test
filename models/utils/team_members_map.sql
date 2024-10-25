{{ config(materialized='table') }}

WITH
    team_members_data AS (SELECT * FROM static.team_members),
    metadata AS (
        SELECT
            tm.display_name,
            tm.id,
            tm.first_name,
            tm.last_name,
            linear_user_id,
            github_user_id,
            s.id AS slack_id,
            s.handle AS slack_handle,
            gu.login AS github_handle,
            lu.display_name AS linear_handle,
            array_agg(
                CASE
                    WHEN tm.email NOT LIKE '%github.com%' THEN tm.email
                    ELSE NULL
                END
            ) FILTER (WHERE tm.email NOT LIKE '%github.com%') AS emails,  
            array_agg(
                CASE
                    WHEN tm.email LIKE '%github.com%' THEN tm.email
                    ELSE NULL
                END
            ) FILTER (WHERE tm.email LIKE '%github.com%') AS github_emails 
        FROM team_members_data tm
        LEFT JOIN static.slack s ON s.email = tm.id
        LEFT JOIN linear.users lu ON lu.id::varchar = tm.linear_user_id
        LEFT JOIN github.user gu ON gu.id::varchar = tm.github_user_id
        GROUP BY s.id, tm.display_name, linear_user_id, github_user_id, gu.login, lu.display_name, tm.id, tm.first_name,
            tm.last_name
    )
SELECT 
    id,
    first_name,
    last_name,
    display_name,
    jsonb_build_object(
        'emails', emails,
        'slack', jsonb_strip_nulls(jsonb_build_object(
            'id', slack_id,
            'handle', slack_handle
        )),
        'linear', jsonb_strip_nulls(jsonb_build_object(
            'id', linear_user_id,
            'handle', linear_handle
        )),
        'github', jsonb_strip_nulls(jsonb_build_object(
            'id', github_user_id,
            'handle', github_handle,
            'emails', github_emails
        ))
    ) AS metadata
FROM metadata
ORDER BY id