
WITH
    date_range AS (
        SELECT date::date AS date
        FROM
            generate_series(
                (SELECT min(date(created_at)) FROM linear.issue),
                current_date,
                '1 day'::interval
            ) date
    ),
    team_members AS (SELECT linear_user_id, id FROM static.team_members),
    issues_data AS (SELECT * FROM linear.issue)    
SELECT
    dr.date,
    la.member_id,
    li.title,
    li.description,
    li.url,
    p.name AS project_name,
    t.name AS team_name,
    li.priority_label,
    li.estimate,
    li.trashed,
    json_agg(la.activity) AS activities
FROM date_range dr
JOIN {{ref("issues_activity_map")}} la ON la.date = dr.date
JOIN linear.issue li ON la.issue_id = li.id AND la.issue_id = li.id
JOIN linear.team t ON t.id = li.team_id
JOIN linear.project p ON p.id = li.project_id
GROUP BY dr.date, la.member_id, li.title, li.description, li.url, li.project_id, li.team_id, li.priority_label, li.estimate, li.trashed, p.name, t.name
ORDER BY dr.date DESC