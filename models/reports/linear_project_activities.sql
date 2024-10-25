WITH
    date_range AS (
        SELECT date::date AS date
        FROM
            generate_series(
                (SELECT LEAST(MIN(updated_at), MIN(created_at)) FROM linear.document),
                current_date,
                '1 day'::interval
            ) date
    ),
    create_document AS (
        SELECT 
            DATE(d.created_at) AS date,
            tm.id AS member_id,
            d.project_id,
            jsonb_build_object(
                'action_type', 'create_document',
                'title', d.title,
                'content', d.content
            ) AS activity
        FROM linear.document d 
        JOIN static.team_members tm ON d.creator_id = tm.linear_user_id
        GROUP BY tm.id, d.id, DATE(d.created_at), d.title, d.content
    ),
    update_document AS (
        SELECT 
            DATE(d.updated_at) AS date,
            tm.id AS member_id,
            d.project_id,
            jsonb_build_object(
                'action_type', 'update_document',
                'title', d.title,
                'content', d.content
            ) AS activity
        FROM linear.document d 
        JOIN static.team_members tm ON d.updated_by = tm.linear_user_id
        GROUP BY tm.id, d.id, DATE(d.updated_at), d.title, d.content
    ),
    create_project AS (
        SELECT 
            DATE(p.created_at) AS date,
            tm.id AS member_id,
            p.id AS project_id,
            jsonb_build_object(
                'action_type', 'create_project'
            ) AS activity
        FROM linear.project p
        JOIN static.team_members tm ON p.creator_id = tm.linear_user_id 
        GROUP BY DATE(p.created_at), p.id, tm.id
    ),
    add_link AS (
        SELECT 
            DATE(p.created_at) AS date,
            tm.id AS member_id,
            p.project_id,
            jsonb_build_object(
                'action_type', 'add_link',
                'link_url', p.url,
                'link_label', p.label
            ) AS activity
        FROM linear.project_link p
        JOIN static.team_members tm ON p.creator_id = tm.linear_user_id
        GROUP BY DATE(p.created_at), project_id, tm.id, p.url, p.label
    ),
    activity AS (
        SELECT DISTINCT ON (project_id, date, member_id) * FROM create_document
        UNION ALL
        SELECT DISTINCT ON (project_id, date, member_id) * FROM update_document
        UNION ALL
        SELECT DISTINCT ON (project_id, date, member_id) * FROM create_project
        UNION ALL
        SELECT DISTINCT ON (project_id, date, member_id) * FROM add_link
    ),
    final_activities AS (
        SELECT
            date,
            member_id,
            project_id,
            jsonb_agg(activity ORDER BY date DESC) AS activities
        FROM activity
        GROUP BY date, project_id, member_id
    )
SELECT
    dr.date,
    a.member_id,
    tm.id AS lead,
    p.name,
    p.content, 
    p.description,
    p.url,
    p.progress,
    p.state,
    p.target_date,
    a.activities AS activities
FROM date_range dr
JOIN final_activities a ON a.date = dr.date
JOIN linear.project p ON p.id = a.project_id
JOIN static.team_members tm ON tm.linear_user_id = p.lead_id
GROUP BY
    dr.date,
    a.member_id,
    tm.id,
    p.name,
    p.content,
    p.description,
    a.activities,
    p.url,
    p.progress,
    p.state,
    p.target_date
ORDER BY dr.date DESC