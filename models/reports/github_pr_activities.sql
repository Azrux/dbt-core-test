WITH 
    pr_data AS (
        SELECT
            pr.id AS pr_id,
            pr.issue_id,
            i.title,
            i.body AS description,
            br.name AS base_repo_name,
            hr.name AS head_repo_name,
            pr.created_at AS created_at,
            pr.updated_at AS updated_at,
            pr.closed_at AS closed_at,
            pr.base_ref AS base_branch_name,
            pr.head_ref AS head_branch_name
        FROM {{ref("filtered_prs")}} pr
        JOIN {{ref("filtered_issues")}} i ON pr.issue_id = i.id
        LEFT JOIN {{ref("filtered_repositories")}} br ON pr.base_repo_id = br.id
        LEFT JOIN {{ref("filtered_repositories")}} hr ON pr.head_repo_id = hr.id
    ),
    date_range as (
        select date::date as date
        from
            generate_series(
                (select min(date(author_date)) from github.commit),
                current_date,
                '1 day'::interval
            ) date
    ),
    create_action AS (
        SELECT
            pr.id AS pr_id,
            pr.created_at AS date,
            tm.id AS member_id,
            'create' AS action_type
        FROM {{ref("filtered_prs")}} pr
        JOIN {{ref("filtered_issues")}} i ON pr.issue_id = i.id
        JOIN static.team_members tm ON i.user_id::varchar = tm.github_user_id
    ),
    assign_pr_action AS (
        SELECT
            pr.id AS pr_id,
            ah.updated_at AS date,
            m.id  AS member_id,
            a.id AS assigner_id,
            'assign' AS action_type,
            ah.assigned
        FROM github.issue_assignee_history ah
        JOIN {{ref("filtered_prs")}} pr ON pr.issue_id = ah.issue_id
        LEFT JOIN static.team_members m ON m.github_user_id = ah.user_id::varchar
        LEFT JOIN static.team_members a ON a.github_user_id = ah.assigner_id::varchar
        WHERE ah.assigned = TRUE
    ),
    unassign_pr_action AS (
        SELECT
            pr.id AS pr_id,
            ah.updated_at AS date,
            m.id  AS member_id,
            a.id AS assigner_id,
            'assign' AS action_type,
            ah.assigned
        FROM github.issue_assignee_history ah
        JOIN {{ref("filtered_prs")}} pr ON pr.issue_id = ah.issue_id
        LEFT JOIN static.team_members m ON m.github_user_id = ah.user_id::varchar
        LEFT JOIN static.team_members a ON a.github_user_id = ah.assigner_id::varchar
        WHERE ah.assigned = FALSE
    ),
    comment_action AS (
        SELECT
            pr.id AS pr_id,
            c.updated_at AS date,
            c.body,
            tm.id AS member_id,
            'comment' AS action_type
        FROM github.issue_comment c
        JOIN {{ref("filtered_prs")}} pr ON pr.issue_id = c.issue_id
        JOIN static.team_members tm ON tm.github_user_id = c.user_id::varchar
    ),
    review_action AS (
        SELECT
            pr.id AS pr_id,
            GREATEST(
                r.submitted_at,
                rc.updated_at
            ) AS date,
            r.body,
            r.state,
            tm.id AS member_id,
            rc.id AS review_comment_id,
            jsonb_strip_nulls(jsonb_build_object(
                'comment_id', rc.id,
                'created_at', rc.created_at,
                'updated_at', rc.updated_at,
                'parent_comment', rc.parent_comment_id,
                'body', rc.body,
                'member_id', tmc.id
            )) AS comments,
            'review' AS action_type
        FROM github.pull_request_review r
        JOIN {{ref("filtered_prs")}} pr ON pr.id = r.pull_request_id
        LEFT JOIN static.team_members tm ON tm.github_user_id = r.user_id::varchar
        LEFT JOIN github.pull_request_review_comments rc on rc.pull_request_review_id = r.id
        LEFT JOIN static.team_members tmc ON tmc.github_user_id = rc.user_id::varchar
        GROUP BY r.id, rc.id, date, r.body, r.state, tm.id, comments, pr.id
    ),
    merge_action AS (
        SELECT
            pr.id AS pr_id, 
            merged_at AS date,
            tm.id AS member_id,
            'merge' AS action_type
        FROM github.issue_merged m
        JOIN {{ref("filtered_prs")}} pr ON pr.issue_id = m.issue_id
        JOIN static.team_members tm ON tm.github_user_id = m.actor_id::varchar
    ),
    rename_action AS (
        SELECT 
            pr.id AS pr_id,
            r.updated_at,
            from_name,
            to_name,
            tm.id AS member_id,
            'rename' AS action_type
        FROM github.issue_renamed r
        JOIN {{ref("filtered_prs")}} pr ON pr.issue_id = r.issue_id
        JOIN static.team_members tm ON tm.github_user_id = r.actor_id::varchar
    ),
    add_review_request_action AS (
        SELECT
            rr.created_at AS date,
            pr.id AS pr_id,
            tmr.id AS requested_member_id,
            tm.id AS member_id,
            rr.removed
        FROM github.requested_reviewer_history rr 
        JOIN {{ref("filtered_prs")}} pr ON pr.id = rr.pull_request_id
        JOIN static.team_members tm ON tm.github_user_id = rr.actor_id::varchar
        JOIN static.team_members tmr ON tmr.github_user_id = rr.requested_id::varchar
        WHERE rr.removed = FALSE
    ),
    remove_review_request_action AS (
        SELECT
            rr.created_at AS date,
            pr.id AS pr_id,
            tmr.id AS requested_member_id,
            tm.id AS member_id,
            rr.removed
        FROM github.requested_reviewer_history rr 
        JOIN {{ref("filtered_prs")}} pr ON pr.id = rr.pull_request_id
        JOIN static.team_members tm ON tm.github_user_id = rr.actor_id::varchar
        JOIN static.team_members tmr ON tmr.github_user_id = rr.requested_id::varchar
        WHERE rr.removed = TRUE
    ),
    activity AS (
        SELECT DISTINCT ON (pr_id, date, member_id)
            pr_id,
            date,
            member_id,
            jsonb_build_object(
                'action_type', 'remove_review_request',
                'unassigned_member', requested_member_id
            ) AS action
        FROM remove_review_request_action

        UNION ALL

        SELECT DISTINCT ON (pr_id, date, member_id)
            pr_id,
            date,
            member_id,
            jsonb_build_object(
                'action_type', 'add_review_request',
                'assigned_member', requested_member_id
            ) AS action
        FROM add_review_request_action

        UNION ALL

        SELECT DISTINCT ON (pr_id, date, member_id)
            pr_id,
            date,
            member_id,
            jsonb_build_object(
                'action_type', 'create'
            ) AS action
        FROM create_action

        UNION ALL

        SELECT DISTINCT ON (pr_id, date, member_id)
            pr_id,
            date,
            member_id,
            jsonb_build_object(
                'action_type', 'assign_pr',
                'assigned_member', member_id
            ) AS action
        FROM assign_pr_action

        UNION ALL

        SELECT DISTINCT ON (pr_id, date, member_id)
            pr_id,
            date,
            member_id,
            jsonb_build_object(
                'action_type', 'unassign_pr',
                'unassigned_member', member_id
            ) AS action
        FROM unassign_pr_action

        UNION ALL

        SELECT DISTINCT ON (pr_id, date, member_id)
            pr_id,
            date,
            member_id,
            jsonb_build_object(
                'action_type', 'comment',
                'body', body
            ) AS action
        FROM comment_action

        UNION ALL

        SELECT DISTINCT ON (pr_id, date, member_id)
            pr_id,
            date,
            member_id,
            jsonb_build_object(
                'action_type', 'review',
                'state', state,
                'body', body,
                'comments', comments
            ) AS action
        FROM review_action

        UNION ALL

        SELECT DISTINCT ON (pr_id, date, member_id)
            pr_id,
            date,
            member_id,
            jsonb_build_object(
                'action_type', 'merge'
            ) AS action
        FROM merge_action

        UNION ALL

        SELECT DISTINCT ON (pr_id, date, member_id)
            pr_id,
            updated_at AS date,
            member_id,
            jsonb_build_object(
                'action_type', 'rename',
                'from_name', from_name,
                'to_name', to_name
            ) AS action
        FROM rename_action
    ),
    final_activity AS (
        SELECT
            DATE(date) AS date,
            pr_id,
            member_id,
            jsonb_agg(action ORDER BY date DESC) AS actions
        FROM activity
        GROUP BY date(date), pr_id, member_id
    )
SELECT 
    dr.date,
    fa.member_id,
    pr.pr_id,
    issue_id,
    title,
    description,
    base_repo_name,
    head_repo_name,
    base_branch_name,
    head_branch_name,
    actions AS activities
FROM date_range dr
JOIN pr_data pr ON 1 = 1
JOIN final_activity fa ON fa.pr_id = pr.pr_id AND dr.date = fa.date
WHERE fa.member_id IS NOT NULL
GROUP BY 
    dr.date,
    fa.member_id,
    pr.updated_at,
    pr.pr_id,
    issue_id,
    title,
    description,
    base_repo_name,
    head_repo_name,
    base_branch_name,
    head_branch_name,
    actions
ORDER BY date DESC, member_id