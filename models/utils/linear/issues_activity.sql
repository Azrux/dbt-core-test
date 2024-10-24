/*
    - Each CTE:
        - member_id (actor_id)
        - date
        - issue_id
        - JSON(activity_data) AS activity
*/

WITH
    update_description AS (
        SELECT
            tm.id AS member_id,
            DATE(ih.updated_at) AS date,
            ih.issue_id,
            jsonb_build_object(
                'action_type', 'update_description'
            ) AS activity
        FROM linear.issue_history ih
        JOIN static.team_members tm ON ih.actor_id =  tm.linear_user_id
        LEFT JOIN LATERAL jsonb_array_elements(ih.changes->'descriptionUpdatedByIds') AS user_id ON tm.linear_user_id = user_id::varchar
        WHERE updated_description = TRUE and ih.changes->>'descriptionUpdatedByIds' IS NOT NULL 
        GROUP BY tm.id, DATE(ih.updated_at), ih.issue_id
    ),
    update_title AS (
        SELECT
            tm.id AS member_id,
            DATE(ih.updated_at) AS date,
            ih.issue_id,
            jsonb_build_object(
                'action_type', 'update_title'
            ) AS activity
        FROM linear.issue_history ih
        JOIN static.team_members tm ON ih.actor_id =  tm.linear_user_id
        WHERE ih.from_title IS NOT NULL AND ih.to_title IS NOT NULL
        GROUP BY tm.id, DATE(ih.updated_at), ih.issue_id
    ),
    add_labels AS (
        SELECT
            tm.id AS member_id,
            DATE(ih.updated_at) AS date,
            ih.issue_id,
            jsonb_build_object(
                'action_type', 'add_labels',
                'labels_name', jsonb_agg(DISTINCT l.name)
            ) AS activity
        FROM linear.issue_history ih
        LEFT JOIN LATERAL jsonb_array_elements_text(ih.added_label_ids) AS labels ON TRUE
        JOIN static.team_members tm ON ih.actor_id = tm.linear_user_id
        LEFT JOIN linear.label l ON labels = l.id
        WHERE ih.added_label_ids IS NOT NULL AND jsonb_array_length(ih.added_label_ids) > 0 
        GROUP BY tm.id, DATE(ih.updated_at), ih.issue_id
    ),
    remove_labels AS (
        SELECT
            tm.id AS member_id,
            DATE(ih.updated_at) AS date,
            ih.issue_id,
            jsonb_build_object(
                'action_type', 'remove_labels',
                'labels_name', jsonb_agg(DISTINCT l.name)
            ) AS activity
        FROM linear.issue_history ih
        LEFT JOIN LATERAL jsonb_array_elements_text(ih.removed_label_ids) AS labels ON TRUE
        JOIN static.team_members tm ON ih.actor_id = tm.linear_user_id
        LEFT JOIN linear.label l ON labels = l.id
        WHERE ih.removed_label_ids IS NOT NULL AND jsonb_array_length(ih.removed_label_ids) > 0 
        GROUP BY tm.id, DATE(ih.updated_at), ih.issue_id
    ),
    change_team AS (
        SELECT
            tm.id AS member_id,
            DATE(ih.updated_at) AS date,
            ih.issue_id,
            jsonb_build_object(
                'action_type', 'change_team',
                'from_team', from_t.name,
                'to_team', to_t.name
            ) AS activity
        FROM linear.issue_history ih
        JOIN static.team_members tm ON ih.actor_id = tm.linear_user_id
        JOIN linear.team as from_t ON from_t.id = from_team_id
        JOIN linear.team as to_t ON to_t.id = to_team_id
        WHERE ih.from_team_id IS NOT NULL AND ih.to_team_id IS NOT NULL
        GROUP BY tm.id, DATE(ih.updated_at), ih.issue_id, from_t.name, to_t.name
    ),
    change_status AS (
        SELECT
            tm.id AS member_id,
            DATE(ih.updated_at) AS date,
            ih.issue_id,
            jsonb_build_object(
                'action_type', 'change_status',
                'from_status', from_s.name,
                'to_status', to_s.name
            ) AS activity
        FROM linear.issue_history ih
        JOIN static.team_members tm ON ih.actor_id = tm.linear_user_id
        JOIN linear.workflow_state as from_s ON from_s.id = from_state_id
        JOIN linear.workflow_state as to_s ON to_s.id = to_state_id
        WHERE ih.from_state_id IS NOT NULL AND ih.to_state_id IS NOT NULL
        GROUP BY tm.id, DATE(ih.updated_at), ih.issue_id, from_s.name, to_s.name
    ),
    change_project AS (
        SELECT
            tm.id AS member_id,
            DATE(ih.updated_at) AS date,
            ih.issue_id,
            jsonb_build_object(
                'action_type', 'change_project',
                'from_project', from_p.name,
                'to_project', to_p.name
            ) AS activity
        FROM linear.issue_history ih
        JOIN static.team_members tm ON ih.actor_id = tm.linear_user_id
        JOIN linear.project as from_p ON from_p.id = from_project_id
        JOIN linear.project as to_p ON to_p.id = to_project_id
        WHERE ih.from_project_id IS NOT NULL AND ih.to_project_id IS NOT NULL
        GROUP BY tm.id, DATE(ih.updated_at), ih.issue_id, from_p.name, to_p.name
    ), 
    change_due_date AS (
        SELECT
            tm.id AS member_id,
            DATE(ih.updated_at) AS date,
            ih.issue_id,
            jsonb_build_object(
                'action_type', 'change_due_date',
                'from_due_date', from_due_date,
                'to_due_date', to_due_date
            ) AS activity
        FROM linear.issue_history ih
        JOIN static.team_members tm ON ih.actor_id = tm.linear_user_id
        WHERE ih.from_due_date IS NOT NULL AND ih.to_due_date IS NOT NULL
        GROUP BY tm.id, DATE(ih.updated_at), ih.issue_id, from_due_date, to_due_date
    ),
    change_estimate AS (
        SELECT
            tm.id AS member_id,
            DATE(ih.updated_at) AS date,
            ih.issue_id,
            jsonb_build_object(
                'action_type', 'change_estimate',
                'from_estimate', from_estimate,
                'to_estimate', to_estimate
            ) AS activity
        FROM linear.issue_history ih
        JOIN static.team_members tm ON ih.actor_id = tm.linear_user_id
        WHERE ih.from_estimate IS NOT NULL AND ih.to_estimate IS NOT NULL
        GROUP BY tm.id, DATE(ih.updated_at), ih.issue_id, from_estimate, to_estimate
    ),
    change_assignee AS (
        SELECT
            tm.id AS member_id,
            DATE(ih.updated_at) AS date,
            ih.issue_id,
            jsonb_build_object(
                'action_type', 'change_assignee',
                'from_assignee', from_a.id,
                'to_assignee', to_a.id
            ) AS activity
        FROM linear.issue_history ih
        JOIN static.team_members tm ON ih.actor_id = tm.linear_user_id
        JOIN static.team_members as from_a ON from_a.linear_user_id = from_assignee_id
        JOIN static.team_members as to_a ON to_a.linear_user_id = to_assignee_id
        WHERE ih.from_assignee_id IS NOT NULL AND ih.to_assignee_id IS NOT NULL
        GROUP BY tm.id, DATE(ih.updated_at), ih.issue_id, from_a.id, to_a.id
    ), 
    change_priority AS (
        SELECT
            tm.id AS member_id,
            DATE(ih.updated_at) AS date,
            ih.issue_id,
            jsonb_build_object(
                'action_type', 'change_priority',
                'from_priority', from_priority,
                'to_priority', to_priority
            ) AS activity
        FROM linear.issue_history ih
        JOIN static.team_members tm ON ih.actor_id = tm.linear_user_id
        WHERE ih.from_priority IS NOT NULL AND ih.to_priority IS NOT NULL
        GROUP BY tm.id, DATE(ih.updated_at), ih.issue_id, from_priority, to_priority
    ),
    create_issue AS (
        SELECT
            tm.id AS member_id,
            DATE(i.created_at) AS date,
            i.id AS issue_id,
            jsonb_build_object(
                'action_type', 'create_issue'
            ) AS activity
        FROM linear.issue i
        JOIN static.team_members tm ON i.creator_id = tm.linear_user_id
        GROUP BY tm.id, DATE(i.created_at), i.id
    ), 
    start_issue AS (
        SELECT
            tm.id AS member_id,
            DATE(i.started_at) AS date,
            i.id AS issue_id,
            jsonb_build_object(
                'action_type', 'start_issue'
            ) AS activity
        FROM linear.issue i
        JOIN static.team_members tm ON i.assignee_id = tm.linear_user_id
        WHERE started_at IS NOT NULL
        GROUP BY tm.id, DATE(i.started_at), i.id
    ),
    move_to_triage AS (
        SELECT
            tm.id AS member_id,
            DATE(i.started_triage_at) AS date,
            i.id AS issue_id,
            jsonb_build_object(
                'action_type', 'move_to_triage'
            ) AS activity
        FROM linear.issue i
        JOIN static.team_members tm ON i.assignee_id = tm.linear_user_id
        WHERE started_triage_at IS NOT NULL
        GROUP BY tm.id, DATE(i.started_triage_at), i.id
    ),
    remove_from_triage AS (
        SELECT
            tm.id AS member_id,
            DATE(i.triaged_at) AS date,
            i.id AS issue_id,
            jsonb_build_object(
                'action_type', 'remove_from_triage'
            ) AS activity
        FROM linear.issue i
        JOIN static.team_members tm ON i.assignee_id = tm.linear_user_id
        WHERE triaged_at IS NOT NULL
        GROUP BY tm.id, DATE(i.triaged_at), i.id
    ),
    complete_issue AS (
        SELECT
            tm.id AS member_id,
            DATE(i.completed_at) AS date,
            i.id AS issue_id,
            jsonb_build_object(
                'action_type', 'complete_issue'
            ) AS activity
        FROM linear.issue i
        JOIN static.team_members tm ON i.assignee_id = tm.linear_user_id
        WHERE completed_at IS NOT NULL
        GROUP BY tm.id, DATE(i.completed_at), i.id
    ),
    cancel_issue AS (
        SELECT
            tm.id AS member_id,
            DATE(i.canceled_at) AS date,
            i.id AS issue_id,
            jsonb_build_object(
                'action_type', 'cancel_issue'
            ) AS activity
        FROM linear.issue i
        JOIN static.team_members tm ON i.assignee_id = tm.linear_user_id
        WHERE canceled_at IS NOT NULL
        GROUP BY tm.id, DATE(i.canceled_at), i.id
    ),
    add_attachment AS (
        SELECT
            tm.id AS member_id,
            DATE(i.updated_at) AS date,
            i.id AS issue_id,
            jsonb_build_object(
                'action_type', 'add_attachment',
                'title', a.title,
                'source', a.source,
                'url', a.url
            ) AS activity
        FROM linear.issue i
        JOIN static.team_members tm ON i.assignee_id = tm.linear_user_id
        JOIN linear.attachment a ON a.issue_id = i.id
        GROUP BY tm.id, DATE(i.updated_at), i.id, a.title, a.source, a.url
    ),
    archive_issue AS (
        SELECT
            tm.id AS member_id,
            DATE(i.archived_at) AS date,
            i.id AS issue_id,
            jsonb_build_object(
                'action_type', 'archive_issue'
            ) AS activity
        FROM linear.issue i
        JOIN static.team_members tm ON i.assignee_id = tm.linear_user_id
        WHERE archived_at IS NOT NULL
        GROUP BY tm.id, DATE(i.archived_at), i.id
    ),
    create_comment AS (
        SELECT
            tm.id AS member_id,
            DATE(c.created_at) AS date,
            i.id AS issue_id,
            jsonb_build_object(
                'action_type', 'create_comment',
                'content', c.body,
                'parent_comment_content', pc.body,
                'url', c.url
            ) AS activity
        FROM linear.issue i
        JOIN linear.comment c ON c.issue_id = i.id
        JOIN static.team_members tm ON c.user_id = tm.linear_user_id
        LEFT JOIN linear.comment pc ON pc.id = c.parent_id
        GROUP BY tm.id, DATE(c.created_at), i.id, c.body, pc.body, c.url
    ),
    comment_reaction AS (
        SELECT
            tm.id AS member_id,
            DATE(reaction.value->>'reactedAt') AS date,
            i.id AS issue_id,
            jsonb_build_object(
                'action_type', 'comment_reaction',
                'reaction_emoji', r.value->>'emoji',
                'comment_reacted_to', c.body,
                'comment_url', c.url
            ) AS activity
        FROM linear.issue i
        JOIN linear.comment c ON c.issue_id = i.id
        LEFT JOIN LATERAL jsonb_array_elements(c.reaction_data) AS r ON true
        LEFT JOIN LATERAL jsonb_array_elements(r.value->'reactions') AS reaction ON true
        LEFT JOIN static.team_members tm ON tm.linear_user_id = reaction.value->>'userId'
        WHERE jsonb_array_length(c.reaction_data) > 0
        GROUP BY tm.id, DATE(reaction.value->>'reactedAt'), i.id, c.url, c.body, r.value
    ),
    edit_comment AS (
        SELECT
            tm.id AS member_id,
            DATE(c.edited_at) AS date,
            i.id AS issue_id,
            jsonb_build_object(
                'action_type', 'edit_comment',
                'content', c.body,
                'parent_comment_content', pc.body,
                'url', c.url
            ) AS activity
        FROM linear.issue i
        JOIN linear.comment c ON c.issue_id = i.id
        JOIN static.team_members tm ON c.user_id = tm.linear_user_id
        LEFT JOIN linear.comment pc ON pc.id = c.parent_id
        WHERE c.edited_at IS NOT NULL
        GROUP BY tm.id, DATE(c.edited_at), i.id, c.body, pc.body, c.url
    )
SELECT * FROM update_description
UNION ALL
SELECT * FROM update_title
UNION ALL
SELECT * FROM add_labels
UNION ALL
SELECT * FROM remove_labels
UNION ALL
SELECT * FROM change_team
UNION ALL
SELECT * FROM change_status
UNION ALL
SELECT * FROM change_project
UNION ALL
SELECT * FROM change_due_date
UNION ALL
SELECT * FROM change_estimate
UNION ALL
SELECT * FROM change_assignee
UNION ALL
SELECT * FROM change_priority
UNION ALL
SELECT * FROM create_issue
UNION ALL
SELECT * FROM start_issue
UNION ALL
SELECT * FROM move_to_triage
UNION ALL     
SELECT * FROM remove_from_triage
UNION ALL  
SELECT * FROM cancel_issue
UNION ALL  
SELECT * FROM add_attachment
UNION ALL  
SELECT * FROM archive_issue
UNION ALL  
SELECT * FROM create_comment
UNION ALL  
SELECT * FROM comment_reaction
UNION ALL  
SELECT * FROM edit_comment
ORDER BY date DESC