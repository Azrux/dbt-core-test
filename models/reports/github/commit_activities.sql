WITH 
    commit_data AS (
        SELECT
            DATE(c.committer_date) AS date,
            c.author_email,
            c.sha,
            r.name AS repository_name,
            c.message,
            cf.additions AS number_additions,
            cf.changes AS number_changes,
            cf.deletions AS number_deletions,
            jsonb_build_object(
                'filename', cf.filename,
                'additions', cf.additions,
                'changes', cf.changes,
                'deletions', cf.deletions,
                'status', cf.status
            ) AS changes,
            cf.filename
        FROM {{ref("filtered_commits")}} c
        JOIN {{ref("filtered_repositories")}} r ON c.repository_id = r.id
        JOIN github.branch_commit_relation b ON b.commit_sha = c.sha
        JOIN github.commit_file cf ON cf.commit_sha = c.sha
    )
SELECT 
    c.date,
    tm.id AS member_id,
    c.sha,
    c.repository_name,
    c.message,
    SUM(c.number_additions) AS total_additions,
    SUM(c.number_changes) AS total_changes,
    SUM(c.number_deletions) AS total_deletions,
    COUNT(DISTINCT c.filename) AS total_files_changed,
    ARRAY_AGG(c.changes) AS changes_array
FROM static.team_members tm
JOIN commit_data c ON c.author_email = tm.email
GROUP BY c.date, tm.id, c.sha, c.repository_name, c.message
ORDER BY c.date DESC