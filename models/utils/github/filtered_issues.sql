SELECT i.*
FROM github.issue i
JOIN {{ ref("filtered_repositories") }} fr ON fr.id = i.repository_id
ORDER BY i.created_at DESC
