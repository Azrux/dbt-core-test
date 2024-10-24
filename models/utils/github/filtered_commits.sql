SELECT c.*
FROM github.commit c
JOIN {{ ref("filtered_repositories") }} fr ON fr.id = c.repository_id
ORDER BY c.committer_date DESC
