\set ON_ERROR_STOP on
\pset pager off
-- Set -v agent=... to the synthetic agent printed by run.ps1.
SELECT r.id, r.mode, r.state, a.active_id=r.id AS serving, r.decision, r.coordinator
FROM hospital_refresh_lab.releases r JOIN hospital_refresh_lab.agents a USING(agent)
WHERE r.agent=:'agent' ORDER BY r.id;

SELECT j.release_id, j.kind, j.instance, p.status, p.rows_processed, p.error
FROM hospital_refresh_lab.jobs j
JOIN hospital_refresh_lab.releases r ON r.id=j.release_id
LEFT JOIN ai.pipeline_runs p ON p.instance_id=j.instance
WHERE r.agent=:'agent' ORDER BY j.release_id,j.kind;

SELECT * FROM hospital_refresh_lab.search_chunks(:'agent','deductible');
