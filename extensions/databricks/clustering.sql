ALTER TABLE workspace.posthog.events CLUSTER BY (team_id, timestamp, event, distinct_id);
ALTER TABLE workspace.posthog.persons CLUSTER BY (team_id, id);
ALTER TABLE workspace.posthog.person_distinct_ids CLUSTER BY (team_id, distinct_id);
ALTER TABLE workspace.posthog.groups CLUSTER BY (team_id, group_type_index, group_key);
ALTER TABLE workspace.posthog.cohort_members CLUSTER BY (team_id, cohort_id, person_id);
