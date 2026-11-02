SELECT uuid, team_id, event, cast(timestamp AS TIMESTAMP) AS timestamp, distinct_id, person_id, properties, person_properties, cast(person_created_at AS TIMESTAMP) AS person_created_at FROM VALUES
('1',7,'signup','2026-01-01 00:00:00','a','a','{"amount":10,"plan":"pro"}','{}','2026-01-01 00:00:00'),
('2',7,'buy','2026-01-01 00:10:00','a','a','{"amount":10,"plan":"pro"}','{}','2026-01-01 00:00:00'),
('3',7,'buy','2026-01-02 01:00:00','a','a','{"amount":10,"plan":"pro"}','{}','2026-01-01 00:00:00'),
('4',7,'signup','2026-01-01 00:00:00','b','b','{}','{}','2026-01-01 00:00:00'),
('5',7,'buy','2026-01-03 01:00:00','b','b','{}','{}','2026-01-01 00:00:00'),
('6',7,'buy','2025-12-30 01:00:00','c','c','{}','{}','2025-12-30 00:00:00'),
('7',7,'buy','2026-01-01 01:00:00','c','c','{}','{}','2025-12-30 00:00:00'),
('8',7,'buy','2026-01-03 01:00:00','c','c','{}','{}','2025-12-30 00:00:00'),
('9',999,'buy','2026-01-01 01:00:00','x','x','{}','{}','2026-01-01 00:00:00')
AS sample(uuid,team_id,event,timestamp,distinct_id,person_id,properties,person_properties,person_created_at);
