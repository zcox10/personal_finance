-- What is the current ad load for users who listen to an hour of podcasts in a
-- day? (source: https://backstage.spotify.net/docs/free-pipelines/)
SELECT
   COUNT(user_id) AS user_count
  ,AVG(ad_load_total_mins_elapsed) AS ad_load_mins_avg
FROM
   `free-core-insights.mission_entities.free_user_entity_{LATEST}`
WHERE
   podcast_mins_played_today > 60
   AND is_dau = 1
