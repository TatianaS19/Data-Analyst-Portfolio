
SELECT this_week, toInt64(count(distinct user_id)*(-1)) AS count_users, status
FROM
(SELECT user_id,previous_week, this_week, weeks_visited, if(has(weeks_visited, this_week)=1, 'retained', 'gone') as status
FROM 
(SELECT distinct user_id, toDate(time) AS previous_week, weeks_visited, addWeeks(toDate(time), 1) as this_week FROM simulator_20250120.feed_actions 
LEFT JOIN (SELECT user_id, groupUniqArray(toDate(time)) as weeks_visited FROM simulator_20250120.feed_actions GROUP BY user_id) a USING(user_id)
) t1) t2
WHERE status='gone'
GROUP BY this_week, status

--УШЕДШИЕ ПОЛЬЗОВАТЕЛИ

UNION ALL

SELECT this_week, toInt64(count(distinct user_id)) AS count_users, status
FROM
(SELECT user_id,previous_week, this_week, weeks_visited, if(has(weeks_visited, previous_week)=1, 'retained', 'new') as status
FROM 
(SELECT distinct user_id, toDate(time) AS this_week, weeks_visited, addWeeks(toDate(time), -1) as previous_week FROM simulator_20250120.feed_actions 
LEFT JOIN (SELECT user_id, groupUniqArray(toDate(time)) as weeks_visited FROM simulator_20250120.feed_actions GROUP BY user_id) a USING(user_id)) t3) t4
GROUP BY this_week, status

--НОВЫЕ И СОХРАНЕННЫЕ ПОЛЬЗОВАТЕЛИ
