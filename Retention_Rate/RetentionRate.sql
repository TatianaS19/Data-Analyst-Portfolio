select user_id, toString(start_date) as start_date, toString(date) as date, age,city,country,os,source from

(select user_id, min(toDate(time)) as start_date
from simulator_20250120.feed_actions
group by user_id 
having start_date>=today()-20
) t1

join

(select distinct user_id, toDate(time) as date, age,city,country,os,source
from simulator_20250120.feed_actions) t2

using user_id
