select user_id, toString(start_date) as start_date, toString(date) as date, age,city,country,os,source from
#рассматриваем пользователей, которые впервые воспользовались приложением 20 дней назад
#таблица feed_actions это активность пользователей
(select user_id, min(toDate(time)) as start_date
from simulator_20250120.feed_actions
group by user_id 
having start_date>=today()-20
) t1

join
#узнаем и приджойним их дальнейшую активность
(select distinct user_id, toDate(time) as date, age,city,country,os,source
from simulator_20250120.feed_actions) t2

using user_id
