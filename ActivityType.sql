with dailyavg as (
select session_id, timestamp_date,avg(lat) as lat,avg(lng) as lng from malliq_analytics_samsung.find_mall_request
where timestamp_date>='2021-01-01'
and lat is not null
and lng is not null
and lat != 0
and lng != 0
group by 1,2)

select session_id,
timestamp_date,
lat,
lng,
lag(lat, 1) over (partition by session_id order by session_id,timestamp_date) as lag_lat,
lag(lng, 1) over (partition by session_id order by session_id,timestamp_date) as lag_lng,
u.home_long as home_lng,
u.home_lat as home_latt,
f_great_circle_distance(lat, lng, lag_lat, lag_lng) / 1000.0   as distance_traveled_km,
f_great_circle_distance(lat, lng,home_latt, home_lng) / 1000.0    as distancetohome,
CASE
WHEN distance_traveled_km >= 150 then 'travel'
WHEN distance_traveled_km < 150 then 'nontravel' end  as activity_type,
CASE
WHEN activity_type='travel' and distancetohome <= 100 then 'backfromholiday'
WHEN activity_type='travel' and distancetohome > 100 then 'gotoholiday' end  as travel_type
from dailyavg
inner join analytics.user_home_work u using(session_id)
order by 1, 2