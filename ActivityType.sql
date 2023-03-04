/* calculate daily average lat lng and create a table view name is "dailyavg" */

with dailyavg as (
select session_id, timestamp_date,avg(lat) as lat,avg(lng) as lng from malliq_analytics_samsung.find_mall_request
where timestamp_date>='2021-01-01'
and lat is not null
and lng is not null
and lat != 0
and lng != 0
group by 1,2)
   /* calculate overall average lat lng and create a table view name is "general_location" to determine home lat,lng */
, general_location as (
select session_id,avg(lat) as home_lat,avg(lng) as home_lng from malliq_analytics_samsung.find_mall_request
where timestamp_date>='2021-01-01'
and lat is not null
and lng is not null
and lat != 0
and lng != 0
group by 1)

select session_id,
timestamp_date,
lat,
lng,
lag(lat, 1) over (partition by session_id order by session_id,timestamp_date) as lag_lat, /* Get the previous lat average from the daily calculated averages with the lag() function. */
lag(lng, 1) over (partition by session_id order by session_id,timestamp_date) as lag_lng, /* Get the previous lng average from the daily calculated averages with the lag() function. */
f_great_circle_distance(lat, lng, lag_lat, lag_lng) / 1000.0                  as distance_traveled_km, /* Calculate the distance from the previous average location as distance_traveled_km  */
f_great_circle_distance(home_lat, home_lng, lag_lat, lag_lng) / 1000.0        as distance_from_home, /* Calculate the distance from home location as distance_from_home  */
CASE
WHEN distance_traveled_km >= 150 then 'travel' /* filter as travel if more than and equal 150km from previous day's location   */
WHEN distance_traveled_km < 150 then 'nontravel' end  as activity_type, /* filter as nontravel if  less than 150km from previous day's location   */
CASE
WHEN activity_type='travel' and distance_from_home<=100 then 'backfromholiday' /* if the activity type is travel then filter as backfromholiday if less than and equal 100km from home's location   */
WHEN activity_type='travel' and distance_from_home>100 then 'gotoholiday' end  as travel_type /* if the activity type is travel filter as gotoholiday if more than 100km from home's location   */
from dailyavg
inner join general_location using (session_id)
order by 1, 2
