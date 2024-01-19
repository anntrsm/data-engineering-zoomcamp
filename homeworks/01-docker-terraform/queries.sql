--1
select
	count(*)
from
	public.green_zone_taxi_data
where 
	lpep_pickup_datetime::date >= '2019-09-18'::date 
	and lpep_dropoff_datetime::date < '2019-09-19'::date

--2
select 
	*
from 
	public.green_zone_taxi_data
where
	trip_distance = (select max(trip_distance) from public.green_zone_taxi_data)
	
--3
select 
	b."Borough", sum(total_amount)
from 
	public.green_zone_taxi_data a
inner join
	public.taxi_zones b on
		a."PULocationID" = b."LocationID" 
where 
	a.lpep_pickup_datetime::date = '2019-09-18'::date 
	and b."Borough" != 'Unknown'
group by
	b."Borough"
	
--4
select 
	b."Zone" as pickup_zone
	, c."Zone" as dropoff_zone
	, a.tip_amount
from 
	public.green_zone_taxi_data a
left join
	public.taxi_zones b on
		a."PULocationID" = b."LocationID"
left join
	public.taxi_zones c on
		a."DOLocationID" = c."LocationID" 
where 
	a.lpep_pickup_datetime::date >= '2019-09-01'::date 
	and a.lpep_pickup_datetime::date < '2019-10-01'::date
	and b."Zone" = 'Astoria'
order by tip_amount desc
limit 1



