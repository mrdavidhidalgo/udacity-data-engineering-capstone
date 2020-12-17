-- Top ten of cities with highest number of accidents in 2018
select c.state,c.city,count(1) from fact_accidents  a 
join  dim_date_hour d USING (date_hour_id)
join dim_cities c  USING (city_id)
where d.year=2018
group by 1,2
order by 3 desc
limit 10


-- Top ten of cities with the highest number of accidents per inhabitant
select state, city, countA::float/population::float, population, countA from (
select c.state,c.city,c.population ,count(1) as countA from fact_accidents a join dim_cities c using (city_id)
where c.population is not null
group by 1,2,3 order by 4 desc)as a
order by 3 desc
limit 10

-- Incidence of weather in accidents in the United States
select weather_condition,count(1) from fact_accidents
group by 1 order by 2 desc


-- Top ten of cities with the lowest number of accidents per inhabitant
select state, city, countA::float/population::float, population, countA from (
select c.state,c.city,c.population ,count(1) as countA from fact_accidents a join dim_cities c using (city_id)
where c.population is not null
group by 1,2,3 order by 4 desc)as a
order by 3 asc
limit 10


-- the hour of day when the most accidents occur
select d.hour,count(1) from fact_accidents  a 
join  dim_date_hour d USING (date_hour_id)
group by 1
order by 2 desc
limit 1
