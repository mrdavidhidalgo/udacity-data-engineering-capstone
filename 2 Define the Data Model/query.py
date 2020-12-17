CREATE_TABLE_STAGING_ACCIDENTS = """
create table staging_accidents(
          row_id int8, 	
          id varchar(32),
          source varchar(128),
          tmc varchar(128),
          timezone varchar(64),
          temperature varchar(32),
          stop varchar(32),
          start_time varchar(64),
          end_time varchar(64),
          city varchar(256),
          country varchar(32),
          state varchar(128),
          zip_code varchar(256),         
          description varchar(1024),
          weather_Timestamp varchar(32),
          weather_Condition varchar(32),
          latitude varchar(32),
          longitude varchar(32)
        )
"""

CREATE_TABLE_STAGING_CITIES = """
create table staging_cities(
          row_id int8, 	
          country varchar(32),
          city varchar(128),
          accent_city varchar(128),
          region varchar(64),
          population varchar(32),
          latitude varchar(32),
          longitude varchar(32)
        )
"""

COPY_STATEMENT="""
	COPY {}
        FROM '{}'
        ACCESS_KEY_ID '{}'
        SECRET_ACCESS_KEY '{}'
        IGNOREHEADER 1
        DELIMITER ','
        TRUNCATECOLUMNS

"""

CREATE_TABLE_DIM_CITIES = """
 create table dim_cities(
          city_id varchar(130) primary key,
          city varchar(128) not null,
          state varchar(2) not null,
          country varchar(2),
          population int8,
          latitude float,
          longitude float
        ) diststyle all
"""

INSERT_DIM_CITIES = """
insert into dim_cities
        select  upper(city)||'_'||upper(region) as city_id, 
        upper(city) as city,
        upper(region) as state,
        upper(country) as country, 
        case when population is null or population ='' then null else population::float::int8 end as population, 
        latitude::float,longitude::float 
        from staging_cities
        where upper(country)='US'

"""

CREATE_TABLE_DIM_DATE_HOURS = """

create table dim_date_hour(
    date_hour_id varchar(32),
    year int not null,
    month int not null, 
    week int not null,
    weekday int not null,
    day int not null,
    hour int not null,
    primary key(date_hour_id)
    )
    diststyle all;

"""

INSERT_DIM_DATE_HOUR = """

insert into dim_date_hour(date_hour_id,"year","month","week", "weekday", "day", "hour")
select distinct
start_time::date || ' ' ||extract(hour from start_time),
extract(year from start_time) as "year",
extract(month from start_time) as "month",
extract(week from start_time) as "week",
extract(weekday from start_time) as "weekday",
extract(day from start_time) as "day",
extract(hour from start_time) as "hour" 
from (select distinct start_time::TIMESTAMP from staging_accidents where start_time is not null) as a

"""


CREATE_TABLE_FACT_ACCIDENTS = """"

 create table fact_accidents(
 		  accident_id varchar(20) primary key, 
          city_id varchar(130) references dim_cities(city_id),
          date_hour_id  varchar(32) not null  references dim_date_hour(date_hour_id),
          source varchar(128),
          tmc varchar(128),
          temperature float,
          start_time TIMESTAMP,
          end_time TIMESTAMP,
          zip_code varchar(64),         
          description varchar(1024),
          weather_condition varchar(32),
          latitude float,
          longitude float
          )
          diststyle key distkey (date_hour_id);
"""


INSERT_FACT_ACCIDNETS = """
insert into fact_accidents (accident_id, city_id, date_hour_id, source, tmc, temperature, start_time, end_time, zip_code, description, weather_condition, latitude, longitude)
        select id as accident_id, 
        c.city_id,
        start_time::TIMESTAMP::date || ' ' ||extract(hour from start_time::TIMESTAMP),
        source,
        tmc,
        case when temperature is null or temperature ='' then null else temperature::float end,
        start_time::TIMESTAMP,
        end_time::TIMESTAMP,
        zip_code,
        description,
        weather_condition,
        a.latitude::float,
        a.longitude::float
         From staging_accidents a
        left join dim_cities c  on lower(c.city) = lower(a.city) and lower(c.state)=lower(a.state)

"""


