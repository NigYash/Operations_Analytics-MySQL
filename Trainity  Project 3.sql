create database operation;
use operation;

create table job_data(
ds varchar(100),
job_id int,
actor_id int,
`event` varchar(50),
language varchar(50),
time_spent int,
org varchar (50)
);
select * from job_data;

insert into job_data (ds, job_id, actor_id, `event`, language, time_spent,org)
values ( STR_TO_DATE('11/30/2020','%m/%d/%Y'), 21, 1001, 'skip', 'English', 15, 'A' ),
	( STR_TO_DATE('11/30/2020','%m/%d/%Y'), 22, 1006, 'transfer', 'Arabic', 25, 'B' ),
	( STR_TO_DATE('11/29/2020','%m/%d/%Y'), 23, 1003, 'decision', 'Persian', 20, 'C' ),
    ( STR_TO_DATE('11/28/2020','%m/%d/%Y'), 23, 1005, 'transfer', 'Persian', 22, 'D' ),
    ( STR_TO_DATE('11/28/2020','%m/%d/%Y'), 25, 1002, 'decision', 'Hindi', 11, 'B' ),
    ( STR_TO_DATE('11/27/2020','%m/%d/%Y'), 11, 1007, 'decision', 'French', 104, 'D' ),
    ( STR_TO_DATE('11/26/2020','%m/%d/%Y'), 23, 1004, 'skip', 'Persian', 56, 'A' ),
	( STR_TO_DATE('11/25/2020','%m/%d/%Y'), 20, 1003, 'transfer', 'Italian', 45, 'C' );
    
select * from job_data;


create table users(
user_id int,
created_at varchar(100),
company_id int,
language varchar(50),
activated_at varchar(100),
state varchar(50)
);
select * from users;

show variables like"local_infile";
set global local_infile=1;

LOAD DATA local infile "C:/ProgramData/MySQL/MySQL Server 8.0/Uploads/users.csv"
INTO TABLE users
FIELDS TERMINATED BY ','
ENCLOSED BY '"'
LINES TERMINATED BY '\n'
IGNORE 1 ROWS;

alter table users add column temp_created_at datetime;
UPDATE users SET temp_created_at= STR_TO_DATE(created_at, '%d-%m-%Y %H:%i');
Alter table users drop column created_at;
alter table users change column temp_created_at created_at DATETIME;
alter table users add column temp_created_at datetime;
Alter table users drop column temp_created_at;
alter table users add column temp_activated_at datetime;
UPDATE users SET temp_activated_at= STR_TO_DATE(activated_at, '%d-%m-%Y %H:%i');
Alter table users drop column activated_at;
alter table users change column temp_activated_at activated_at DATETIME;


create table eventss(
 user_id INT,
 occured_at VARCHAR(100), 
 event_type VARCHAR(50),
 event_name VARCHAR(100),
 location VARCHAR(50),
 device VARCHAR(50),
 user_type INT
 );
 
 select * from eventss;
 
 LOAD DATA local infile "C:/ProgramData/MySQL/MySQL Server 8.0/Uploads/events.csv"
INTO TABLE eventss
FIELDS TERMINATED BY ','
ENCLOSED BY '"'
LINES TERMINATED BY '\n'
IGNORE 1 ROWS;

alter table eventss add column temp_occured_at datetime;
UPDATE eventss SET temp_occured_at= STR_TO_DATE(occured_at, '%d-%m-%Y %H:%i');
Alter table eventss drop occured_at;
alter table eventss change column temp_occured_at occured_at DATETIME;

# TAble email_events
 
create table email_events(
user_id INT,
occured_at VARCHAR(100),
`action` VARCHAR(50),
user_type INT
);

select * from email_events;

LOAD DATA local infile "C:/ProgramData/MySQL/MySQL Server 8.0/Uploads/email_events.csv"
INTO TABLE email_events
FIELDS TERMINATED BY ','
ENCLOSED BY '"'
LINES TERMINATED BY '\n'
IGNORE 1 ROWS;

alter table email_events add column temp_occured_at datetime;
UPDATE email_events SET temp_occured_at= STR_TO_DATE(occured_at, '%d-%m-%Y %H:%i');
Alter table email_events drop occured_at;
alter table email_events change column temp_occured_at occured_at DATETIME;


#Task 1

SELECT
CAST(ds AS DATE) AS Job_Date,
COUNT(job_id) AS Jobs_Per_Day,
SUM(time_spent)/3600 AS Total_Time_Spent_in_seconds,
ROUND((3600 * COUNT(job_id)) / SUM(Time_Spent)/3600) AS Jobs_Reviewed_Hour_Day
FROM
job_data
WHERE
CAST(ds AS DATE) >= '2020/11/01'
AND CAST(ds AS DATE) <= '2020/11/30'
GROUP BY Job_Date
ORDER BY Job_Date;

#Task2

select ds, job_reviewed,
avg(job_reviewed)over(order by ds rows between 6 preceding and current row) as throughput_7
from(
select ds, count(distinct job_id) as job_reviewed
from job_data
where ds between "2020-11-01" and "2020-11-30"
group by ds )a;

#Task 3 
SELECT 
    language AS Languages,
    ROUND(100 * COUNT(*) / total, 2) AS percentage,
    sub.total
FROM
    job_data
        CROSS JOIN
    (SELECT 
        COUNT(*) AS total
    FROM
        job_data) AS sub
GROUP BY language , sub.total;

#Task 4

SELECT * FROM job_data;

select * from(
select * , row_number()over(partition by job_id) as row_num
from job_data
)a
where row_num>1;

#Case Study 2: Investigating Metric Spike

#Task 1
select * from eventss;

Select extract(week from occured_at) as week_num,
count(Distinct user_id) as active_user
from eventss
where event_type= 'engagement'
group by week_num
order by week_num;

#Task 2

select year_num, week_num, num_users, sum(num_users)
over(order by year_num , week_num rows between unbounded preceding and current row) as cum_users
from (
select extract(year from activated_at) as year_num, extract(week from activated_at) as week_num, count(distinct user_id) as num_users
from operation.users
#where state='active'
group by year_num, week_num
order by year_num, week_num
)a;

SELECT * FROM users;

#Task3

WITH user_signups AS (
    SELECT user_id,
           EXTRACT(WEEK FROM occured_at) AS signup_week
    FROM eventss
    WHERE event_type = 'signup_flow'
    AND event_name = 'complete_signup' 
    AND EXTRACT(WEEK FROM occured_at) = 18
),
user_activity AS (
    SELECT user_id,
           EXTRACT(WEEK FROM occured_at) AS engagement_week
    FROM eventss
    WHERE event_type = 'engagement'
)
SELECT 
    COUNT(user_id) AS total_engaged_users,
    SUM(CASE WHEN retention_week > 0 THEN 1 ELSE 0 END) AS retained_users
FROM (
    SELECT 
        a.user_id,
        a.signup_week,
        b.engagement_week,
        b.engagement_week - a.signup_week AS retention_week
    FROM user_signups a
    LEFT JOIN user_activity b ON a.user_id = b.user_id
) AS sum
GROUP BY signup_week;


#task4

SELECT 
    EXTRACT(WEEK FROM occured_at) AS week,
    EXTRACT(YEAR FROM occured_at) AS year,
    device,
    COUNT(DISTINCT user_id) AS count 
FROM eventss
WHERE event_type = 'engagement'
GROUP BY week, year, device
ORDER BY week, year, device;

#Task 5 

select 
action, 
count(distinct user_id) as unique_users_count,
count(*) as total_actions_count
from email_events 
group by action
order by action;

select * from email_events;

