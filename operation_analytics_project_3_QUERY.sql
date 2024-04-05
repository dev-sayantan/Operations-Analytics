USE ops_analytics;
/* Operation Analytics and Investigating Metric Spike
Advanced SQL*/
-- Case Study 1 (Job Data)
-- Calculate the number of jobs reviewed per hour per day for November 2020? 
SELECT 
    a.ds AS days_in_nov,
    ROUND(SUM(a.seconds_spent / 3600), 3) AS hrs_spent,
    a.job_num AS num_of_jobs
FROM
    (SELECT 
        ds,
            SUM(time_spent) AS seconds_spent,
            COUNT(job_id) AS job_num
    FROM
        ops_analytics.job_data
    GROUP BY ds) a
GROUP BY days_in_nov , num_of_jobs
ORDER BY days_in_nov;
-- Calculate 7 day rolling average of throughput?(It is the no. of events happening per second)
SELECT
	sub.ds AS dates,
    sub.events_count/sub.seconds AS throughput,
    AVG(sub.events_count/sub.seconds) OVER(ORDER BY sub.ds ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS avg_rolling_throughput
FROM
(SELECT 
    ds, SUM(time_spent) AS seconds, COUNT(event) AS events_count
FROM
    ops_analytics.job_data
GROUP BY ds) AS sub
GROUP BY dates;
-- Calculate the percentage share of each language for different contents in the last 30 days?
SELECT
	sub.language AS lang,
	sub.jobs AS jobs_in_lang,
    100*sub.jobs/SUM(sub.jobs) OVER() AS lang_share_percent
FROM
(SELECT 
    language, COUNT(job_id) AS jobs
FROM
    ops_analytics.job_data
GROUP BY language) AS sub
GROUP BY lang;
-- Let’s say you see some duplicate rows in the data. How will you display duplicates from the table?
SELECT 
	ds,job_id,actor_id,event,language,time_spent,org,
    count(*) AS row_occurence
FROM
	job_data
GROUP BY
	ds,job_id,actor_id,event,language,time_spent,org
HAVING count(*) > 1;
-- Case Study 2 (Investigating metric spike)
-- To measure the activeness of a user. Measuring if the user finds quality in a product/service. Calculate the weekly user engagement? 
SELECT 
    WEEK(occurred_at) AS week_num,
    event_type,
    COUNT(user_id) AS users
FROM
    ops_analytics.events
GROUP BY week_num,event_type
HAVING event_type = 'engagement';

-- Calculate the amount of user growth over time for a product?
SELECT sub.month_num, sub.users, sum(sub.users) - LAG(sub.users,1) OVER() AS user_growth
FROM
(SELECT 
    MONTH(occurred_at) AS month_num, COUNT(user_id) AS users
FROM
    events
GROUP BY month_num) AS sub
GROUP BY month_num;

-- Calculate the weekly retention of users-sign up cohort?
SELECT 
	sub1.*, sub1.users_weekly - sub1.users_prev_wk AS user_retention
FROM (SELECT 
    sub.week_num, sub.users AS users_weekly , LAG(sub.users,1) OVER(ORDER BY week_num) AS users_prev_wk
FROM
    (SELECT 
        WEEK(users.created_at) AS week_num,
            COUNT(users.user_id) AS users,
            event_type,
            state
    FROM
        users
    JOIN events ON users.user_id = events.user_id
    GROUP BY week_num , event_type , state
    HAVING events.event_type = 'engagement'
        AND users.state = 'active') AS sub
GROUP BY sub.week_num, sub.users) AS sub1;
-- Calculate the weekly engagement per device? To measure the activeness of a user. Measuring if the user finds quality in a product/service weekly.
SELECT
	sub.device,
    sub.week_num,
    sub.user_engagement_device,
    sum(sub.user_engagement_device) OVER(PARTITION BY sub.week_num) AS user_engagement_wk
FROM (SELECT 
    device,
    event_type,
    COUNT(event_type) AS user_engagement_device,
    WEEK(occurred_at) AS week_num
FROM
    events
GROUP BY device , event_type , WEEK(occurred_at)
HAVING
 event_type = 'engagement') AS sub
GROUP BY sub.device, sub.week_num, sub.user_engagement_device
ORDER BY sub.week_num;
-- Calculate the email engagement metrics?
-- No of users associated with each type of action and user_type
SELECT 
    email_events.user_type,
    action,
    COUNT(email_events.user_id) AS users
FROM
    email_events
GROUP BY action , user_type
ORDER BY user_type;