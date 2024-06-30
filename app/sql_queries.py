
Q1 = """-- What is the total number of records in the final table?
select count(1) from silver.FACT_trips; -- 38.310.226
"""

Q2 = """
--What is the total number of trips started and completed on June 17th?
select count(1)
from silver.FACT_trips
where  PUDate::Date = '2023-06-17' and DODate::Date = '2023-06-17'; -- 104548
"""

Q3 = """
-- What was the day of the longest trip traveled?
SELECT PUDate::Date
FROM silver.FACT_trips
WHERE trip_distance = (SELECT MAX(trip_distance) FROM silver.FACT_trips);  -- 2023-08-15
"""

Q4 = """
-- What is the mean, standard deviation, minimum, maximum and quartiles of the distribution of distance traveled in total trips?
SELECT 
    AVG(trip_distance) AS mean_distance,
    STDDEV(trip_distance) AS stddev_distance,
    MIN(trip_distance) AS min_distance,
    MAX(trip_distance) AS max_distance,
    PERCENTILE_CONT(0.25) WITHIN GROUP (ORDER BY trip_distance) AS first_quartile,
    PERCENTILE_CONT(0.50) WITHIN GROUP (ORDER BY trip_distance) AS median,
    PERCENTILE_CONT(0.75) WITHIN GROUP (ORDER BY trip_distance) AS third_quartile
FROM silver.FACT_trips;
--mean_distance	        stddev_distance	    min_distance	max_distance	first_quartile	median	third_quartile
--4.088946216071824	    241.2508998901031	0	            345729.44	    1.04	        1.79	3.4
"""