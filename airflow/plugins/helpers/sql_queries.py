class SqlQueries:
    # Define schemas and tables for each tier
    CREATE_SCHEMAS = """
        CREATE SCHEMA IF NOT EXISTS bronze;
        CREATE SCHEMA IF NOT EXISTS silver;
        CREATE SCHEMA IF NOT EXISTS gold;
    """

    CREATE_TABLES = """
    CREATE TABLE IF NOT EXISTS bronze.processed_files_metadata (
    id INT IDENTITY(1,1) PRIMARY KEY,
    filename VARCHAR(512) NOT NULL,
    processed_datetime TIMESTAMP DEFAULT GETDATE(),
    UNIQUE (filename)
    );
    
    -- Bronze Schema (raw data)
    CREATE TABLE IF NOT EXISTS bronze.staging_yellow_trips_tmp (
        VendorID VARCHAR,
        tpep_pickup_datetime VARCHAR,
        tpep_dropoff_datetime VARCHAR,
        passenger_count VARCHAR,
        trip_distance VARCHAR,
        RatecodeID VARCHAR,
        store_and_fwd_flag VARCHAR,
        PULocationID VARCHAR,
        DOLocationID VARCHAR,
        payment_type VARCHAR,
        fare_amount VARCHAR,
        extra VARCHAR,
        mta_tax VARCHAR,
        tip_amount VARCHAR,
        tolls_amount VARCHAR,
        improvement_surcharge VARCHAR,
        total_amount VARCHAR,
        congestion_surcharge VARCHAR,
        airport_fee VARCHAR
    );
    CREATE TABLE IF NOT EXISTS bronze.staging_yellow_trips (
        VendorID VARCHAR,
        tpep_pickup_datetime VARCHAR,
        tpep_dropoff_datetime VARCHAR,
        passenger_count VARCHAR,
        trip_distance VARCHAR,
        RatecodeID VARCHAR,
        store_and_fwd_flag VARCHAR,
        PULocationID VARCHAR,
        DOLocationID VARCHAR,
        payment_type VARCHAR,
        fare_amount VARCHAR,
        extra VARCHAR,
        mta_tax VARCHAR,
        tip_amount VARCHAR,
        tolls_amount VARCHAR,
        improvement_surcharge VARCHAR,
        total_amount VARCHAR,
        congestion_surcharge VARCHAR,
        airport_fee VARCHAR,
        load_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    );


    CREATE TABLE IF NOT EXISTS bronze.staging_lookup_trips_tmp (
        LocationID VARCHAR,
        Borough VARCHAR,
        Zone VARCHAR
    );
    
    CREATE TABLE IF NOT EXISTS bronze.staging_lookup_trips (
        LocationID VARCHAR,
        Borough VARCHAR,
        Zone VARCHAR,
        load_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    );

    -- Silver Schema (cleaned/transformed data)

    CREATE TABLE IF NOT EXISTS silver.DIM_pickup (
        LocationID INT PRIMARY KEY,
        Borough TEXT,
        Zone TEXT,
        load_timestamp TIMESTAMP
    );

    CREATE TABLE IF NOT EXISTS silver.DIM_dropoff (
        LocationID INT PRIMARY KEY,
        Borough TEXT,
        Zone TEXT,
        load_timestamp TIMESTAMP
    );

    CREATE TABLE IF NOT EXISTS silver.FACT_trips (
        ID INT IDENTITY(0,1) PRIMARY KEY,
        PUDate TIMESTAMP,
        DODate TIMESTAMP,
        PULocationID INT REFERENCES silver.DIM_pickup(LocationID),
        DOLocationID INT REFERENCES silver.DIM_dropoff(LocationID),
        passenger_count INT,
        trip_distance FLOAT,
        total_amount FLOAT,
        payment_type FLOAT,
        load_timestamp TIMESTAMP
    );

    -- Gold Schema (aggregated/insight data)

    CREATE TABLE IF NOT EXISTS gold.pop_destination_passengers_month(
        month TEXT,
        pick_up TEXT,
        drop_off TEXT,
        total_passengers INT,
        ranking INT
    );

    CREATE TABLE IF NOT EXISTS gold.pop_destination_rides_month(
        month TEXT,
        pick_up TEXT,
        drop_off TEXT,
        total_rides TEXT,
        ranking INT
    );

    CREATE TABLE IF NOT EXISTS gold.popular_rides_full (
        month TEXT,
        pick_up TEXT,
        drop_off TEXT,
        ranking INT
    );

    CREATE TABLE IF NOT EXISTS gold.cur_popular_dest (
        pick_up TEXT,
        drop_off TEXT,
        ranking INT
    );
    """

    COPY_S3_SQL_CSV = """
    COPY {table} FROM '{s3_path}'
    CREDENTIALS 'aws_access_key_id={access_key};aws_secret_access_key={secret_key}'
    IGNOREHEADER 1
    CSV
    DELIMITER ','
    """

    COPY_S3_SQL_PARQUET = """
    TRUNCATE TABLE {table}_tmp;
    
    COPY {table}_tmp FROM '{s3_path}'
    CREDENTIALS 'aws_access_key_id={access_key};aws_secret_access_key={secret_key}'
    FORMAT AS PARQUET;
    
    INSERT INTO {table} 
    SELECT *, GETDATE()
    FROM {table}_tmp;
    
    TRUNCATE TABLE {table}_tmp;
    """

    LOAD_DIM_PICKUP_SQL = """
    INSERT INTO silver.DIM_pickup
    SELECT 
        CAST(LocationID AS INT) AS LocationID,
        Borough,
        Zone,
        load_timestamp 
    FROM bronze.staging_lookup_trips
    WHERE LocationID NOT IN (SELECT DISTINCT LocationID FROM silver.DIM_pickup);
    """

    LOAD_DIM_DROPOFF_SQL = """
    INSERT INTO silver.DIM_dropoff
    SELECT 
        CAST(LocationID AS INT) AS LocationID,
        Borough,
        Zone,
        load_timestamp 
    FROM bronze.staging_lookup_trips
    WHERE LocationID NOT IN (SELECT DISTINCT LocationID FROM silver.DIM_dropoff);
    """

    LOAD_FACT_TRIPS = """
    INSERT INTO silver.FACT_trips (
        PUDate,
        DODate,
        PULocationID,
        DOLocationID,
        passenger_count,
        trip_distance,
        total_amount,
        payment_type,
        load_timestamp
    )
    SELECT
        TO_TIMESTAMP(COALESCE(NULLIF(tpep_pickup_datetime, 'nan'),'1900-01-01'), 'YYYY-MM-DD HH24:MI:SS') AS PUDate,
        TO_TIMESTAMP(COALESCE(NULLIF(tpep_dropoff_datetime, 'nan'),'1900-01-01'), 'YYYY-MM-DD HH24:MI:SS') AS DODate,
        COALESCE(CAST(NULLIF(PULocationID, 'nan') AS INT), 0) AS PULocationID,
        COALESCE(CAST(NULLIF(DOLocationID, 'nan') AS INT), 0) AS DOLocationID,
        COALESCE(CAST(NULLIF(passenger_count, 'nan') AS FLOAT), 0.0) AS passenger_count,
        COALESCE(CAST(NULLIF(trip_distance, 'nan') AS FLOAT), 0.0) AS trip_distance,
        COALESCE(CAST(NULLIF(total_amount, 'nan') AS FLOAT), 0.0) AS total_amount,
        COALESCE(CAST(NULLIF(payment_type, 'nan') AS FLOAT), 0.0) AS payment_type,
        CURRENT_TIMESTAMP AS load_timestamp
    FROM bronze.staging_yellow_trips st
    WHERE st.load_timestamp > (
        SELECT COALESCE(MAX(ft.load_timestamp), '1900-01-01')
        FROM silver.FACT_trips ft
    );
    """

    CALC_POP_DESTINATION_PASSENGERS_MONTH = """
        TRUNCATE TABLE gold.pop_destination_passengers_month;
        
        INSERT INTO gold.pop_destination_passengers_month
        WITH total_passengers AS (
            SELECT
                to_char(t.PUDate, 'YYYY-MM') as month,
                p.zone as pick_up,
                d.zone as drop_off,
                sum(CAST(t.passenger_count AS INTEGER)) as total_passengers
            FROM silver.FACT_trips t
            LEFT JOIN silver.DIM_pickup p ON p.LocationID = t.PULocationID
            LEFT JOIN silver.DIM_dropoff d ON d.LocationID = t.DOLocationID
            WHERE to_date(t.PUDate, 'YYYY-MM-DD', FALSE) = '{}'
            GROUP BY t.PUDate, p.zone, d.zone
        ),
        ranked_total_passengers AS (
            SELECT
                *,
                rank() OVER (PARTITION BY pick_up ORDER BY total_passengers DESC) as ranking
            FROM total_passengers
        )
        SELECT
            *
        FROM ranked_total_passengers
        WHERE ranking <= 5;
        """

    CALC_POP_DESTINATION_RIDES_MONTH = """
        TRUNCATE TABLE gold.pop_destination_rides_month;
        
        INSERT INTO gold.pop_destination_rides_month
        WITH total_rides AS (
            SELECT
                to_char(t.PUDate, 'YYYY-MM') as month,
                p.Borough as pick_up,
                d.Borough as drop_off,
                count(t.ID) as total_rides
            FROM silver.FACT_trips t
            LEFT JOIN silver.DIM_pickup p ON p.LocationID = t.PULocationID
            LEFT JOIN silver.DIM_dropoff d ON d.LocationID = t.DOLocationID
            WHERE to_date(t.PUDate, 'YYYY-MM-DD', FALSE) = '{}'::Date
            GROUP BY t.PUDate, p.Borough, d.Borough
        ),
        ranked_borough_destination AS (
            SELECT
                *,
                rank() OVER (PARTITION BY pick_up ORDER BY total_rides DESC) as ranking
            FROM total_rides
        )
        SELECT
            *
        FROM ranked_borough_destination;
        """

    CALC_POPULAR_RIDES_FULL = """
        TRUNCATE TABLE gold.popular_rides_full;
        
        INSERT INTO gold.popular_rides_full
        WITH total_rides AS (
            SELECT
                to_char(t.PUDate, 'YYYY-MM') as month,
                p.Borough as pick_up,
                d.Borough as drop_off,
                count(t.ID) as total_rides
            FROM silver.FACT_trips t
            LEFT JOIN silver.DIM_pickup p ON p.LocationID = t.PULocationID
            LEFT JOIN silver.DIM_dropoff d ON d.LocationID = t.DOLocationID
            WHERE to_date(t.PUDate, 'YYYY-MM-DD', FALSE) = '{}'::Date
            GROUP BY t.PUDate, p.Borough, d.Borough
        ),
        ranked_borough_destination AS (
            SELECT
                month,
                pick_up,
                drop_off,
                rank() OVER (PARTITION BY pick_up ORDER BY total_rides DESC) as ranking
            FROM total_rides
        ),
        prev_rank AS (
            SELECT 
                *
            FROM gold.popular_rides_full
            WHERE month = '{}'
        )
        SELECT
            current.*
        FROM ranked_borough_destination current
        LEFT JOIN prev_rank ON prev_rank.pick_up = current.pick_up AND prev_rank.drop_off = current.drop_off AND prev_rank.ranking = current.ranking
        WHERE prev_rank.ranking IS NULL AND current.ranking <= 10
        ;
        """

    CALC_CURRENT_POP_DEST = """
        TRUNCATE TABLE gold.cur_popular_dest;
        
        INSERT INTO gold.cur_popular_dest
        WITH total_rides AS (
            SELECT
                to_char(t.PUDate, 'YYYY-MM') as month,
                p.Borough as pick_up,
                d.Borough as drop_off,
                count(t.ID) as total_rides
            FROM silver.FACT_trips t
            LEFT JOIN silver.DIM_pickup p ON p.LocationID = t.PULocationID
            LEFT JOIN silver.DIM_dropoff d ON d.LocationID = t.DOLocationID
            WHERE to_date(t.PUDate, 'YYYY-MM-DD', FALSE) = '{}'::Date
            GROUP BY t.PUDate, p.Borough, d.Borough
        ),
        ranked_borough_destination AS (
            SELECT
                pick_up,
                drop_off,
                rank() OVER (PARTITION BY pick_up ORDER BY total_rides DESC) as ranking
            FROM total_rides
        )
        SELECT
            *
        FROM ranked_borough_destination
        WHERE ranking <= 10;
        """