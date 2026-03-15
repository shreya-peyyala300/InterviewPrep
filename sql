1)
    -- your code goes here
    create table events (
        event VARCHAR(50),
        event_dt DATE
    );
    INSERT INTO events (event, event_dt) VALUES
    ('fail', TO_DATE('2020-01-04','YYYY-MM-DD'));
    INSERT INTO events (event, event_dt) VALUES
    ('success', TO_DATE('2020-01-01','YYYY-MM-DD'));
    INSERT INTO events (event, event_dt) VALUES
    ('success', TO_DATE('2020-01-03','YYYY-MM-DD'));
    INSERT INTO events (event, event_dt) VALUES
    ('success', TO_DATE('2020-01-06','YYYY-MM-DD'));
    INSERT INTO events (event, event_dt) VALUES
    ('fail', TO_DATE('2020-01-05','YYYY-MM-DD'));
    INSERT INTO events (event, event_dt) VALUES
    ('success', TO_DATE('2020-01-02','YYYY-MM-DD'));
    WITH cte AS (
        SELECT event,
               event_dt,
               ROW_NUMBER() OVER (ORDER BY event_dt) -
               Row_number() over(partition by event order by event_dt)as grp_id
        FROM events
    )
    SELECT event, min(event_dt),max(event_dt)
    FROM cte group by event ,grp_id;

2) Repeat non repeat customers
        CREATE TABLE Orders (
            Order_id INT,
            Cust_id VARCHAR2(10),
            Order_dt TIMESTAMP,
            Order_amt NUMBER(10,2)
        );
        INSERT INTO Orders VALUES
        (1,'C1', TO_TIMESTAMP('2025-01-01 09:15:00','YYYY-MM-DD HH24:MI:SS'), 200.00);
        INSERT INTO Orders VALUES
        (2,'C1', TO_TIMESTAMP('2025-01-01 16:45:00','YYYY-MM-DD HH24:MI:SS'), 150.00);
        INSERT INTO Orders VALUES
        (3,'C2', TO_TIMESTAMP('2025-01-01 10:20:00','YYYY-MM-DD HH24:MI:SS'), 300.00);
        INSERT INTO Orders VALUES
        (4,'C3', TO_TIMESTAMP('2025-01-02 11:10:00','YYYY-MM-DD HH24:MI:SS'), 250.00);
        INSERT INTO Orders VALUES
        (5,'C1', TO_TIMESTAMP('2025-01-02 13:00:00','YYYY-MM-DD HH24:MI:SS'), 100.00);
        INSERT INTO Orders VALUES
        (6,'C1', TO_TIMESTAMP('2025-01-03 09:30:00','YYYY-MM-DD HH24:MI:SS'), 180.00);
        INSERT INTO Orders VALUES
        (7,'C2', TO_TIMESTAMP('2025-01-03 15:50:00','YYYY-MM-DD HH24:MI:SS'), 220.00);
        WITH cte AS (
           SELECT DISTINCT 
               cust_id,
               TRUNC(order_dt) AS order_date,
               MIN(TRUNC(order_dt)) OVER (PARTITION BY cust_id) AS first_order_dt
           FROM orders
        )
        SELECT 
            order_date,
            SUM(CASE 
                    WHEN order_date = first_order_dt THEN 1 
                    ELSE 0 
                END) AS new_cust,
            SUM(CASE 
                    WHEN order_date > first_order_dt THEN 1 
                    ELSE 0 
                END) AS repeat_cust
        FROM cte
        GROUP BY order_date
        ORDER BY order_date;
3)Temperature higher than previous day
            CREATE TABLE Weather (
                id NUMBER PRIMARY KEY,
                recordDate DATE,
                temperature NUMBER
            );
            
            INSERT INTO Weather VALUES (1, DATE '2025-10-01', 20);
            INSERT INTO Weather VALUES (2, DATE '2025-10-02', 25);
            INSERT INTO Weather VALUES (3, DATE '2025-10-04', 22);
            INSERT INTO Weather VALUES (4, DATE '2025-10-05', 30);
            INSERT INTO Weather VALUES (5, DATE '2025-10-06', 28);
            INSERT INTO Weather VALUES (6, DATE '2025-10-08', 35);
            
            with cte as(SELECT id,recordDate,temperature,
            LAG(recordDate) OVER (ORDER BY recordDate) AS prev_record,
                   LAG(temperature) OVER (ORDER BY recordDate) AS prev_temp
            FROM Weather)
            select id from cte where temperature>prev_temp and recordDate-prev_record=1
4)
        CREATE TABLE baskets (
            Person VARCHAR(10),
            Basket VARCHAR(100)
        );
        INSERT INTO baskets (Person, Basket) VALUES
        ('A', 'Apple,Mango,Orange'),
        ('B', 'Apple'),
        ('C', 'Guava,Cherry'),
        ('D', 'Mango,Cherry,Orange');
        
        select Person,
        case when basket like '%Apple%' then 'YES' else 'No' end as Apple,
        case when basket like '%Mango%' then 'YES' else 'No' end as Mango,
        case when basket like '%Orange%' then 'YES' else 'No' end as Orange,
        case when basket like '%Guava%' then 'YES' else 'No' end as Guava,
        case when basket like '%Cherry%' then 'YES' else 'No' end as Cherry
        from baskets

5) Source and destination
        create table routes ( source_city VARCHAR(50), destination_city VARCHAR(50) );
        INSERT INTO routes (source_city, destination_city) VALUES ('Delhi','Hyderabad');
        INSERT INTO routes (source_city, destination_city) VALUES ('Hyderabad','Delhi');
        INSERT INTO routes (source_city, destination_city) VALUES ('Bangalore','Mumbai');
        INSERT INTO routes (source_city, destination_city) VALUES ('Mumbai','Bangalore');
        INSERT INTO routes (source_city, destination_city) VALUES ('Kolkata','Pune');
        INSERT INTO routes (source_city, destination_city) VALUES ('Pune','Kolkata');
        with cte as (select source_city,destination_city,
        case 
            when source_city<destination_city then source_city else destination_city end as city1,
        case 
            when source_city>destination_city then destination_city else source_city end as city2
        from ROUTES)
        select city1,city2 from cte group by city1,city2
    Alter:
        WITH cte AS (
            SELECT source_city,
                   destination_city,
                   ROW_NUMBER() OVER (ORDER BY source_city, destination_city) rn
            FROM routes
        )
        SELECT cte1.source_city,
               cte1.destination_city
        FROM cte cte1
        JOIN cte cte2
        ON cte1.source_city = cte2.destination_city
        AND cte1.destination_city = cte2.source_city
        AND cte1.rn < cte2.rn;        


