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
3)
