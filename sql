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

2)
