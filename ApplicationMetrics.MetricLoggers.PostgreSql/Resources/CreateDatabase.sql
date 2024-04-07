--------------------------------------------------------------------------------
--------------------------------------------------------------------------------
-- Create Database
--------------------------------------------------------------------------------
--------------------------------------------------------------------------------

CREATE DATABASE "ApplicationMetrics";


--------------------------------------------------------------------------------
--------------------------------------------------------------------------------
-- Create Tables
--------------------------------------------------------------------------------
--------------------------------------------------------------------------------

CREATE TABLE public.Categories
(
    Id    bigserial      NOT NULL PRIMARY KEY,  
    Name  varchar(450)   NOT NULL
);

CREATE UNIQUE INDEX CategoriesNameIndex ON public.Categories (Name);

CREATE TABLE public.CountMetrics
(
    Id               bigserial      NOT NULL PRIMARY KEY,  
    Name             varchar(450)   NOT NULL, 
    Description      varchar(4000)  NOT NULL 
);

CREATE UNIQUE INDEX CountMetricsNameIndex ON public.CountMetrics (Name);

CREATE TABLE public.AmountMetrics
(
    Id               bigserial      NOT NULL PRIMARY KEY,  
    Name             varchar(450)   NOT NULL, 
    Description      varchar(4000)  NOT NULL 
);

CREATE UNIQUE INDEX AmountMetricsNameIndex ON public.AmountMetrics (Name);

CREATE TABLE public.StatusMetrics
(
    Id               bigserial      NOT NULL PRIMARY KEY,  
    Name             varchar(450)   NOT NULL, 
    Description      varchar(4000)  NOT NULL 
);

CREATE UNIQUE INDEX StatusMetricsNameIndex ON public.StatusMetrics (Name);

CREATE TABLE public.IntervalMetrics
(
    Id               bigserial      NOT NULL PRIMARY KEY,  
    Name             varchar(450)   NOT NULL, 
    Description      varchar(4000)  NOT NULL 
);

CREATE UNIQUE INDEX IntervalMetricsNameIndex ON public.IntervalMetrics (Name);

CREATE TABLE public.CountMetricInstances
(
    Id              bigserial  NOT NULL PRIMARY KEY,  
    CategoryId      bigint     NOT NULL, 
    EventTime       timestamp  NOT NULL, 
    CountMetricId   bigint     NOT NULL
);

CREATE INDEX CountMetricInstancesCategoryIdIndex ON public.CountMetricInstances (CategoryId);
CREATE INDEX CountMetricInstancesCountMetricIdIndex ON public.CountMetricInstances (CountMetricId);
CREATE INDEX CountMetricInstancesEventTimeIndex ON public.CountMetricInstances (EventTime);

CREATE TABLE public.AmountMetricInstances
(
    Id              bigserial  NOT NULL PRIMARY KEY,  
    CategoryId      bigint     NOT NULL, 
    AmountMetricId  bigint     NOT NULL, 
    EventTime       timestamp  NOT NULL, 
    Amount          bigint     NOT NULL
);

CREATE INDEX AmountMetricInstancesCategoryIdIndex ON public.AmountMetricInstances (CategoryId);
CREATE INDEX AmountMetricInstancesAmountMetricIdIndex ON public.AmountMetricInstances (AmountMetricId);
CREATE INDEX AmountMetricInstancesEventTimeIndex ON public.AmountMetricInstances (EventTime);

CREATE TABLE public.StatusMetricInstances
(
    Id              bigserial  NOT NULL PRIMARY KEY,  
    CategoryId      bigint     NOT NULL, 
    StatusMetricId  bigint     NOT NULL, 
    EventTime       timestamp  NOT NULL, 
    Value           bigint     NOT NULL
);

CREATE INDEX StatusMetricInstancesCategoryIdIndex ON public.StatusMetricInstances (CategoryId);
CREATE INDEX StatusMetricInstancesAmountMetricIdIndex ON public.StatusMetricInstances (StatusMetricId);
CREATE INDEX StatusMetricInstancesEventTimeIndex ON public.StatusMetricInstances (EventTime);

CREATE TABLE public.IntervalMetricInstances
(
    Id                bigserial  NOT NULL PRIMARY KEY,  
    CategoryId        bigint     NOT NULL, 
    IntervalMetricId  bigint     NOT NULL, 
    EventTime         timestamp  NOT NULL, 
    Duration          bigint     NOT NULL
);

CREATE INDEX IntervalMetricInstancesCategoryIdIndex ON public.IntervalMetricInstances (CategoryId);
CREATE INDEX IntervalMetricInstancesAmountMetricIdIndex ON public.IntervalMetricInstances (IntervalMetricId);
CREATE INDEX IntervalMetricInstancesEventTimeIndex ON public.IntervalMetricInstances (EventTime);

CREATE TABLE public.SchemaVersions
(
    Id         bigserial    NOT NULL PRIMARY KEY,  
    Version    varchar(20)  NOT NULL, 
    Created    timestamp    NOT NULL 
);


--------------------------------------------------------------------------------
--------------------------------------------------------------------------------
-- Create Views
--------------------------------------------------------------------------------
--------------------------------------------------------------------------------

CREATE VIEW public.CountMetricInstancesView AS
SELECT  cmi.Id          Id, 
        c.Name          Category, 
        cm.Name         CountMetric, 
        cm.Description  CountMetricDescription, 
        cmi.EventTime   EventTime
FROM    CountMetricInstances cmi
        INNER JOIN CountMetrics cm
          ON cmi.CountMetricId = cm.Id
        Inner JOIN Categories c
          ON cmi.CategoryId = c.Id;

CREATE VIEW public.AmountMetricInstancesView AS
SELECT  ami.Id          Id, 
        c.Name          Category, 
        am.Name         AmountMetric, 
        am.Description  AmountMetricDescription, 
        ami.EventTime   EventTime, 
        ami.Amount      Amount
FROM    AmountMetricInstances ami
        INNER JOIN AmountMetrics am
          ON ami.AmountMetricId = am.Id
        Inner JOIN Categories c
          ON ami.CategoryId = c.Id;

CREATE VIEW public.StatusMetricInstancesView AS
SELECT  smi.Id          Id, 
        c.Name          Category, 
        sm.Name         StatusMetric, 
        sm.Description  StatusMetricDescription, 
        smi.EventTime   EventTime, 
        smi.Value       Value
FROM    StatusMetricInstances smi
        INNER JOIN StatusMetrics sm
          ON smi.StatusMetricId = sm.Id
        Inner JOIN Categories c
          ON smi.CategoryId = c.Id;
          
CREATE VIEW public.IntervalMetricInstancesView AS
SELECT  imi.Id          Id, 
        c.Name          Category, 
        im.Name         IntervalMetric, 
        im.Description  IntervalMetricDescription, 
        imi.EventTime   EventTime, 
        imi.Duration    Duration
FROM    IntervalMetricInstances imi
        INNER JOIN IntervalMetrics im
          ON imi.IntervalMetricId = im.Id
        Inner JOIN Categories c
          ON imi.CategoryId = c.Id;

CREATE VIEW public.AllMetricInstancesView AS
SELECT  Id                      Id, 
        Category                Category, 
        'Count'                 MetricType, 
        CountMetric             MetricName, 
        CountMetricDescription  MetricDescription, 
        EventTime               EventTime, 
        null                    Value
FROM    CountMetricInstancesView
UNION ALL
SELECT  Id, 
        Category, 
        'Amount', 
        AmountMetric, 
        AmountMetricDescription, 
        EventTime, 
        Amount
FROM    AmountMetricInstancesView
UNION ALL
SELECT  Id, 
        Category, 
        'Status', 
        StatusMetric, 
        StatusMetricDescription, 
        EventTime, 
        Value
FROM    StatusMetricInstancesView
UNION ALL
SELECT  Id, 
        Category, 
        'Interval', 
        IntervalMetric, 
        IntervalMetricDescription, 
        EventTime, 
        Duration
FROM    IntervalMetricInstancesView;


--------------------------------------------------------------------------------
--------------------------------------------------------------------------------
-- Create Functions / Stored Procedures
--------------------------------------------------------------------------------
--------------------------------------------------------------------------------

--------------------------------------------------------------------------------
-- InsertCountMetrics

CREATE OR REPLACE PROCEDURE InsertCountMetrics
(
    Category       varchar, 
    CountMetrics   json
)
LANGUAGE 'plpgsql'
AS $$
DECLARE 

    CurrentMetricEventAsJson  json;
    CurrentMetricName         varchar;
    CurrentMetricDescription  varchar;
    CurrentEventTimeAsString  varchar;
    CurrentEventTime          timestamp;
    CategoryId                bigint;
    CountMetricId             bigint;

    BEGIN

    FOR CurrentMetricEventAsJson IN 
    SELECT  * 
    FROM    json_array_elements(CountMetrics)
    LOOP 

        CurrentMetricName := CurrentMetricEventAsJson->>'Name';
        CurrentMetricDescription := CurrentMetricEventAsJson->>'Description';
        CurrentEventTimeAsString := CurrentMetricEventAsJson->>'Time';
        BEGIN
            SELECT  TO_TIMESTAMP(CurrentEventTimeAsString, 'YYYY-MM-DD HH24:MI:ss.US') AS timestamp
            INTO    CurrentEventTime;
        EXCEPTION
            WHEN OTHERS THEN
                RAISE EXCEPTION 'Failed to convert metric Time ''%'' to a timestamp; %', COALESCE(CurrentEventTimeAsString, '(null)'), SQLERRM;
        END;

        BEGIN
            INSERT  
            INTO    CountMetricInstances 
                    (
                        CategoryId, 
                        CountMetricId, 
                        EventTime
                    )
            VALUES  (
                        ( 
                            SELECT  Id 
                            FROM    Categories 
                            WHERE   Name = Category 
                        ), 
                        ( 
                            SELECT  Id 
                            FROM    CountMetrics
                            WHERE   Name = CurrentMetricName 
                        ), 
                        CurrentEventTime
                    );
        EXCEPTION
            WHEN not_null_violation THEN
                -- Insert failed due to 'not_null_violation' error
                --   Need to ensure Category and CurrentMetricName exist

                -- Take an exclusive lock on the Categories table to prevent other sessions attempting to insert the same category 
                --   (or insert the same metric... since the change to the metric table occurs with the same lock on Categories in place)
                LOCK TABLE public.Categories IN EXCLUSIVE MODE;

                SELECT  Id
                INTO    CategoryId 
                FROM    Categories 
                WHERE   Name = Category;

                IF (CategoryId IS NULL) THEN 
                    BEGIN
                        -- Insert Category
                        INSERT  
                        INTO    Categories 
                                (
                                    Name
                                )
                        VALUES  (
                                    Category
                                );
                    EXCEPTION
                        WHEN OTHERS THEN
                            RAISE EXCEPTION 'Error occurred when inserting Category ''%''; %', COALESCE(Category, '(null)'), SQLERRM;
                    END;
                END IF;

                SELECT  Id 
                INTO    CountMetricId
                FROM    CountMetrics 
                WHERE   Name = CurrentMetricName;
                
                IF (CountMetricId IS NULL) THEN 
                    BEGIN
                    -- Insert CurrentMetricName
                        INSERT  
                        INTO    CountMetrics 
                                (
                                    Name, 
                                    Description
                                )
                        VALUES  (
                                    CurrentMetricName, 
                                    CurrentMetricDescription
                                );
                        EXCEPTION
                        WHEN OTHERS THEN
                            RAISE EXCEPTION 'Error occurred when inserting CountMetric ''%''; %', COALESCE(CurrentMetricName, '(null)'), SQLERRM;
                    END;
                END IF;

                -- Commit athe transaction to allow the new category and/or metric to be available to other sessions
                COMMIT;

                -- Repeat the original insert
                BEGIN
                    INSERT  
                    INTO    CountMetricInstances 
                            (
                                CategoryId, 
                                CountMetricId, 
                                EventTime
                            )
                    VALUES  (
                                ( 
                                    SELECT  Id 
                                    FROM    Categories 
                                    WHERE   Name = Category 
                                ), 
                                ( 
                                    SELECT  Id 
                                    FROM    CountMetrics
                                    WHERE   Name = CurrentMetricName 
                                ), 
                                CurrentEventTime
                            );
                EXCEPTION
                    WHEN OTHERS THEN    
                        RAISE EXCEPTION 'Error occurred when inserting count metric instance for category ''%'' and count metric ''%''; %', COALESCE(Category, '(null)'), COALESCE(CurrentMetricName, '(null)'), SQLERRM;
                END;

            WHEN OTHERS THEN
                RAISE EXCEPTION 'Error occurred when inserting count metric instance for category ''%'' and count metric ''%''; %', COALESCE(Category, '(null)'), COALESCE(CurrentMetricName, '(null)'), SQLERRM;
        END;

    END LOOP;

END 
$$;

--------------------------------------------------------------------------------
-- InsertAmountMetrics

CREATE OR REPLACE PROCEDURE InsertAmountMetrics
(
    Category       varchar, 
    AmountMetrics   json
)
LANGUAGE 'plpgsql'
AS $$
DECLARE 

    CurrentMetricEventAsJson  json;
    CurrentMetricName         varchar;
    CurrentMetricDescription  varchar;
    CurrentEventTimeAsString  varchar;
    CurrentEventTime          timestamp;
    CurrentAmountAsString     varchar;
    CurrentAmount             bigint;
    CategoryId                bigint;
    AmountMetricId            bigint;

    BEGIN

    FOR CurrentMetricEventAsJson IN 
    SELECT  * 
    FROM    json_array_elements(AmountMetrics)
    LOOP 

        CurrentMetricName := CurrentMetricEventAsJson->>'Name';
        CurrentMetricDescription := CurrentMetricEventAsJson->>'Description';
        CurrentEventTimeAsString := CurrentMetricEventAsJson->>'Time';
        CurrentAmountAsString := CurrentMetricEventAsJson->>'Amount';
        BEGIN
            SELECT  TO_TIMESTAMP(CurrentEventTimeAsString, 'YYYY-MM-DD HH24:MI:ss.US') AS timestamp
            INTO    CurrentEventTime;
        EXCEPTION
            WHEN OTHERS THEN
                RAISE EXCEPTION 'Failed to convert metric Time ''%'' to a timestamp; %', COALESCE(CurrentEventTimeAsString, '(null)'), SQLERRM;
        END;
        BEGIN
            SELECT  CurrentAmountAsString::bigint
            INTO    CurrentAmount;
        EXCEPTION
            WHEN OTHERS THEN
                RAISE EXCEPTION 'Failed to convert metric Amount ''%'' to a bigint; %', COALESCE(CurrentAmountAsString, '(null)'), SQLERRM;
        END;

        BEGIN
            INSERT  
            INTO    AmountMetricInstances  
                    (
                        CategoryId, 
                        AmountMetricId, 
                        EventTime, 
                        Amount 
                    )
            VALUES  (
                        ( 
                            SELECT  Id 
                            FROM    Categories 
                            WHERE   Name = Category 
                        ), 
                        ( 
                            SELECT  Id 
                            FROM    AmountMetrics
                            WHERE   Name = CurrentMetricName 
                        ), 
                        CurrentEventTime, 
                        CurrentAmount
                    );
        EXCEPTION
            WHEN not_null_violation THEN
                -- Insert failed due to 'not_null_violation' error
                --   Need to ensure Category and CurrentMetricName exist

                -- Take an exclusive lock on the Categories table to prevent other sessions attempting to insert the same category 
                --   (or insert the same metric... since the change to the metric table occurs with the same lock on Categories in place)
                LOCK TABLE public.Categories IN EXCLUSIVE MODE;

                SELECT  Id
                INTO    CategoryId 
                FROM    Categories 
                WHERE   Name = Category;

                IF (CategoryId IS NULL) THEN 
                    BEGIN
                        -- Insert Category
                        INSERT  
                        INTO    Categories 
                                (
                                    Name
                                )
                        VALUES  (
                                    Category
                                );
                    EXCEPTION
                        WHEN OTHERS THEN
                            RAISE EXCEPTION 'Error occurred when inserting Category ''%''; %', COALESCE(Category, '(null)'), SQLERRM;
                    END;
                END IF;

                SELECT  Id 
                INTO    AmountMetricId
                FROM    AmountMetrics 
                WHERE   Name = CurrentMetricName;
                
                IF (AmountMetricId IS NULL) THEN 
                    BEGIN
                    -- Insert CurrentMetricName
                        INSERT  
                        INTO    AmountMetrics 
                                (
                                    Name, 
                                    Description
                                )
                        VALUES  (
                                    CurrentMetricName, 
                                    CurrentMetricDescription
                                );
                        EXCEPTION
                        WHEN OTHERS THEN
                            RAISE EXCEPTION 'Error occurred when inserting AmountMetric ''%''; %', COALESCE(CurrentMetricName, '(null)'), SQLERRM;
                    END;
                END IF;

                -- Commit athe transaction to allow the new category and/or metric to be available to other sessions
                COMMIT;

                -- Repeat the original insert
                BEGIN
                    INSERT  
                    INTO    AmountMetricInstances  
                            (
                                CategoryId, 
                                AmountMetricId, 
                                EventTime, 
                                Amount 
                            )
                    VALUES  (
                                ( 
                                    SELECT  Id 
                                    FROM    Categories 
                                    WHERE   Name = Category 
                                ), 
                                ( 
                                    SELECT  Id 
                                    FROM    AmountMetrics
                                    WHERE   Name = CurrentMetricName 
                                ), 
                                CurrentEventTime, 
                                CurrentAmount
                            );
                EXCEPTION
                    WHEN OTHERS THEN    
                        RAISE EXCEPTION 'Error occurred when inserting amount metric instance for category ''%'' and amount metric ''%''; %', COALESCE(Category, '(null)'), COALESCE(CurrentMetricName, '(null)'), SQLERRM;
                END;

            WHEN OTHERS THEN
                RAISE EXCEPTION 'Error occurred when inserting amount metric instance for category ''%'' and amount metric ''%''; %', COALESCE(Category, '(null)'), COALESCE(CurrentMetricName, '(null)'), SQLERRM;
        END;

    END LOOP;

END 
$$;

--------------------------------------------------------------------------------
-- InsertStatusMetrics

CREATE OR REPLACE PROCEDURE InsertStatusMetrics
(
    Category       varchar, 
    StatusMetrics  json
)
LANGUAGE 'plpgsql'
AS $$
DECLARE 

    CurrentMetricEventAsJson  json;
    CurrentMetricName         varchar;
    CurrentMetricDescription  varchar;
    CurrentEventTimeAsString  varchar;
    CurrentEventTime          timestamp;
    CurrentValueAsString      varchar;
    CurrentValue              bigint;
    CategoryId                bigint;
    StatusMetricId            bigint;

    BEGIN

    FOR CurrentMetricEventAsJson IN 
    SELECT  * 
    FROM    json_array_elements(StatusMetrics)
    LOOP 

        CurrentMetricName := CurrentMetricEventAsJson->>'Name';
        CurrentMetricDescription := CurrentMetricEventAsJson->>'Description';
        CurrentEventTimeAsString := CurrentMetricEventAsJson->>'Time';
        CurrentValueAsString := CurrentMetricEventAsJson->>'Value';
        BEGIN
            SELECT  TO_TIMESTAMP(CurrentEventTimeAsString, 'YYYY-MM-DD HH24:MI:ss.US') AS timestamp
            INTO    CurrentEventTime;
        EXCEPTION
            WHEN OTHERS THEN
                RAISE EXCEPTION 'Failed to convert metric Time ''%'' to a timestamp; %', COALESCE(CurrentEventTimeAsString, '(null)'), SQLERRM;
        END;
        BEGIN
            SELECT  CurrentValueAsString::bigint
            INTO    CurrentValue;
        EXCEPTION
            WHEN OTHERS THEN
                RAISE EXCEPTION 'Failed to convert metric Value ''%'' to a bigint; %', COALESCE(CurrentValueAsString, '(null)'), SQLERRM;
        END;

        BEGIN
            INSERT  
            INTO    StatusMetricInstances   
                    (
                        CategoryId, 
                        StatusMetricId, 
                        EventTime, 
                        Value  
                    )
            VALUES  (
                        ( 
                            SELECT  Id 
                            FROM    Categories 
                            WHERE   Name = Category 
                        ), 
                        ( 
                            SELECT  Id 
                            FROM    StatusMetrics
                            WHERE   Name = CurrentMetricName 
                        ), 
                        CurrentEventTime, 
                        CurrentValue 
                    );
        EXCEPTION
            WHEN not_null_violation THEN
                -- Insert failed due to 'not_null_violation' error
                --   Need to ensure Category and CurrentMetricName exist

                -- Take an exclusive lock on the Categories table to prevent other sessions attempting to insert the same category 
                --   (or insert the same metric... since the change to the metric table occurs with the same lock on Categories in place)
                LOCK TABLE public.Categories IN EXCLUSIVE MODE;

                SELECT  Id
                INTO    CategoryId 
                FROM    Categories 
                WHERE   Name = Category;

                IF (CategoryId IS NULL) THEN 
                    BEGIN
                        -- Insert Category
                        INSERT  
                        INTO    Categories 
                                (
                                    Name
                                )
                        VALUES  (
                                    Category
                                );
                    EXCEPTION
                        WHEN OTHERS THEN
                            RAISE EXCEPTION 'Error occurred when inserting Category ''%''; %', COALESCE(Category, '(null)'), SQLERRM;
                    END;
                END IF;

                SELECT  Id 
                INTO    StatusMetricId 
                FROM    StatusMetrics  
                WHERE   Name = CurrentMetricName;
                
                IF (StatusMetricId IS NULL) THEN 
                    BEGIN
                    -- Insert CurrentMetricName
                        INSERT  
                        INTO    StatusMetrics  
                                (
                                    Name, 
                                    Description
                                )
                        VALUES  (
                                    CurrentMetricName, 
                                    CurrentMetricDescription
                                );
                        EXCEPTION
                        WHEN OTHERS THEN
                            RAISE EXCEPTION 'Error occurred when inserting StatusMetric ''%''; %', COALESCE(CurrentMetricName, '(null)'), SQLERRM;
                    END;
                END IF;

                -- Commit athe transaction to allow the new category and/or metric to be available to other sessions
                COMMIT;

                -- Repeat the original insert
                BEGIN
                    INSERT  
                    INTO    StatusMetricInstances   
                            (
                                CategoryId, 
                                StatusMetricId, 
                                EventTime, 
                                Value  
                            )
                    VALUES  (
                                ( 
                                    SELECT  Id 
                                    FROM    Categories 
                                    WHERE   Name = Category 
                                ), 
                                ( 
                                    SELECT  Id 
                                    FROM    StatusMetrics
                                    WHERE   Name = CurrentMetricName 
                                ), 
                                CurrentEventTime, 
                                CurrentValue 
                            );
                EXCEPTION
                    WHEN OTHERS THEN    
                        RAISE EXCEPTION 'Error occurred when inserting status metric instance for category ''%'' and status metric ''%''; %', COALESCE(Category, '(null)'), COALESCE(CurrentMetricName, '(null)'), SQLERRM;
                END;

            WHEN OTHERS THEN
                RAISE EXCEPTION 'Error occurred when inserting status metric instance for category ''%'' and status metric ''%''; %', COALESCE(Category, '(null)'), COALESCE(CurrentMetricName, '(null)'), SQLERRM;
        END;

    END LOOP;

END 
$$;

--------------------------------------------------------------------------------
-- InsertIntervalMetrics

CREATE OR REPLACE PROCEDURE InsertIntervalMetrics
(
    Category         varchar, 
    IntervalMetrics  json
)
LANGUAGE 'plpgsql'
AS $$
DECLARE 

    CurrentMetricEventAsJson  json;
    CurrentMetricName         varchar;
    CurrentMetricDescription  varchar;
    CurrentEventTimeAsString  varchar;
    CurrentEventTime          timestamp;
    CurrentDurationAsString   varchar;
    CurrentDuration           bigint;
    CategoryId                bigint;
    IntervalMetricId          bigint;

    BEGIN

    FOR CurrentMetricEventAsJson IN 
    SELECT  * 
    FROM    json_array_elements(IntervalMetrics)
    LOOP 

        CurrentMetricName := CurrentMetricEventAsJson->>'Name';
        CurrentMetricDescription := CurrentMetricEventAsJson->>'Description';
        CurrentEventTimeAsString := CurrentMetricEventAsJson->>'Time';
        CurrentDurationAsString := CurrentMetricEventAsJson->>'Duration';
        BEGIN
            SELECT  TO_TIMESTAMP(CurrentEventTimeAsString, 'YYYY-MM-DD HH24:MI:ss.US') AS timestamp
            INTO    CurrentEventTime;
        EXCEPTION
            WHEN OTHERS THEN
                RAISE EXCEPTION 'Failed to convert metric Time ''%'' to a timestamp; %', COALESCE(CurrentEventTimeAsString, '(null)'), SQLERRM;
        END;
        BEGIN
            SELECT  CurrentDurationAsString::bigint
            INTO    CurrentDuration;
        EXCEPTION
            WHEN OTHERS THEN
                RAISE EXCEPTION 'Failed to convert metric Duration ''%'' to a bigint; %', COALESCE(CurrentDurationAsString, '(null)'), SQLERRM;
        END;

        BEGIN
            INSERT  
            INTO    IntervalMetricInstances    
                    (
                        CategoryId, 
                        IntervalMetricId, 
                        EventTime, 
                        Duration   
                    )
            VALUES  (
                        ( 
                            SELECT  Id 
                            FROM    Categories 
                            WHERE   Name = Category 
                        ), 
                        ( 
                            SELECT  Id 
                            FROM    IntervalMetrics
                            WHERE   Name = CurrentMetricName 
                        ), 
                        CurrentEventTime, 
                        CurrentDuration  
                    );
        EXCEPTION
            WHEN not_null_violation THEN
                -- Insert failed due to 'not_null_violation' error
                --   Need to ensure Category and CurrentMetricName exist

                -- Take an exclusive lock on the Categories table to prevent other sessions attempting to insert the same category 
                --   (or insert the same metric... since the change to the metric table occurs with the same lock on Categories in place)
                LOCK TABLE public.Categories IN EXCLUSIVE MODE;

                SELECT  Id
                INTO    CategoryId 
                FROM    Categories 
                WHERE   Name = Category;

                IF (CategoryId IS NULL) THEN 
                    BEGIN
                        -- Insert Category
                        INSERT  
                        INTO    Categories 
                                (
                                    Name
                                )
                        VALUES  (
                                    Category
                                );
                    EXCEPTION
                        WHEN OTHERS THEN
                            RAISE EXCEPTION 'Error occurred when inserting Category ''%''; %', COALESCE(Category, '(null)'), SQLERRM;
                    END;
                END IF;

                SELECT  Id 
                INTO    IntervalMetricId  
                FROM    IntervalMetrics 
                WHERE   Name = CurrentMetricName;
                
                IF (IntervalMetricId IS NULL) THEN 
                    BEGIN
                    -- Insert CurrentMetricName
                        INSERT  
                        INTO    IntervalMetrics  
                                (
                                    Name, 
                                    Description
                                )
                        VALUES  (
                                    CurrentMetricName, 
                                    CurrentMetricDescription
                                );
                        EXCEPTION
                        WHEN OTHERS THEN
                            RAISE EXCEPTION 'Error occurred when inserting IntervalMetric ''%''; %', COALESCE(CurrentMetricName, '(null)'), SQLERRM;
                    END;
                END IF;

                -- Commit athe transaction to allow the new category and/or metric to be available to other sessions
                COMMIT;

                -- Repeat the original insert
                BEGIN
                    INSERT  
                    INTO    IntervalMetricInstances    
                            (
                                CategoryId, 
                                IntervalMetricId, 
                                EventTime, 
                                Duration   
                            )
                    VALUES  (
                                ( 
                                    SELECT  Id 
                                    FROM    Categories 
                                    WHERE   Name = Category 
                                ), 
                                ( 
                                    SELECT  Id 
                                    FROM    IntervalMetrics
                                    WHERE   Name = CurrentMetricName 
                                ), 
                                CurrentEventTime, 
                                CurrentDuration  
                            );
                EXCEPTION
                    WHEN OTHERS THEN    
                        RAISE EXCEPTION 'Error occurred when inserting interval metric instance for category ''%'' and interval metric ''%''; %', COALESCE(Category, '(null)'), COALESCE(CurrentMetricName, '(null)'), SQLERRM;
                END;

            WHEN OTHERS THEN
                RAISE EXCEPTION 'Error occurred when inserting interval metric instance for category ''%'' and interval metric ''%''; %', COALESCE(Category, '(null)'), COALESCE(CurrentMetricName, '(null)'), SQLERRM;
        END;

    END LOOP;

END 
$$;


--------------------------------------------------------------------------------
--------------------------------------------------------------------------------
-- Update 'SchemaVersions' table
--------------------------------------------------------------------------------
--------------------------------------------------------------------------------

INSERT 
INTO    public.SchemaVersions
        (
            Version, 
            Created
        )
VALUES  (
            '1.0.0', 
            NOW()
        );