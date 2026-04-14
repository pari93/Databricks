-- =====================================================================
-- CTE 1: Define analysis date range
-- =====================================================================
WITH date_range AS (
    SELECT
        -- Set your own start & end dates
        DATE('2026-03-16') AS start_date,
        DATE('2026-04-14') AS end_date
),

-- =====================================================================
-- CTE 2: SQL compute engines used (STRUCT → STRING)
-- =====================================================================
sql_compute_engines AS (
    SELECT DISTINCT
        to_json(compute) AS compute_json
    FROM system.query.history h
    CROSS JOIN date_range d
    WHERE DATE(h.start_time) BETWEEN d.start_date AND d.end_date
),

-- =====================================================================
-- CTE 3: Extract warehouse_ids from compute JSON
-- =====================================================================
warehouse_ids AS (
    SELECT DISTINCT
        get_json_object(to_json(compute), '$.warehouse_id') AS warehouse_id
    FROM system.query.history h
    CROSS JOIN date_range d
    WHERE get_json_object(to_json(compute), '$.type') = 'WAREHOUSE'
      AND get_json_object(to_json(compute), '$.warehouse_id') IS NOT NULL
      AND DATE(h.start_time) BETWEEN d.start_date AND d.end_date
),

-- =====================================================================
-- CTE 4: SQL workload summary (STRUCT → STRING)
-- =====================================================================
sql_workload_summary AS (
    SELECT
        to_json(compute) AS compute_json,
        COUNT(*) AS num_queries,
        SUM(total_duration_ms) / 1000 AS total_seconds
    FROM system.query.history h
    CROSS JOIN date_range d
    WHERE DATE(h.start_time) BETWEEN d.start_date AND d.end_date
    GROUP BY to_json(compute)
),

-- =====================================================================
-- CTE 5: Cluster metadata
-- =====================================================================
cluster_metadata AS (
    SELECT
        cluster_id,
        cluster_name,
        create_time,
        delete_time
    FROM system.compute.clusters c
    CROSS JOIN date_range d
    WHERE DATE(c.change_date) BETWEEN d.start_date AND d.end_date
),

-- =====================================================================
-- CTE 6: Identify DLT clusters
-- =====================================================================
dlt_clusters AS (
    SELECT *
    FROM cluster_metadata
    WHERE cluster_name LIKE 'dlt-execution-%'
),

-- =====================================================================
-- CTE 7: Approximate DLT cost
-- =====================================================================
dlt_cost_estimate AS (
    SELECT
        COUNT(*) AS num_dlt_clusters,
        COUNT(*) * 6 AS approx_dbu_hours,
        (COUNT(*) * 6) * 81.86 AS approx_cost_in_inr
    FROM dlt_clusters
),

-- =====================================================================
-- CTE 8: Warehouse events for ALL warehouses
-- =====================================================================
warehouse_events AS (
    SELECT
        e.warehouse_id,
        e.event_type,
        e.event_time,
        LEAD(e.event_type) OVER (
            PARTITION BY e.warehouse_id ORDER BY e.event_time
        ) AS next_event_type,
        LEAD(e.event_time) OVER (
            PARTITION BY e.warehouse_id ORDER BY e.event_time
        ) AS next_event_time
    FROM system.compute.warehouse_events e
    INNER JOIN warehouse_ids w
        ON e.warehouse_id = w.warehouse_id
),

-- =====================================================================
-- CTE 9: Warehouse running sessions
-- =====================================================================
warehouse_sessions AS (
    SELECT
        warehouse_id,
        event_time AS session_start,
        next_event_time AS session_end
    FROM warehouse_events
    WHERE event_type = 'RUNNING'
      AND next_event_type IN ('STOPPING', 'STOPPED')
),

-- =====================================================================
-- CTE 10: Filter warehouse sessions by date range
-- =====================================================================
warehouse_filtered AS (
    SELECT
        warehouse_id,
        DATEDIFF(MINUTE, session_start, session_end) AS minutes
    FROM warehouse_sessions s
    CROSS JOIN date_range d
    WHERE DATE(session_start) BETWEEN d.start_date AND d.end_date
),

-- =====================================================================
-- CTE 11: SQL Warehouse cost per warehouse
-- =====================================================================
warehouse_cost AS (
    SELECT
        warehouse_id,
        SUM(minutes) AS total_active_minutes,
        (SUM(minutes) / 60.0) * 4 * 63.67 AS estimated_cost_in_inr
    FROM warehouse_filtered
    GROUP BY warehouse_id
)

-- =====================================================================
-- FINAL NORMALIZED OUTPUT (UNION-SAFE)
-- =====================================================================

-- SQL Compute Engines
SELECT
    'SQL Compute Engines' AS section,
    'compute' AS key,
    compute_json AS value
FROM sql_compute_engines

UNION ALL

-- SQL Workload Summary
SELECT
    'SQL Workload Summary' AS section,
    CONCAT(
        'compute=', compute_json,
        ', num_queries=', CAST(num_queries AS STRING)
    ) AS key,
    CAST(total_seconds AS STRING) AS value
FROM sql_workload_summary

UNION ALL

-- Cluster Metadata
SELECT
    'Cluster Metadata' AS section,
    cluster_id AS key,
    cluster_name AS value
FROM cluster_metadata

UNION ALL

-- DLT Clusters
SELECT
    'DLT Clusters' AS section,
    cluster_id AS key,
    cluster_name AS value
FROM dlt_clusters

UNION ALL

-- Approx DLT Cost
SELECT
    'Approx DLT Cost (INR)' AS section,
    CONCAT(
        'clusters=', CAST(num_dlt_clusters AS STRING),
        ', dbu_hours=', CAST(approx_dbu_hours AS STRING)
    ) AS key,
    CAST(approx_cost_in_inr AS STRING) AS value
FROM dlt_cost_estimate

UNION ALL

-- SQL Warehouse Cost
SELECT
    'SQL Warehouse Cost (INR)' AS section,
    warehouse_id AS key,
    CAST(estimated_cost_in_inr AS STRING) AS value
FROM warehouse_cost;
