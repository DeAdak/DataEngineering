--1. Total Charge Amount per provider by department
-- Step 1: Initialize the Gold Tier Analytics Table
CREATE TABLE IF NOT EXISTS `avd-databricks-demo.gold_dataset.provider_charge_summary`(
    Provider_Name    STRING OPTIONS(description="Full name of the healthcare practitioner"),
    Department       STRING OPTIONS(description="Primary clinical department name associated with the encounter"),
    Charge_Amount    NUMERIC OPTIONS(description="Aggregated gross billing charge amount")
)
OPTIONS(
    description="Analytics-ready aggregation of total transactions and medical billing charges grouped by provider and department."
);

-- Step 2: Clear historical data to allow a clean batch overwrite (Full-Refresh Pattern)
TRUNCATE TABLE `avd-databricks-demo.gold_dataset.provider_charge_summary`;

-- Step 3: Populate the table applying structural health metrics
INSERT INTO `avd-databricks-demo.gold_dataset.provider_charge_summary` (Provider_Name, Department, Charge_Amount)
WITH active_providers AS (
    -- Isolating current, non-quarantined clinical staff variants
    SELECT 
        SPLIT(ProviderID, '-')[SAFE_OFFSET(1)] AS Clean_ProviderID,
        CONCAT(FirstName, ' ', LastName) AS Provider_Name,
        DeptID,
        datasource
    FROM `avd-databricks-demo.silver_dataset.providers`
    WHERE is_quarantined = FALSE
      AND is_current = TRUE -- Ensures we don't duplicate metrics over old provider variants
),

active_departments AS (
    -- Normalizing departments to safeguard aggregations
    SELECT 
        SRC_DeptID,
        Name AS Department_Name,
        datasource
    FROM `avd-databricks-demo.silver_dataset.departments`
    WHERE is_quarantined = FALSE
)

SELECT 
    COALESCE(p.Provider_Name, 'Unassigned Provider') AS Provider_Name,
    COALESCE(d.Department_Name, 'Unknown Department') AS Department,
    ROUND(CAST(SUM(t.Amount) AS NUMERIC), 2) AS Charge_Amount
FROM `avd-databricks-demo.silver_dataset.transactions` AS t

-- Left joins preserve financial reporting metrics even if master data records are delayed
LEFT JOIN active_providers AS p
    ON t.ProviderID = p.Clean_ProviderID
    AND t.datasource = p.datasource

LEFT JOIN active_departments AS d
    ON p.DeptID = d.SRC_DeptID
    AND p.datasource = d.datasource

WHERE t.is_quarantined = FALSE
  AND t.is_current = TRUE
GROUP BY 
    Provider_Name, 
    Department;


--------------------------------------------------------------------------------------------------
--2. Patient History (Gold) : This table provides a complete history of a patient’s visits, diagnoses, and financial interactions.
CREATE TABLE IF NOT EXISTS `avd-databricks-demo.gold_dataset.patient_history`(
    Patient_Key         STRING OPTIONS(description="Unique enterprise patient surrogate key"),
    FirstName           STRING,
    LastName            STRING,
    MiddleName          STRING,
    Gender              STRING,
    DOB                 DATE,
    Address             STRING,
    EncounterID         STRING,
    EncounterDate       DATE,
    EncounterType       STRING,
    Doctor              STRING,
    TransactionID       STRING,
    VisitDate           DATE,
    ServiceDate         DATE,
    PaidDate            DATE,
    BilledAmount        NUMERIC,
    PaidAmount          NUMERIC,
    ClaimAmount         NUMERIC,
    ClaimPaidAmount     NUMERIC,
    ClaimStatus         STRING,
    PayorType           STRING
)
PARTITION BY EncounterDate
CLUSTER BY Patient_Key;

-- DECLARE variables to create a dynamic incremental look-back window
DECLARE lookback_timestamp TIMESTAMP;
DECLARE lookback_date DATE;

-- Look at data modified or inserted in the last 3 days
SET lookback_timestamp = TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 3 DAY);
SET lookback_date = DATE_SUB(CURRENT_DATE(), INTERVAL 3 DAY);

MERGE INTO `avd-databricks-demo.gold_dataset.patient_history` AS target
USING (
    -- CTE 1: Filter ONLY for patients updated within the lookback window
    WITH incremental_patients AS (
        SELECT Patient_Key, SRC_PatientID, FirstName, LastName, MiddleName, Gender, DOB, Address, datasource
        FROM `avd-databricks-demo.silver_dataset.patients`
        WHERE is_current = TRUE 
          AND is_quarantined = FALSE
          AND modified_date >= lookback_timestamp -- Incremental Filter
    ),

    -- CTE 2: Filter ONLY for encounters updated within the lookback window
    incremental_encounters AS (
        SELECT Encounter_Key, EncounterID, PatientID, EncounterType, ProviderID, datasource,
               PARSE_DATE('%d-%m-%Y', EncounterDate) AS EncounterDate
        FROM `avd-databricks-demo.silver_dataset.encounters`
        WHERE is_current = TRUE 
          AND is_quarantined = FALSE
          AND modified_date >= lookback_timestamp -- Incremental Filter
    ),

    current_providers AS (
        SELECT SPLIT(ProviderID, '-')[SAFE_OFFSET(1)] AS Clean_ProviderID,
               CONCAT(FirstName, ' ', LastName) AS Doctor_Name, datasource
        FROM `avd-databricks-demo.silver_dataset.providers`
        WHERE is_current = TRUE AND is_quarantined = FALSE
    ),

    -- CTE 3: Filter ONLY for transactions updated within the lookback window
    incremental_transactions AS (
        SELECT SRC_TransactionID, EncounterID, PatientID, Amount, PaidAmount, datasource,
               PARSE_DATE('%d-%m-%Y', VisitDate) AS VisitDate,
               PARSE_DATE('%d-%m-%Y', ServiceDate) AS ServiceDate,
               PARSE_DATE('%d-%m-%Y', PaidDate) AS PaidDate
        FROM `avd-databricks-demo.silver_dataset.transactions`
        WHERE is_current = TRUE 
          AND is_quarantined = FALSE
          AND modified_date >= lookback_timestamp -- Incremental Filter
    ),

    current_claims AS (
        SELECT TransactionID, ClaimStatus, PayorType, ClaimAmount, PaidAmount, datasource
        FROM `avd-databricks-demo.silver_dataset.claims`
        WHERE is_current = TRUE AND is_quarantined = FALSE
    ),

    -- Combine the incremental datasets
    combined_incremental AS (
        SELECT 
            p.Patient_Key, p.FirstName, p.LastName, p.MiddleName, p.Gender, p.DOB, p.Address,
            e.EncounterID, e.EncounterDate,
            COALESCE(e.EncounterType, 'Unknown') AS EncounterType,
            COALESCE(pr.Doctor_Name, 'Unknown Doctor') AS Doctor,
            t.SRC_TransactionID AS TransactionID, t.VisitDate, t.ServiceDate, t.PaidDate,
            CAST(t.Amount AS NUMERIC) AS BilledAmount,
            CAST(t.PaidAmount AS NUMERIC) AS PaidAmount,
            CAST(c.ClaimAmount AS NUMERIC) AS ClaimAmount,
            CAST(c.PaidAmount AS NUMERIC) AS ClaimPaidAmount,
            COALESCE(c.ClaimStatus, 'No Claim File') AS ClaimStatus,
            COALESCE(c.PayorType, 'Not Listed') AS PayorType
        FROM incremental_patients AS p
        LEFT JOIN incremental_encounters AS e 
            ON p.SRC_PatientID = e.PatientID AND p.datasource = e.datasource
        LEFT JOIN current_providers AS pr 
            ON e.ProviderID = pr.Clean_ProviderID AND e.datasource = pr.datasource
        LEFT JOIN incremental_transactions AS t 
            ON e.EncounterID = t.EncounterID AND e.datasource = t.datasource
        LEFT JOIN current_claims AS c 
            ON t.SRC_TransactionID = c.TransactionID AND t.datasource = c.datasource
    )

    SELECT * FROM combined_incremental
    WHERE EncounterDate >= lookback_date -- Ensures we don't scan deep historical partitions
) AS staging

-- BigQuery Best Practice: Use Partition Pruning in the ON clause to restrict target table scan cost
ON target.EncounterDate >= lookback_date
AND target.Patient_Key = staging.Patient_Key
AND COALESCE(target.EncounterID, 'N/A') = COALESCE(staging.EncounterID, 'N/A')

-- If the patient encounter already exists in the Gold partition, update it with new financial/claim info
WHEN MATCHED THEN
    UPDATE SET
        target.FirstName = staging.FirstName,
        target.LastName = staging.LastName,
        target.Address = staging.Address,
        target.BilledAmount = staging.BilledAmount,
        target.PaidAmount = staging.PaidAmount,
        target.ClaimAmount = staging.ClaimAmount,
        target.ClaimPaidAmount = staging.ClaimPaidAmount,
        target.ClaimStatus = staging.ClaimStatus,
        target.PayorType = staging.PayorType

-- If it is a brand new patient encounter within the lookback window, insert it
WHEN NOT MATCHED THEN
    INSERT (
        Patient_Key, FirstName, LastName, MiddleName, Gender, DOB, Address,
        EncounterID, EncounterDate, EncounterType, Doctor,
        TransactionID, VisitDate, ServiceDate, PaidDate, BilledAmount, PaidAmount,
        ClaimAmount, ClaimPaidAmount, ClaimStatus, PayorType
    )
    VALUES (
        staging.Patient_Key, staging.FirstName, staging.LastName, staging.MiddleName, staging.Gender, staging.DOB, staging.Address,
        staging.EncounterID, staging.EncounterDate, staging.EncounterType, staging.Doctor,
        staging.TransactionID, staging.VisitDate, staging.ServiceDate, staging.PaidDate, staging.BilledAmount, staging.PaidAmount,
        staging.ClaimAmount, staging.ClaimPaidAmount, staging.ClaimStatus, staging.PayorType
    );
--------------------------------------------------------------------------------------------------
-- 3. Provider Performance Summary (Gold) : This table summarizes provider activity, including the number of encounters, total billed amount, and claim success rate.

CREATE TABLE IF NOT EXISTS `avd-databricks-demo.gold_dataset.provider_performance` (
    Provider_Key        STRING OPTIONS(description="Unique business composite identifier"),
    ProviderID          STRING,
    FirstName           STRING,
    LastName            STRING,
    Specialization      STRING,
    TotalEncounters     INT64,
    TotalTransactions   INT64,
    TotalBilledAmount   NUMERIC, -- Changed to numeric for exact currency calculations
    TotalPaidAmount     NUMERIC,
    ApprovedClaims      INT64,
    TotalClaims         INT64,
    ClaimApprovalRate   NUMERIC
)
CLUSTER BY Provider_Key; -- Clustered for fast lookup by reporting dashboards

DECLARE lookback_days INT64 DEFAULT 3;         -- Length of rolling safety window for incremental runs
DECLARE lookback_timestamp TIMESTAMP;           -- Internal operational variables

MERGE INTO `avd-databricks-demo.gold_dataset.provider_performance` AS target
USING (
    -- 1. Identify which provider profiles were added or updated within the incremental window
    WITH updated_providers_flag AS (
        SELECT DISTINCT SPLIT(ProviderID, '-')[SAFE_OFFSET(1)] AS Clean_ProviderID, datasource
        FROM `avd-databricks-demo.silver_dataset.providers`
        WHERE modified_date >= lookback_timestamp AND is_current = TRUE AND is_quarantined = FALSE
        
        UNION DISTINCT
        
        SELECT DISTINCT ProviderID, datasource
        FROM `avd-databricks-demo.silver_dataset.encounters`
        WHERE modified_date >= lookback_timestamp AND is_current = TRUE AND is_quarantined = FALSE
        
        UNION DISTINCT
        
        SELECT DISTINCT ProviderID, datasource
        FROM `avd-databricks-demo.silver_dataset.transactions`
        WHERE modified_date >= lookback_timestamp AND is_current = TRUE AND is_quarantined = FALSE
    ),

    -- 2. Pull down the full profile metadata of affected providers
    active_providers AS (
        SELECT 
            CONCAT(pr.ProviderID, '-', pr.datasource) AS Provider_Key,
            pr.ProviderID, pr.FirstName, pr.LastName, pr.Specialization, pr.datasource,
            SPLIT(pr.ProviderID, '-')[SAFE_OFFSET(1)] AS Clean_ProviderID
        FROM `avd-databricks-demo.silver_dataset.providers` pr
        INNER JOIN updated_providers_flag upf
            ON SPLIT(pr.ProviderID, '-')[SAFE_OFFSET(1)] = upf.Clean_ProviderID
            AND pr.datasource = upf.datasource
        WHERE pr.is_current = TRUE AND pr.is_quarantined = FALSE
    ),

    -- 3. Gather encounters belonging exclusively to the targeted providers
    active_encounters AS (
        SELECT EncounterID, ProviderID, datasource
        FROM `avd-databricks-demo.silver_dataset.encounters`
        WHERE is_current = TRUE AND is_quarantined = FALSE
    ),

    -- 4. Gather transactions belonging exclusively to the targeted providers
    active_transactions AS (
        SELECT SRC_TransactionID, EncounterID, Amount, PaidAmount, datasource
        FROM `avd-databricks-demo.silver_dataset.transactions`
        WHERE is_current = TRUE AND is_quarantined = FALSE
    ),

    -- 5. Gather claims connected downstream to those transactions
    active_claims AS (
        SELECT TransactionID, ClaimStatus, Claim_Key, datasource
        FROM `avd-databricks-demo.silver_dataset.claims`
        WHERE is_current = TRUE AND is_quarantined = FALSE
    )

    -- 6. Perform the linear aggregation chain
    SELECT 
        p.Provider_Key,
        p.ProviderID,
        p.FirstName,
        p.LastName,
        p.Specialization,
        COUNT(DISTINCT e.EncounterID) AS TotalEncounters,
        COUNT(DISTINCT t.SRC_TransactionID) AS TotalTransactions,
        ROUND(CAST(SUM(t.Amount) AS NUMERIC), 2) AS TotalBilledAmount,
        ROUND(CAST(SUM(t.PaidAmount) AS NUMERIC), 2) AS TotalPaidAmount,
        COUNT(DISTINCT CASE WHEN c.ClaimStatus IN ('Approved', 'Paid') THEN c.Claim_Key END) AS ApprovedClaims,
        COUNT(DISTINCT c.Claim_Key) AS TotalClaims,
        ROUND(
            (COUNT(DISTINCT CASE WHEN c.ClaimStatus IN ('Approved', 'Paid') THEN c.Claim_Key END) / 
            NULLIF(COUNT(DISTINCT c.Claim_Key), 0)) * 100, 2
        ) AS ClaimApprovalRate
    FROM active_providers p
    LEFT JOIN active_encounters e 
        ON p.Clean_ProviderID = e.ProviderID AND p.datasource = e.datasource
    LEFT JOIN active_transactions t 
        ON e.EncounterID = t.EncounterID AND e.datasource = t.datasource
    LEFT JOIN active_claims c 
        ON t.SRC_TransactionID = c.TransactionID AND t.datasource = c.datasource
    GROUP BY p.Provider_Key, p.ProviderID, p.FirstName, p.LastName, p.Specialization
) AS staging
ON target.Provider_Key = staging.Provider_Key

-- When matched during daily runs, update the provider activity metric blocks completely
WHEN MATCHED THEN
    UPDATE SET
        target.FirstName = staging.FirstName,
        target.LastName = staging.LastName,
        target.Specialization = staging.Specialization,
        target.TotalEncounters = staging.TotalEncounters,
        target.TotalTransactions = staging.TotalTransactions,
        target.TotalBilledAmount = staging.TotalBilledAmount,
        target.TotalPaidAmount = staging.TotalPaidAmount,
        target.ApprovedClaims = staging.ApprovedClaims,
        target.TotalClaims = staging.TotalClaims,
        target.ClaimApprovalRate = staging.ClaimApprovalRate

-- When not matched, insert fresh performance summary tracking row
WHEN NOT MATCHED THEN
    INSERT (
        Provider_Key, ProviderID, FirstName, LastName, Specialization, 
        TotalEncounters, TotalTransactions, TotalBilledAmount, TotalPaidAmount, 
        ApprovedClaims, TotalClaims, ClaimApprovalRate
    )
    VALUES (
        staging.Provider_Key, staging.ProviderID, staging.FirstName, staging.LastName, staging.Specialization, 
        staging.TotalEncounters, staging.TotalTransactions, staging.TotalBilledAmount, staging.TotalPaidAmount, 
        staging.ApprovedClaims, staging.TotalClaims, staging.ClaimApprovalRate
    );
--------------------------------------------------------------------------------------------------
-- 4. Department Performance Analytics (Gold): Provides insights into department-level efficiency, revenue, and patient volume.
DECLARE lookback_days INT64 DEFAULT 3;         -- Length of rolling safety window for incremental runs
DECLARE lookback_timestamp TIMESTAMP;           -- Internal operational variables

CREATE TABLE IF NOT EXISTS `avd-databricks-demo.gold_dataset.department_performance`(
    Department_Key          STRING OPTIONS(description="Unique business composite identifier"),
    DepartmentID            STRING,
    DepartmentName          STRING,
    TotalEncounters         INT64,
    TotalTransactions       INT64,
    TotalBilledAmount       NUMERIC,
    TotalPaidAmount         NUMERIC,
    UniquePatients          INT64,
    ActiveProviders         INT64,
    RevenueCollectionRate   NUMERIC
)
CLUSTER BY Department_Key; -- Optimized for quick downstream reporting/dashboard lookup

MERGE INTO `avd-databricks-demo.gold_dataset.department_performance` AS target
USING (
    -- 1. Identify which departments had activity modified within the incremental lookback window
    WITH updated_departments_flag AS (
        SELECT DISTINCT SRC_DeptID, datasource
        FROM `avd-databricks-demo.silver_dataset.departments`
        WHERE modified_date >= lookback_timestamp AND is_quarantined = FALSE
        
        UNION DISTINCT
        
        SELECT DISTINCT DepartmentID AS SRC_DeptID, datasource
        FROM `avd-databricks-demo.silver_dataset.encounters`
        WHERE modified_date >= lookback_timestamp AND is_current = TRUE AND is_quarantined = FALSE
        
        UNION DISTINCT
        
        SELECT DISTINCT DeptID AS SRC_DeptID, datasource
        FROM `avd-databricks-demo.silver_dataset.transactions`
        WHERE modified_date >= lookback_timestamp AND is_current = TRUE AND is_quarantined = FALSE
    ),

    -- 2. Pull down the full profile metadata of affected departments
    active_departments AS (
        SELECT 
            CONCAT(d.DeptID, '-', d.datasource) AS Department_Key,
            d.SRC_DeptID,
            d.Name AS DepartmentName,
            d.datasource
        FROM `avd-databricks-demo.silver_dataset.departments` AS d
        INNER JOIN updated_departments_flag AS udf
            ON d.SRC_DeptID = udf.SRC_DeptID
            AND d.datasource = udf.datasource
        WHERE d.is_quarantined = FALSE
    ),

    -- 3. Gather encounters linked to the targeted departments
    active_encounters AS (
        SELECT EncounterID, PatientID, ProviderID, DepartmentID, datasource
        FROM `avd-databricks-demo.silver_dataset.encounters`
        WHERE is_current = TRUE AND is_quarantined = FALSE
    ),

    -- 4. Gather transactions cascading safely downstream from those specific encounters
    active_transactions AS (
        SELECT SRC_TransactionID, EncounterID, Amount, PaidAmount, datasource
        FROM `avd-databricks-demo.silver_dataset.transactions`
        WHERE is_current = TRUE AND is_quarantined = FALSE
    )

    -- 5. Compile metric aggregations sequentially
    SELECT 
        d.Department_Key,
        d.SRC_DeptID AS DepartmentID,
        COALESCE(d.DepartmentName, 'Unassigned Department') AS DepartmentName,
        COUNT(DISTINCT e.EncounterID) AS TotalEncounters,
        COUNT(DISTINCT t.SRC_TransactionID) AS TotalTransactions,
        ROUND(CAST(SUM(t.Amount) AS NUMERIC), 2) AS TotalBilledAmount,
        ROUND(CAST(SUM(t.PaidAmount) AS NUMERIC), 2) AS TotalPaidAmount,
        COUNT(DISTINCT e.PatientID) AS UniquePatients,
        COUNT(DISTINCT e.ProviderID) AS ActiveProviders,
        ROUND(
            (CAST(SUM(t.PaidAmount) AS NUMERIC) / NULLIF(CAST(SUM(t.Amount) AS NUMERIC), 0)) * 100, 2
        ) AS RevenueCollectionRate 
    FROM active_departments AS d
    LEFT JOIN active_encounters AS e 
        ON d.SRC_DeptID = e.DepartmentID 
        AND d.datasource = e.datasource
    LEFT JOIN active_transactions AS t 
        ON e.EncounterID = t.EncounterID 
        AND e.datasource = t.datasource
    GROUP BY d.Department_Key, d.SRC_DeptID, d.DepartmentName
) AS staging
ON target.Department_Key = staging.Department_Key

-- MATCH BLOCK: Overwrites operational efficiency data inside target partitions
WHEN MATCHED THEN
    UPDATE SET
        target.DepartmentName = staging.DepartmentName,
        target.TotalEncounters = staging.TotalEncounters,
        target.TotalTransactions = staging.TotalTransactions,
        target.TotalBilledAmount = staging.TotalBilledAmount,
        target.TotalPaidAmount = staging.TotalPaidAmount,
        target.UniquePatients = staging.UniquePatients,
        target.ActiveProviders = staging.ActiveProviders,
        target.RevenueCollectionRate = staging.RevenueCollectionRate

-- NOT MATCHED BLOCK: Inserts fresh performance analytics records
WHEN NOT MATCHED THEN
    INSERT (
        Department_Key, DepartmentID, DepartmentName, TotalEncounters, TotalTransactions, 
        TotalBilledAmount, TotalPaidAmount, UniquePatients, ActiveProviders, RevenueCollectionRate
    )
    VALUES (
        staging.Department_Key, staging.DepartmentID, staging.DepartmentName, staging.TotalEncounters, staging.TotalTransactions, 
        staging.TotalBilledAmount, staging.TotalPaidAmount, staging.UniquePatients, staging.ActiveProviders, staging.RevenueCollectionRate
    );

--------------------------------------------------------------------------------------------------

-- 5. Financial Metrics (Gold) : Aggregates financial KPIs, such as total revenue, claim success rate, and outstanding balances.
DECLARE lookback_days INT64 DEFAULT 3;         -- Length of rolling safety window for incremental runs
-- Internal operational variables
DECLARE lookback_timestamp TIMESTAMP;

CREATE TABLE IF NOT EXISTS `avd-databricks-demo.gold_dataset.financial_metrics` (
    datasource          STRING    OPTIONS(description="The source facility or hospital system (e.g., HospitalA)"),
    TotalBilledAmount   NUMERIC   OPTIONS(description="Total gross charges generated (Amount)"),
    TotalPaidAmount     NUMERIC   OPTIONS(description="Total actual cash collected/revenue realized (PaidAmount)"),
    OutstandingBalance  NUMERIC   OPTIONS(description="Uncollected gross balance (Billed minus Paid)"),
    TotalClaims         INT64     OPTIONS(description="Total distinct insurance claims submitted"),
    ApprovedClaims      INT64     OPTIONS(description="Total claims successfully approved or paid"),
    ClaimSuccessRate    NUMERIC   OPTIONS(description="Percentage of approved claims relative to total submitted claims")
);

MERGE INTO `avd-databricks-demo.gold_dataset.financial_metrics` AS target
USING (
    -- 1. Identify which facilities had activity modified within the incremental lookback window
    WITH updated_facilities_flag AS (
        SELECT DISTINCT datasource 
        FROM `avd-databricks-demo.silver_dataset.transactions`
        WHERE modified_date >= lookback_timestamp AND is_current = TRUE AND is_quarantined = FALSE
        
        UNION DISTINCT
        
        SELECT DISTINCT datasource 
        FROM `avd-databricks-demo.silver_dataset.claims`
        WHERE modified_date >= lookback_timestamp AND is_current = TRUE AND is_quarantined = FALSE
    ),

    -- 2. Aggregate Transaction-level revenue metrics per facility (only for affected facilities)
    transaction_kpis AS (
        SELECT 
            t.datasource,
            SUM(CAST(t.Amount AS NUMERIC)) AS TotalBilledAmount,
            SUM(CAST(t.PaidAmount AS NUMERIC)) AS TotalPaidAmount,
            SUM(CAST(t.Amount AS NUMERIC)) - SUM(CAST(t.PaidAmount AS NUMERIC)) AS OutstandingBalance
        FROM `avd-databricks-demo.silver_dataset.transactions` AS t
        INNER JOIN updated_facilities_flag AS uff
            ON t.datasource = uff.datasource
        WHERE t.is_current = TRUE AND t.is_quarantined = FALSE
        GROUP BY t.datasource
    ),

    -- 3. Aggregate Claim-level success metrics per facility (only for affected facilities)
    claim_kpis AS (
        SELECT 
            c.datasource,
            COUNT(DISTINCT c.SRC_ClaimID) AS TotalClaims,
            COUNT(DISTINCT CASE WHEN c.ClaimStatus IN ('Approved', 'Paid') THEN c.SRC_ClaimID END) AS ApprovedClaims
        FROM `avd-databricks-demo.silver_dataset.claims` AS c
        INNER JOIN updated_facilities_flag AS uff
            ON c.datasource = uff.datasource
        WHERE c.is_current = TRUE AND c.is_quarantined = FALSE
        GROUP BY c.datasource
    )

    -- 4. Final Selection & Safe Assembly 
    SELECT 
        COALESCE(t.datasource, c.datasource) AS datasource,
        ROUND(COALESCE(t.TotalBilledAmount, 0), 2) AS TotalBilledAmount,
        ROUND(COALESCE(t.TotalPaidAmount, 0), 2) AS TotalPaidAmount,
        ROUND(COALESCE(t.OutstandingBalance, 0), 2) AS OutstandingBalance,
        COALESCE(c.TotalClaims, 0) AS TotalClaims,
        COALESCE(c.ApprovedClaims, 0) AS ApprovedClaims,
        ROUND(
            (COALESCE(c.ApprovedClaims, 0) / NULLIF(COALESCE(c.TotalClaims, 0), 0)) * 100, 2
        ) AS ClaimSuccessRate
    FROM transaction_kpis AS t
    FULL OUTER JOIN claim_kpis AS c 
        ON t.datasource = c.datasource
) AS staging
ON target.datasource = staging.datasource

-- MATCH BLOCK: Overwrites executive level metrics if the facility already exists
WHEN MATCHED THEN
    UPDATE SET
        target.TotalBilledAmount = staging.TotalBilledAmount,
        target.TotalPaidAmount = staging.TotalPaidAmount,
        target.OutstandingBalance = staging.OutstandingBalance,
        target.TotalClaims = staging.TotalClaims,
        target.ApprovedClaims = staging.ApprovedClaims,
        target.ClaimSuccessRate = staging.ClaimSuccessRate

-- NOT MATCHED BLOCK: Inserts fresh tracking records if a new facility dataset lands
WHEN NOT MATCHED THEN
    INSERT (
        datasource, TotalBilledAmount, TotalPaidAmount, OutstandingBalance, 
        TotalClaims, ApprovedClaims, ClaimSuccessRate
    )
    VALUES (
        staging.datasource, staging.TotalBilledAmount, staging.TotalPaidAmount, staging.OutstandingBalance, 
        staging.TotalClaims, staging.ApprovedClaims, staging.ClaimSuccessRate
    );


-- 6. Payor Performance & Claims Summary (Gold): This table tracks the performance of insurance payors, focusing on claim approval rates, payout amounts, and processing efficiency.
-- =========================================================================
-- SYSTEM CONFIGURATION PARAMETERS (Passed by your Orchestrator)
-- =========================================================================
DECLARE is_incremental_run BOOL DEFAULT TRUE; -- Toggle: TRUE = Fast Merge, FALSE = Full History Rebuild
DECLARE lookback_days INT64 DEFAULT 3;         -- Length of rolling safety window for incremental runs

-- Internal operational variables
DECLARE lookback_timestamp TIMESTAMP;

-- Calculate execution windows dynamically
IF is_incremental_run THEN
  SET lookback_timestamp = TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL lookback_days DAY);
ELSE
  SET lookback_timestamp = TIMESTAMP('1970-01-01 00:00:00 UTC');
END IF;


-- =========================================================================
-- STEP 1: DESTINATION INFRASTRUCTURE INITIALIZATION (DDL)
-- =========================================================================
CREATE TABLE IF NOT EXISTS `avd-databricks-demo.gold_dataset.payor_performance` (
    Payor_Key               STRING    OPTIONS(description="Unique business composite identifier"),
    PayorID                 STRING    OPTIONS(description="The unique identifier for the insurance payor (e.g. Medicare, BlueCross)"),
    PayorType               STRING    OPTIONS(description="Classification category (e.g. Private, Government, Self-pay)"),
    TotalClaimsSubmitted    INT64     OPTIONS(description="Total distinct claims sent to this payor"),
    TotalClaimsApproved     INT64     OPTIONS(description="Total distinct claims approved or paid by this payor"),
    ClaimApprovalRate       NUMERIC   OPTIONS(description="Percentage of submitted claims that were approved/paid"),
    TotalClaimedAmount      NUMERIC   OPTIONS(description="The total gross money billed to the insurance payor"),
    TotalPaidAmount         NUMERIC   OPTIONS(description="The actual monetary payout collected from the payor"),
    AverageProcessingDays   NUMERIC   OPTIONS(description="Average days elapsed between Service Date and Claim Submission Date")
)
CLUSTER BY Payor_Key; -- Optimized for instant BI dashboard and lookup performance


-- =========================================================================
-- STEP 2: DYNAMIC EXECUTION CONTROL
-- =========================================================================
IF NOT is_incremental_run THEN
  -- Complete wipeout executed only during full historical loads
  TRUNCATE TABLE `avd-databricks-demo.gold_dataset.payor_performance`;
END IF;


-- =========================================================================
-- STEP 3: UNIFIED IDEMPOTENT DATA PROCESSING LAYER (DML MERGE)
-- =========================================================================
MERGE INTO `avd-databricks-demo.gold_dataset.payor_performance` AS target
USING (
    -- 1. Identify which payor networks had modifications within the incremental lookback window
    WITH updated_payors_flag AS (
        SELECT DISTINCT PayorID, datasource
        FROM `avd-databricks-demo.silver_dataset.claims`
        WHERE modified_date >= lookback_timestamp 
          AND is_current = TRUE 
          AND is_quarantined = FALSE
          AND PayorID IS NOT NULL
    ),

    -- 2. Extract and parse operational claims strictly for targeted payors
    verified_claims_data AS (
        SELECT 
            CONCAT(c.PayorID, '-', c.datasource) AS Payor_Key,
            c.PayorID,
            c.PayorType,
            c.SRC_ClaimID,
            c.ClaimStatus,
            CAST(c.ClaimAmount AS NUMERIC) AS ClaimAmount,
            CAST(c.PaidAmount AS NUMERIC) AS PaidAmount,
            SAFE.PARSE_DATE('%d-%m-%Y', c.ServiceDate) AS parsed_service_date,
            SAFE.PARSE_DATE('%d-%m-%Y', c.ClaimDate) AS parsed_claim_date
        FROM `avd-databricks-demo.silver_dataset.claims` AS c
        INNER JOIN updated_payors_flag AS upf
            ON c.PayorID = upf.PayorID 
            AND c.datasource = upf.datasource
        WHERE c.is_current = TRUE 
          AND c.is_quarantined = FALSE
    )

    -- 3. Run localized analytical aggregations
    SELECT 
        Payor_Key,
        PayorID,
        MAX(PayorType) AS PayorType, -- Aggregate fallback grouping strategy
        COUNT(DISTINCT SRC_ClaimID) AS TotalClaimsSubmitted,
        COUNT(DISTINCT CASE WHEN ClaimStatus IN ('Approved', 'Paid') THEN SRC_ClaimID END) AS TotalClaimsApproved,
        
        ROUND(
            (COUNT(DISTINCT CASE WHEN ClaimStatus IN ('Approved', 'Paid') THEN SRC_ClaimID END) / 
            NULLIF(COUNT(DISTINCT SRC_ClaimID), 0)) * 100, 2
        ) AS ClaimApprovalRate,
        
        ROUND(SUM(ClaimAmount), 2) AS TotalClaimedAmount,
        ROUND(SUM(PaidAmount), 2) AS TotalPaidAmount,
        
        ROUND(
            CAST(AVG(DATE_DIFF(parsed_claim_date, parsed_service_date, DAY)) AS NUMERIC), 1
        ) AS AverageProcessingDays
    FROM verified_claims_data
    GROUP BY Payor_Key, PayorID
) AS staging
ON target.Payor_Key = staging.Payor_Key

-- MATCH BLOCK: Overwrites operational metric profiles if the payor entity already exists
WHEN MATCHED THEN
    UPDATE SET
        target.PayorType = staging.PayorType,
        target.TotalClaimsSubmitted = staging.TotalClaimsSubmitted,
        target.TotalClaimsApproved = staging.TotalClaimsApproved,
        target.ClaimApprovalRate = staging.ClaimApprovalRate,
        target.TotalClaimedAmount = staging.TotalClaimedAmount,
        target.TotalPaidAmount = staging.TotalPaidAmount,
        target.AverageProcessingDays = staging.AverageProcessingDays

-- NOT MATCHED BLOCK: Inserts a fresh tracking profile if a new payor record initializes
WHEN NOT MATCHED THEN
    INSERT (
        Payor_Key, PayorID, PayorType, TotalClaimsSubmitted, TotalClaimsApproved, 
        ClaimApprovalRate, TotalClaimedAmount, TotalPaidAmount, AverageProcessingDays
    )
    VALUES (
        staging.Payor_Key, staging.PayorID, staging.PayorType, staging.TotalClaimsSubmitted, staging.TotalClaimsApproved, 
        staging.ClaimApprovalRate, staging.TotalClaimedAmount, staging.TotalPaidAmount, staging.AverageProcessingDays
    );
