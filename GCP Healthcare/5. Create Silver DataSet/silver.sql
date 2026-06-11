-- IN THIS WE WE WILL IMPLEMENTING BOTH SCD2 AND CDM LOGIC FOR THE SILVER TABLES

-----------------------Departments------------------------
-- Execute an Atomic Full-Load Overwrite with Zero Downtime
CREATE OR REPLACE TABLE `avd-databricks-demo.silver_dataset.departments` (
	DeptID				STRING,
	SRC_DeptID         	STRING,
	Name               	STRING,
	datasource         	STRING,
	is_quarantined     	BOOL,
	insert_timestamp    TIMESTAMP -- Production Audit Trail: Tracks exactly when this snapshot full-load ran
)           
-- Performance Optimization: Speeds up downstream analytics joins when connecting claims to departments 
CLUSTER BY DeptID,datasource

AS ( WITH combined_source AS(
		-- Consolidate hospital source systems in memory
		SELECT DISTINCT DeptID,Name,'HospitalA' AS datasource FROM `avd-databricks-demo.bronze_dataset.departments_ha`
		UNION ALL
		SELECT DISTINCT DeptID,Name,'HospitalB' AS datasource FROM `avd-databricks-demo.bronze_dataset.departments_hb`
							),
	
    cleaned_source AS (
		SELECT DISTINCT
			CONCAT(DeptID,'-',datasource) AS DeptID,
			DeptID AS SRC_DeptID,
			Name,
			datasource,
			-- Data Quality Quarantine Check
			CASE
				WHEN DeptID IS NULL OR Name IS NULL
					THEN TRUE
					ELSE FALSE
			END AS is_quarantined,
			CURRENT_TIMRSTAMP() AS insert_timestamp
		FROM combined_source
					)

	SELECT DeptID,
		SRC_DeptID,
		Name,
		datasource,
		is_quarantined,
		insert_timestamp
	FROM cleaned_source
);

----------------------------Providers-----------------------
-- Execute an Atomic Full-Load Overwrite with Zero Downtime 
CREATE OR REPLACE TABLE `avd-bq-demo.silver_dataset.providers`(
    ProviderID          STRING,
    FirstName           STRING,
    LastName            STRING,
    Specialization      STRING,
    DeptID              STRING,
    NPI                 INT64,
    datasource          STRING,
    is_quarantined      BOOL,
    insert_timestamp    TIMESTAMP -- Tracks the exact snapshot runtime execution footprint
)
-- Performance Optimization: Speeds up downstream analytics joins when connecting claims to departments 
CLUSTER BY ProviderID, datasource AS (
    WITH combined_source AS (
        -- Consolidate hospital source systems in memory
        SELECT DISTINCT 
            ProviderID, FirstName, LastName, Specialization, DeptID, 
            SAFE_CAST(NPI AS INT64) AS NPI,
            'HospitalA' AS datasource 
        FROM `avd-bq-demo.bronze_dataset.providers_ha`
        
        UNION ALL
        
        SELECT DISTINCT 
            ProviderID, FirstName, LastName, Specialization, DeptID, 
            SAFE_CAST(NPI AS INT64) AS NPI, 
            'HospitalB' AS datasource 
        FROM `avd-bq-demo.bronze_dataset.providers_hb`
    ),
    
    cleaned_source AS (
        SELECT DISTINCT 
            ProviderID, 
            FirstName, 
            LastName, 
            Specialization, 
            DeptID, 
            NPI, 
            datasource,
            -- Data Quality Defensive Check
            CASE 
                WHEN ProviderID IS NULL OR DeptID IS NULL THEN TRUE
                ELSE FALSE
            END AS is_quarantined,
            CURRENT_TIMESTAMP() AS insert_timestamp
        FROM combined_source
    )
    
    SELECT 
        ProviderID, 
        FirstName, 
        LastName, 
        Specialization, 
        DeptID, 
        NPI, 
        datasource,
        is_quarantined,
        insert_timestamp
    FROM cleaned_source
);

------------------------Patients----------------------
-- 1. Create patients Table in BigQuery														 
CREATE TABLE IF NOT EXISTS `avd-databricks-demo.silver_dataset.patients`(
	Patient_Key			STRING,
    SRC_PatientID       STRING,
    FirstName           STRING,
    LastName            STRING,
    MiddleName          STRING,
    SSN                 STRING,
    PhoneNumber         STRING,
    Gender              STRING,
    DOB                 DATE,
    Address			    STRING,
    SRC_ModifiedDate    DATE,
    datasource          STRING,
    is_quaratined       BOOL,
    inserted_date	    TIMESTAMP,
    modified_date       TIMESTAMP,
    is_current          BOOL
)
PARTITION BY (inserted_date)
CLUSTER BY SRC_PatientID;	

-- 2. Apply SCD Type 2 Logic with MERGE
MERGE INTO `avd-databricks-demo.silver_dataset.patients` AS target
USING (
	WITH combine_tables AS (
	SELECT PatientID AS SRC_PatientID,
	FirstName,
	LastName,
	MiddleName,
	SSN,
	PhoneNumber,
	Gender,
	PARSE_DATE('%d-%m-%Y',DOB) AS DOB,
	Address,
	PARSE_DATE('%d-%m-%Y',ModifiedDate) AS SRC_ModifiedDate,
	'HospitalA' AS datasource,
	(PatientID IS NULL OR FirstName IS NULL OR lower(FirstName) = 'null' OR DOB IS NULL) AS is_quaratined
	FROM `avd-databricks-demo.bronze_dataset.patients_ha`
	
	UNION ALL 
	
	SELECT ID AS SRC_PatientID,
	F_Name AS FirstName,
	L_Name AS LastName,
	M_Name AS MiddleName,
	SSN,
	PhoneNumber,
	Gender,
	PARSE_DATE('%d-%m-%Y',DOB) AS DOB,
	Address,
	PARSE_DATE('%d-%m-%Y',Updated_Date) AS SRC_ModifiedDate,
	'HospitalB' AS datasource,
	(ID IS NULL OR F_Name IS NULL OR lower(F_Name) = 'null' OR DOB IS NULL) AS is_quaratined
	FROM `avd-databricks-demo.bronze_dataset.patients_hb`
	),
	
	quality_checks AS (
	SELECT CONCAT (SRC_PatientID,'-',datasource) AS Patient_Key,
	SRC_PatientID,
	FirstName,
	LastName,
	MiddleName,
	SSN,
	PhoneNumber,
	Gender,
	DOB,
	Address,
	SRC_ModifiedDate,
	datasource,
	is_quaratined
	FROM combine_tables
	)
	-- Sub-query 1: Rows destined to insert as brand NEW records or overwrite completely new keys	
	SELECT Patient_Key AS Merge_Key,
	source.Patient_Key,
	source.SRC_PatientID,
	source.FirstName,
	source.LastName,
	source.MiddleName,
	source.SSN,
	source.PhoneNumber,
	source.Gender,
	source.DOB,
	source.Address,
	source.SRC_ModifiedDate,
	source.datasource,
	source.is_quaratined
	FROM quality_checks
	
	UNION ALL
	-- Sub-query 2: Rows destined to expire the historical record (forces a MATCH block invocation)
	SELECT 
	NULL as Merge_Key,
	source.Patient_Key,
	source.SRC_PatientID,
	source.FirstName,
	source.LastName,
	source.MiddleName,
	source.SSN,
	source.PhoneNumber,
	source.Gender,
	source.DOB,
	source.Address,
	source.SRC_ModifiedDate,
	source.datasource,
	source.is_quaratined
	
	FROM quality_checks AS source
	JOIN `avd-databricks-demo.silver_dataset.patients` AS target
	ON target.Patient_Key = source.Patient_Key AND target.is_current = TRUE
	WHERE FARM_FINGERPRINT(TO_JASON_STRING(STRUCT(
		target.SRC_PatientID,
		target.FirstName,
		target.LastName,
		target.MiddleName,
		target.SSN,
		target.PhoneNumber,
		target.Gender,
		target.DOB,
		target.Address,
		target.SRC_ModifiedDate,
		target.datasource,
		target.is_quaratined)))
		!=
		FARM_FINGERPRINT(TO_JASON_STRING(STRUCT(
		source.SRC_PatientID,
		source.FirstName,
		source.LastName,
		source.MiddleName,
		source.SSN,
		source.PhoneNumber,
		source.Gender,
		source.DOB,
		source.Address,
		source.SRC_ModifiedDate,
		source.datasource,
		source.is_quaratined)))
		) AS staging
	
	ON target.Patient_Key = staging.Merge_Key AND target.is_current = TRUE
	
	-- When Merge_Key matches, it means a change occurred: close out the active record.
	WHWN MATCHED THEN UPDATE SET target.is_current = FALSE, target.modified_date = CURRENT_TIMESTAMP()
	
	-- When Merge_Key is NULL (from Sub-query 2) or doesn't exist, insert the fresh version.
	WHEN NOT MATCHED THEN INSERT (
		Patient_Key,
	    SRC_PatientID
	    FirstName,
	    LastName,
	    MiddleName,
	    SSN,
	    PhoneNumber,
	    Gender,
	    DOB,
	    Address,
	    SRC_ModifiedDate,
	    datasource,
	    is_quaratined,
	    inserted_date,
	    modified_date,
	    is_current
		) 
		VALUES (
		staging.Patient_Key,
	    staging.SRC_PatientID,
	    staging.FirstName,
	    staging.LastName,
	    staging.MiddleName,
	    staging.SSN,
	    staging.PhoneNumber,
	    staging.Gender,
	    staging.DOB,
	    staging.Address,
	    staging.SRC_ModifiedDate,
	    staging.datasource,
	    staging.is_quaratined,
		CURRENT_TIMESTAMP(),
		CURRENT_TIMESTAMP(),
		TRUE
	);

-----------------------------------Transactions-----------------------------------------
-- 1. Create Transactions Table in BigQuery														 
CREATE TABLE IF NOT EXISTS `avd-databricks-demo.silver_dataset.transactions` (
	Transaction_Key		STRING,
    SRC_TransactionID	STRING,
    EncounterID         STRING,
    PatientID           STRING,
    ProviderID          STRING,
    DeptID              STRING,
    VisitDate           DATE,
    ServiceDate         DATE,
    PaidDate            DATE,
    VisitType           STRING,
    Amount              NUMERIC,
    AmountType          STRING,
    PaidAmount          NUMERIC,
    ClaimID             STRING,
    PayorID             STRING,
    ProcedureCode       INT64,
    ICDCode             STRING,
    LineOfBusiness      STRING,
    MedicaidID          STRING,
    MedicareID          STRING,
    SRC_InsertDate      DATE,
    SRC_ModifiedDate    DATE,
    datasource          STRING,
    is_quarantine       BOOL,
    inserted_date       TIMESTAMP,
    modified_date       TIMESTAMP,
    is_current          BOOL
	)
	PARTITION BY DATE(inserted_date)
	CLUSTER BY SRC_TransactionID;
	
-- 2. Apply SCD Type 2 Logic with MERGE
------------------------------------------------------
MERGE INTO `avd-databricks-demo.silver_dataset.patients` AS target
USING(
	WITH combine_tables AS (
	SELECT
	TransactionID AS SRC_TransactionID	,
	EncounterID			,
	PatientID			,
	ProviderID			,
	DeptID				,
	PARSE_DATE('%d-%m-%Y',VisitDate) AS VisitDate		,
	PARSE_DATE('%d-%m-%Y',ServiceDate) AS ServiceDate	,
	PARSE_DATE('%d-%m-%Y',PaidDate) AS PaidDate			,
	VisitType			,
	SAFE_CAST(Amount AS NUMERIC) AS Amount,
	AmountType			,
	SAFE_CAST(PaidAmount AS NUMERIC) AS PaidAmount	,
	ClaimID				,
	PayorID				,
	SAFE_CAST(ProcedureCode	AS INT64) AS ProcedureCode	,
	ICDCode				,
	LineOfBusiness		,
	MedicaidID			,
	MedicareID			,
	PARSE_DATE('%d-%m-%Y',SRC_InsertDate) AS SRC_InsertDate		,
	PARSE_DATE('%d-%m-%Y',SRC_ModifiedDate)	AS SRC_ModifiedDate	,
	'HospitalA' AS datasource,
	(TransactionID IS NULL OR PatientID IS NULL OR EncounterID IS NULL OR VisitDate IS NULL) AS is_quarantine		,
	FROM `avd-databricks-demo.bronze_dataset.transactions_ha`
	
	UNION ALL
	
	SELECT 
	TransactionID AS SRC_TransactionID	,
	EncounterID			,
	PatientID			,
	ProviderID			,
	DeptID				,
	PARSE_DATE('%d-%m-%Y',VisitDate) AS VisitDate		,
	PARSE_DATE('%d-%m-%Y',ServiceDate) AS ServiceDate	,
	PARSE_DATE('%d-%m-%Y',PaidDate) AS PaidDate			,
	VisitType			,
	SAFE_CAST(Amount AS NUMERIC) AS Amount,
	AmountType			,
	SAFE_CAST(PaidAmount AS NUMERIC) AS PaidAmount	,
	ClaimID				,
	PayorID				,
	SAFE_CAST(ProcedureCode	AS INT64) AS ProcedureCode	,
	ICDCode				,
	LineOfBusiness		,
	MedicaidID			,
	MedicareID			,
	PARSE_DATE('%d-%m-%Y',SRC_InsertDate) AS SRC_InsertDate		,
	PARSE_DATE('%d-%m-%Y',SRC_ModifiedDate)	AS SRC_ModifiedDate	,
	'HospitalB' AS datasource,
	(TransactionID IS NULL OR PatientID IS NULL OR EncounterID IS NULL OR VisitDate IS NULL) AS is_quarantine		,
	FROM `avd-databricks-demo.bronze_dataset.transactions_hb`)
	,
	quality_checks AS (
	SELECT
	CONCAT(SRC_TransactionID,'-',datasource) AS Transaction_Key,	
	SRC_TransactionID	,	
	EncounterID			,
	PatientID			,
	ProviderID			,
	DeptID				,
	VisitDate			,
	ServiceDate			,
	PaidDate			,
	VisitType			,
	Amount				,
	AmountType			,
	PaidAmount			,
	ClaimID				,
	PayorID				,
	ProcedureCode		,
	ICDCode				,
	LineOfBusiness		,
	MedicaidID			,
	MedicareID			,
	SRC_InsertDate		,
	SRC_ModifiedDate	,
	datasource			,
	is_quarantine		
	FROM combine_tables
	)
	-- Sub-query 1: Rows destined to insert as brand NEW records or overwrite completely new keys	
	SELECT
	Transaction_Key AS Merge_Key,
	Transaction_Key		,	
	SRC_TransactionID	,	
	EncounterID			,
	PatientID			,
	ProviderID			,
	DeptID				,
	VisitDate			,
	ServiceDate			,
	PaidDate			,
	VisitType			,
	Amount				,
	AmountType			,
	PaidAmount			,
	ClaimID				,
	PayorID				,
	ProcedureCode		,
	ICDCode				,
	LineOfBusiness		,
	MedicaidID			,
	MedicareID			,
	SRC_InsertDate		,
	SRC_ModifiedDate	,
	datasource			,
	is_quarantine		
	FROM FROM quality_checks
	
	UNION ALL
	-- Sub-query 2: Rows destined to expire the historical record (forces a MATCH block invocation)
	SELECT
	NULL AS Merge_Key	,
	Transaction_Key		,	
	SRC_TransactionID	,	
	EncounterID			,
	PatientID			,
	ProviderID			,
	DeptID				,
	VisitDate			,
	ServiceDate			,
	PaidDate			,
	VisitType			,
	Amount				,
	AmountType			,
	PaidAmount			,
	ClaimID				,
	PayorID				,
	ProcedureCode		,
	ICDCode				,
	LineOfBusiness		,
	MedicaidID			,
	MedicareID			,
	SRC_InsertDate		,
	SRC_ModifiedDate	,
	datasource			,
	is_quarantine		
	
	FROM quality_checks AS source
	
	JOIN `avd-databricks-demo.silver_dataset.transactions` AS target
	ON target.Transaction_Key = source.Transaction_Key AND target.is_current = TRUE
	WHERE FARM_FINGETPRINT(TO_JSON_STRING(STRUCT(
	source.SRC_TransactionID	,	
	source.EncounterID			,
	source.PatientID			,
	source.ProviderID			,
	source.DeptID				,
	source.VisitDate			,
	source.ServiceDate			,
	source.PaidDate				,
	source.VisitType			,
	source.Amount				,
	source.AmountType			,
	source.PaidAmount			,
	source.ClaimID				,
	source.PayorID				,
	source.ProcedureCode		,
	source.ICDCode				,
	source.LineOfBusiness		,
	source.MedicaidID			,
	source.MedicareID			,
	source.SRC_InsertDate		,
	source.SRC_ModifiedDate		,
	source.datasource			,
	source.is_quarantine	
	)))
	!=
	FARM_FINGETPRINT(TO_JSON_STRING(STRUCT(
	target.SRC_TransactionID	,	
	target.EncounterID			,
	target.PatientID			,
	target.ProviderID			,
	target.DeptID				,
	target.VisitDate			,
	target.ServiceDate			,
	target.PaidDate				,
	target.VisitType			,
	target.Amount				,
	target.AmountType			,
	target.PaidAmount			,
	target.ClaimID				,
	target.PayorID				,
	target.ProcedureCode		,
	target.ICDCode				,
	target.LineOfBusiness		,
	target.MedicaidID			,
	target.MedicareID			,
	target.SRC_InsertDate		,
	target.SRC_ModifiedDate		,
	target.datasource			,
	target.is_quarantine	
	)))
		) AS staging
ON target.Transaction_Key = staging.Merge_Key AND target.is_current = TRUE

-- When Merge_Key matches, it means a change occurred: close out the active record.
WHEN MATCHED THEN UPDATE SET target.is_current = FALSE, target.modified_date = CURENT_TIMESTAMP()

-- When Merge_Key is NULL (from Sub-query 2) or doesn't exist, insert the fresh version.
WHEN NOT MATCHED THEN INSERT (
	Transaction_Key		,
    SRC_TransactionID   ,
    EncounterID         ,
    PatientID           ,
    ProviderID          ,
    DeptID              ,
    VisitDate           ,
    ServiceDate         ,
    PaidDate            ,
    VisitType           ,
    Amount              ,
    AmountType          ,
    PaidAmount          ,
    ClaimID             ,
    PayorID             ,
    ProcedureCode       ,
    ICDCode             ,
    LineOfBusiness      ,
    MedicaidID          ,
    MedicareID          ,
    SRC_InsertDate      ,
    SRC_ModifiedDate    ,
    datasource          ,
    is_quarantine       ,
    inserted_date       ,
    modified_date       ,
	is_current			
	) 
	
	VALUES (
	
	staging.Transaction_Key		,
	staging.SRC_TransactionID	,
    staging.EncounterID         ,
	staging.PatientID           ,
    staging.ProviderID          ,
    staging.DeptID              ,
    staging.VisitDate           ,
    staging.ServiceDate         ,
    staging.PaidDate            ,
    staging.VisitType           ,
    staging.Amount              ,
    staging.AmountType          ,
    staging.PaidAmount          ,
    staging.ClaimID             ,
    staging.PayorID             ,
    staging.ProcedureCode       ,
    staging.ICDCode             ,
    staging.LineOfBusiness      ,
    staging.MedicaidID          ,
    staging.MedicareID          ,
    staging.SRC_InsertDate      ,
    staging.SRC_ModifiedDate    ,
    staging.datasource          ,
    staging.is_quarantine       ,
    CURENT_TIMESTAMP()          ,
    CURENT_TIMESTAMP()          ,
	TRUE);

-----------------------Encounters------------------------------------------
-- 1. Create Target encounters Table with Enterprise Specifications
CREATE TABLE IF NOT EXISTS `avd-databricks-demo.silver_dataset.encounters`(
	Encounter_Key		STRING,
	SRC_EncounterID	    STRING,
	PatientID		    STRING,
	EncounterDate	    DATE,
	EncounterType	    STRING,
	ProviderID	        STRING,
	DepartmentID	    STRING,
	ProcedureCode	    INT64,
	SRC_InsertedDate	DATE,
	SRC_ModifiedDate    DATE,
	datasource          STRING,
	is_quarantined      BOOL,
	inserted_date       TIMESTAMP,
	modified_date       TIMESTAMP,
	is_current          BOOL
	)
-- Cost Reduction Strategy: Eliminates slow full-table scans during the MERGE operation
PARTITION BY DATE(inserted_date)
CLUSTER BY Encounter_Key,is_current;

-- 2. Execute the Finalized Union-Split SCD Type 2 MERGE Pipeline
MERGE INTO `avd-databricks-demo.silver_dataset.encounters` AS target
USING ( 
	WITH quality_check AS(
	SELECT EncounterID AS SRC_EncounterID,	
			PatientID,		
			PARSE_DATE('%d-%m-%Y', EncounterDate) AS EncounterDate,	EncounterType,	ProviderID,	DepartmentID,ProcedureCode,	
			PARSE_DATE('%d-%m-%Y', InsertedDate) AS SRC_InsertedDate,	
			PARSE_DATE('%d-%m-%Y', ModifiedDate) AS SRC_ModifiedDate,
			'HospitalA' AS datasource,
			(EncounterID IS NULL OR PatientID IS NULL OR EncounterDate IS NULL OR LOWER(EncounterType) = 'null') AS is_quarantined
	FROM `avd-databricks-demo.bronze_dataset.encounters_ha`
	
	UNION ALL
	
	SELECT EncounterID AS SRC_EncounterID,	
			PatientID,		
			PARSE_DATE('%d-%m-%Y', EncounterDate) AS EncounterDate,	EncounterType,	ProviderID,	DepartmentID,ProcedureCode,	
			PARSE_DATE('%d-%m-%Y', InsertedDate) AS SRC_InsertedDate,	
			PARSE_DATE('%d-%m-%Y', ModifiedDate) AS SRC_ModifiedDate,
			'HospitalB' AS datasource,
			(EncounterID IS NULL OR PatientID IS NULL OR EncounterDate IS NULL OR LOWER(EncounterType) = 'null') AS is_quarantined
	FROM `avd-databricks-demo.bronze_dataset.encounters_hb`
),
	quality_checks AS (
		SELECT 
		CONCAT (SRC_EncounterID,'-',datasource) AS Encounter_Key,	
       SRC_EncounterID,PatientID,		
       EncounterDate, EncounterType, ProviderID, DepartmentID,	ProcedureCode, SRC_InsertedDate, SRC_ModifiedDate, datasource, is_quarantined
FROM quality_check)	
	
	SELECT Encounter_Key AS Merge_Key,* FROM quality_checks AS source
	UNION ALL
	SELECT NULL AS Merge_Key,* 
	FROM quality_checks AS source
	JOIN `avd-databricks-demo.silver_dataset.encounters` AS target
	ON target.Encounter_Key = source.Encounter_Key AND target.is_current = TRUE
	WHERE FARM_FINGERPRINT(TO_JSON_STRING(STRUCT(
		target.SRC_EncounterID,target.PatientID,		target.EncounterDate,	target.EncounterType,	
		target.ProviderID,	    target.DepartmentID,	target.ProcedureCode,	target.SRC_InsertedDate,target.SRC_ModifiedDate,target.datasource,      target.is_quarantined     
		))) != FARM_FINGERPRINT(TO_JSON_STRING(STRUCT(
		source.SRC_EncounterID,source.PatientID,		source.EncounterDate,	source.EncounterType,	source.ProviderID,	    source.DepartmentID,	
		source.ProcedureCode,	source.SRC_InsertedDate,source.SRC_ModifiedDate,source.datasource,      source.is_quarantined)))
) AS staging

ON target.Encounter_Key = staging.Merge_Key AND  target.is_current = TRUE
WHEN MATCHED 
	THEN UPDATE SET target.is_current = FALSE, target.modified_date = CURRENT_TIMESTAMP()
WHEN NOT MATCHED THEN INSERT (
	Encounter_Key,	
	SRC_EncounterID,
	PatientID,		
	EncounterDate,	
	EncounterType,	
	ProviderID,	    
	DepartmentID,	
	ProcedureCode,	
	SRC_InsertedDate,
	SRC_ModifiedDate,
	datasource,      
	is_quarantined,
	inserted_date,
	modified_date,
	is_current
) VALUES (
	staging.Encounter_Key,	
	staging.SRC_EncounterID,
	staging.PatientID,		
	staging.EncounterDate,	
	staging.EncounterType,	
	staging.ProviderID,	    
	staging.DepartmentID,	
	staging.ProcedureCode,	
	staging.SRC_InsertedDate,
	staging.SRC_ModifiedDate,
	staging.datasource,      
	staging.is_quarantined,
	CURRENT_TIMESTAMP(),
	CURRENT_TIMESTAMP(),
	TRUE
);


-----------------------Claims------------------------

-- 1. Create Target Claims Table with Enterprise Specifications
CREATE TABLE IF NOT EXISTS `avd-databricks-demo.silver_dataset.claims` (
	Claim_Key 			STRING,				
	SRC_ClaimID		    STRING,
	TransactionID	    STRING,
	PatientID		    STRING,
	EncounterID	        STRING,
	ProviderID	        STRING,
	DeptID	            STRING,
	ServiceDate	        DATE,
	ClaimDate	        DATE,
	PayorID				STRING,
	ClaimAmount	        NUMERIC,
	PaidAmount	        NUMERIC,
	ClaimStatus	        STRING,
	PayorType	        STRING,
	Deductible	        NUMERIC,
	Coinsurance	        NUMERIC,
	Copay	            NUMERIC,
	SRC_InsertDate	    DATE,
	SRC_ModifiedDate	DATE,
	datasource	        STRING,
	is_quarantined	    BOOL,
	insert_date	        DATE,
	modified_date       DATE,
	is_current          BOOL
-- Cost Reduction Strategy: Eliminates slow full-table scans during the MERGE operation
PARTITION BY insert_date
CLUSTER BY Claim_Key, is_current;
);

-- 2. Execute the Finalized Union-Split SCD Type 2 MERGE Pipeline
MERGE INTO `avd-databricks-demo.silver_dataset.claims` AS target
USING (
	WITH check_quality AS (
	SELECT
		CONCAT(ClaimID,'-',datasource) AS Claim_Key,		
		ClaimID AS SRC_ClaimID,	     
		TransactionID,	 
		PatientID,		 
		EncounterID,	     
		ProviderID,	     
		DeptID,	         
		PARSE_DATE('%d-%m-%Y',ServiceDate) AS ServiceDate,   
		PARSE_DATE('%d-%m-%Y',ClaimDate) AS ClaimDate,	     
		PayorID,			
		SAFE_CAST(ClaimAmount AS NUMERIC) AS ClaimAmount,  
		SAFE_CAST(PaidAmount AS NUMERIC) AS PaidAmount,
		ClaimStatus,	     
		PayorType,	     
		SAFE_CAST(Deductible AS NUMERIC) AS Deductible,   
		SAFE_CAST(Coinsurance AS NUMERIC) AS Coinsurance,
		SAFE_CAST(Copay	AS NUMERIC) AS Copay,
		PARSE_DATE('%d-%m-%Y',InsertDate) AS SRC_InsertDate,
		PARSE_DATE('%d-%m-%Y',ModifiedDate) AS SRC_ModifiedDate,
		datasource,	     
		(ClaimID IS NULL OR PatientID IS NULL OR TransactionID IS NULL OR LOWER(ClaimStatus) = 'null' ) AS is_quarantined	 
	FROM `avd-databricks-demo.bronze_dataset.claims`
	)
	
	SELECT source.Claim_Key AS Merge_Key, source.* FROM check_quality AS source
	UNION ALL
	SELECT NULL AS Merge_Key, source.* FROM check_quality AS source
	JOIN `avd-databricks-demo.silver_dataset.claims` AS target
	ON source.Claim_Key = target.Claim_Key AND target.is_current = TRUE
	WHERE FARM_FINGERPRINT(TO_JSON_STRING(STRUCT(
		source.SRC_ClaimID		,
		source.TransactionID	,
		source.PatientID		,
		source.EncounterID	    ,
		source.ProviderID	    ,
		source.DeptID	        ,
		source.ServiceDate	    ,
		source.ClaimDate	    ,
		source.PayorID			,
		source.ClaimAmount	    ,
		source.PaidAmount	    ,
		source.ClaimStatus	    ,
		source.PayorType	    ,
		source.Deductible	    ,
		source.Coinsurance	    ,
		source.Copay	        ,
		source.SRC_InsertDate	,
		source.SRC_ModifiedDate ,
		source.datasource	    ,
		source.is_quarantined	    
	))) != FARM_FINGERPRINT(TO_JSON_STRING(STRUCT(
	    target.SRC_ClaimID		,
	    target.TransactionID	,
	    target.PatientID		,
	    target.EncounterID	    ,
	    target.ProviderID	    ,
	    target.DeptID	        ,
	    target.ServiceDate	    ,
	    target.ClaimDate	    ,
	    target.PayorID			,
	    target.ClaimAmount	    ,
	    target.PaidAmount	    ,
	    target.ClaimStatus	    ,
	    target.PayorType	    ,
	    target.Deductible	    ,
	    target.Coinsurance	    ,
	    target.Copay	        ,
	    target.SRC_InsertDate	,
	    target.SRC_ModifiedDate ,
	    target.datasource	    ,
	    target.is_quarantined
	)))
) AS staging
ON target.Claim_Key = staging.Merge_Key AND target.is_current = TRUE
WHEN MATCHED THEN UPDATE
	SET target.is_current = FALSE,
	target.modified_date = CURRENT_DATE()

WHEN NOT MATCHED THEN INSERT (
	Claim_Key 			,
    SRC_ClaimID		    ,
    TransactionID	    ,
    PatientID		    ,
    EncounterID	        ,
    ProviderID	        ,
    DeptID	            ,
    ServiceDate	        ,
    ClaimDate	        ,
    PayorID				,
    ClaimAmount	        ,
    PaidAmount	        ,
    ClaimStatus	        ,
    PayorType	        ,
    Deductible	        ,
    Coinsurance	        ,
    Copay	            ,
    SRC_InsertDate	    ,
    SRC_ModifiedDate	,
    datasource	        ,
    is_quarantined	    ,
    insert_date	        ,
    modified_date       ,
    is_current  
		)
VALUES (
	staging.Claim_Key 		,	
	staging.SRC_ClaimID	    ,    
	staging.TransactionID	,
	staging.PatientID		,
	staging.EncounterID	    ,    
	staging.ProviderID	    ,    
	staging.DeptID	        ,    
	staging.ServiceDate	    ,    
	staging.ClaimDate	    ,
	staging.PayorID			,	
	staging.ClaimAmount	    ,    
	staging.PaidAmount	    ,    
	staging.ClaimStatus	    ,    
	staging.PayorType	    ,
	staging.Deductible	    ,    
	staging.Coinsurance	    ,    
	staging.Copay	        ,
	staging.SRC_InsertDate	,    
	staging.SRC_ModifiedDate,
	staging.datasource	    ,    
	staging.is_quarantined	,    
	CURRENT_DATE()	    ,    
	CURRENT_DATE()	    ,    
	TRUE
	);       