SELECT * FROM hospital_data.mortality_data;

SELECT * FROM hospital_data.admission_data;

SELECT * FROM hospital_data.pollution_data;

-- Create dimension tables 

-- 1. dim_date
CREATE TABLE hospital_data.dim_date (
    date_id DATE PRIMARY KEY,
    year INT,
    month INT,
    day INT,
    day_of_week INT,
    is_weekend BOOLEAN,
    quarter INT
);

-- Populate dim_date 
INSERT INTO hospital_data.dim_date (date_id, year, month, day, day_of_week, is_weekend, quarter)
SELECT DISTINCT
    DATES,
    EXTRACT(YEAR FROM DATES),
    EXTRACT(MONTH FROM DATES),
    EXTRACT(DAY FROM DATES),
    EXTRACT(DOW FROM DATES),
    CASE WHEN EXTRACT(DOW FROM DATES) IN (6, 0) THEN TRUE ELSE FALSE END,
    EXTRACT(QUARTER FROM DATES)
FROM hospital_data.pollution_data
UNION
SELECT DISTINCT
    DOA,
    EXTRACT(YEAR FROM DOA),
    EXTRACT(MONTH FROM DOA),
    EXTRACT(DAY FROM DOA),
    EXTRACT(DOW FROM DOA),
    CASE WHEN EXTRACT(DOW FROM DOA) IN (6, 0) THEN TRUE ELSE FALSE END,
    EXTRACT(QUARTER FROM DOA)
FROM hospital_data.admission_data;

SELECT * FROM hospital_data.dim_date;

-- 2. dim_patient
CREATE TABLE hospital_data.dim_patient (
    patient_id VARCHAR(20) PRIMARY KEY,
    age INT,
    gender VARCHAR(2),
    rural VARCHAR(2),
    smoking INT,
    alcohol INT,
    dm INT,
    htn INT,
    cad INT,
    prior_cmp INT,
    ckd INT
);

INSERT INTO hospital_data.dim_patient (patient_id, age, gender, rural, smoking, alcohol, dm, htn, cad, prior_cmp, ckd)
SELECT DISTINCT
    MRD, AGE, GENDER, RURAL, SMOKING, ALCOHOL, DM, HTN, CAD, PRIOR_CMP, CKD
FROM hospital_data.admission_data;


SELECT * FROM hospital_data.dim_patient;

-- 3. dim_admission
CREATE TABLE hospital_data.dim_admission (
    admission_id INT IDENTITY(1,1) PRIMARY KEY,
    patient_id VARCHAR(20) REFERENCES hospital_data.dim_patient(patient_id),
    type_of_admission VARCHAR(5),
    duration_of_stay INT,
    duration_of_intensive_unit_stay INT,
    outcome VARCHAR(20),
    hb FLOAT,
    tlc FLOAT,
    platelets VARCHAR(20),
    glucose VARCHAR(20),
    urea VARCHAR(20),
    creatinine VARCHAR(20),
    bnp VARCHAR(20),
    raised_cardiac_enzymes INT,
    ef VARCHAR(20),
    severe_anaemia INT,
    anaemia INT,
    stable_angina INT,
    acs INT,
    stemi INT,
    atypical_chest_pain INT,
    heart_failure INT,
    hfref INT,
    hfnef INT,
    valvular INT,
    chb INT,
    sss INT,
    aki INT,
    cva_infract INT,
    cva_bleed INT,
    af INT,
    vt INT,
    psvt INT,
    congenital INT,
    uti INT,
    neuro_cardiogenic_syncope INT,
    orthostatic INT,
    infective_endocarditis INT,
    dvt INT,
    cardiogenic_shock INT,
    shock INT,
    pulmonary_embolism INT,
    chest_infection INT
);

INSERT INTO hospital_data.dim_admission (patient_id, type_of_admission, duration_of_stay, duration_of_intensive_unit_stay, outcome, hb, tlc, platelets, glucose, urea, creatinine, bnp, raised_cardiac_enzymes, ef, severe_anaemia, anaemia, stable_angina, acs, stemi, atypical_chest_pain, heart_failure, hfref, hfnef, valvular, chb, sss, aki, cva_infract, cva_bleed, af, vt, psvt, congenital, uti, neuro_cardiogenic_syncope, orthostatic, infective_endocarditis, dvt, cardiogenic_shock, shock, pulmonary_embolism, chest_infection)
SELECT
    MRD, TYPE_OF_ADMISSION_EMERGENCY, DURATION_OF_STAY, duration_of_intensive_unit_stay, OUTCOME, HB, TLC, PLATELETS, GLUCOSE, UREA, CREATININE, BNP, RAISED_CARDIAC_ENZYMES, EF, SEVERE_ANAEMIA, ANAEMIA, STABLE_ANGINA, ACS, STEMI, ATYPICAL_CHEST_PAIN, HEART_FAILURE, HFREF, HFNEF, VALVULAR, CHB, SSS, AKI, CVA_INFRACT, CVA_BLEED, AF, VT, PSVT, CONGENITAL, UTI, NEURO_CARDIOGENIC_SYNCOPE, ORTHOSTATIC, INFECTIVE_ENDOCARDITIS, DVT, CARDIOGENIC_SHOCK, SHOCK, PULMONARY_EMBOLISM, CHEST_INFECTION
FROM hospital_data.admission_data;


SELECT * FROM hospital_data.dim_admission;

-- 4. dim_pollution
CREATE TABLE hospital_data.dim_pollution (
    pollution_date DATE PRIMARY KEY REFERENCES hospital_data.dim_date(date_id),
    aqi INT,
    pm_avg INT,
    pm2_min INT,
    pm2_max INT,
    pm10_avg INT,
    pm10_min INT,
    pm10_max VARCHAR(20),
    no2_avg INT,
    no2_min INT,
    no2_max INT,
    nh3_avg INT,
    nh3_min INT,
    nh3_max INT,
    so2_avg INT,
    so2_min INT,
    so2_max INT,
    co_avg INT,
    co_min INT,
    co_max INT,
    ozone_avg INT,
    ozone_min INT,
    ozone_max INT,
    prominent_pollutent VARCHAR(20),
    max_temp INT,
    min_temp INT,
    humidity VARCHAR(20)
);

INSERT INTO hospital_data.dim_pollution
SELECT * FROM hospital_data.pollution_data;


SELECT * FROM hospital_data.dim_pollution;

-- Create the fact table

-- 5. fact_hospital_events
CREATE TABLE hospital_data.fact_hospital_events (
    event_id INT IDENTITY(1,1) PRIMARY KEY,
    event_type VARCHAR(10),
    event_date DATE REFERENCES hospital_data.dim_date(date_id),
    patient_id VARCHAR(20) REFERENCES hospital_data.dim_patient(patient_id),
    admission_id INT REFERENCES hospital_data.dim_admission(admission_id)
);

-- Create a view to enforce the constraint
CREATE VIEW hospital_data.vw_fact_hospital_events AS
SELECT *
FROM hospital_data.fact_hospital_events
WHERE event_type IN ('ADMISSION', 'MORTALITY');

-- Insert admissions 
INSERT INTO hospital_data.fact_hospital_events (event_type, event_date, patient_id, admission_id)
SELECT 'ADMISSION', DOA, MRD, admission_id
FROM hospital_data.admission_data a
JOIN hospital_data.dim_admission da ON a.MRD = da.patient_id;

-- Insert mortalities 
INSERT INTO hospital_data.fact_hospital_events (event_type, event_date, patient_id, admission_id)
SELECT 'MORTALITY', DATE_OF_BROUGHT_DEAD, MRD, NULL
FROM hospital_data.mortality_data;

SELECT * FROM hospital_data.fact_hospital_events;