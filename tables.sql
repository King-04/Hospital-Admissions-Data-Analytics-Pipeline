CREATE SCHEMA hospital_data;


CREATE TABLE IF NOT EXISTS hospital_data.admission_data (
    SNO INT,
    MRD VARCHAR(20),
    DOA DATE,
    DOD DATE,
    AGE INT,
    GENDER VARCHAR(2),
    RURAL VARCHAR(2),
    TYPE_OF_ADMISSION_EMERGENCY VARCHAR(5),
    month_year VARCHAR(20),
    DURATION_OF_STAY INT,
    duration_of_intensive_unit_stay INT,
    OUTCOME VARCHAR(20),
    SMOKING INT,
    ALCOHOL INT,
    DM INT,
    HTN INT, 
    CAD INT,
    PRIOR_CMP INT,
    CKD INT,
    HB FLOAT,
    TLC FLOAT,
    PLATELETS VARCHAR(20),
    GLUCOSE VARCHAR(20),
    UREA VARCHAR(20),
    CREATININE VARCHAR(20),
    BNP VARCHAR(20),
    RAISED_CARDIAC_ENZYMES INT,
    EF VARCHAR(20),
    SEVERE_ANAEMIA INT,
    ANAEMIA INT,
    STABLE_ANGINA INT,
    ACS INT,
    STEMI INT,
    ATYPICAL_CHEST_PAIN INT,
    HEART_FAILURE INT,
    HFREF INT,
    HFNEF INT,
    VALVULAR INT,
    CHB INT,
    SSS INT,
    AKI INT,
    CVA_INFRACT INT,
    CVA_BLEED INT,
    AF INT,
    VT INT,
    PSVT INT,
    CONGENITAL INT,
    UTI INT,
    NEURO_CARDIOGENIC_SYNCOPE INT,
    ORTHOSTATIC INT,
    INFECTIVE_ENDOCARDITIS INT,
    DVT INT,
    CARDIOGENIC_SHOCK INT,
    SHOCK INT,
    PULMONARY_EMBOLISM INT,
    CHEST_INFECTION INT
);

CREATE TABLE IF NOT EXISTS hospital_data.mortality_data (
    SNO INT,
    MRD VARCHAR(20),
    AGE INT,
    GENDER VARCHAR(2),
    RURAL VARCHAR(2), 
    DATE_OF_BROUGHT_DEAD DATE
);

CREATE TABLE IF NOT EXISTS hospital_data.pollution_data (
    DATES DATE,
    AQI INT,
    PM_AVG INT,
    PM2_MIN INT,
    PM2_MAX INT,
    PM10_AVG INT,
    PM10_MIN INT,
    PM10_MAX VARCHAR(20),
    NO2_AVG INT,
    NO2_MIN INT,
    NO2_MAX INT,
    NH3_AVG INT,
    NH3_MIN INT,
    NH3_MAX INT,
    SO2_AVG INT,
    SO2_MIN INT,
    SO2_MAX INT,
    CO_AVG INT,
    CO_MIN INT,
    CO_MAX INT,
    OZONE_AVG INT,
    OZONE_MIN INT,
    OZONE_MAX INT,
    PROMINENT_POLLUTENT VARCHAR(20),
    MAX_TEMP INT,
    MIN_TEMP INT,
    HUMIDITY VARCHAR(20)
);

SELECT * FROM hospital_data.mortality_data;

SELECT * FROM hospital_data.admission_data;

SELECT * FROM hospital_data.pollution_data;

-- DROP TABLE hospital_data.mortality_data;

-- TRUNCATE TABLE hospital_data.mortality_data;

-- TRUNCATE TABLE hospital_data.pollution_data;

-- DROP TABLE hospital_data.admission_data;

-- DROP TABLE hospital_data.pollution_data;

-- DROP TABLE hospital_data.mortality_data;