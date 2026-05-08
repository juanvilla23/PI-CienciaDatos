DROP TABLE IF EXISTS raw_db.application_train;
DROP TABLE IF EXISTS raw_db.bureau;
DROP TABLE IF EXISTS raw_db.bureau_balance;
DROP TABLE IF EXISTS raw_db.credit_card_balance;
DROP TABLE IF EXISTS raw_db.installments_payments;
DROP TABLE IF EXISTS raw_db.pos_cash_balance;
DROP TABLE IF EXISTS raw_db.previous_application;

CREATE EXTERNAL TABLE raw_db.application_train (
    SK_ID_CURR                      INT,
    TARGET                          INT,
    NAME_CONTRACT_TYPE              STRING,
    CODE_GENDER                     STRING,
    FLAG_OWN_CAR                    STRING,
    FLAG_OWN_REALTY                 STRING,
    CNT_CHILDREN                    INT,
    AMT_INCOME_TOTAL                DOUBLE,
    AMT_CREDIT                      DOUBLE,
    AMT_ANNUITY                     DOUBLE,
    AMT_GOODS_PRICE                 DOUBLE,
    NAME_TYPE_SUITE                 STRING,
    NAME_INCOME_TYPE                STRING,
    NAME_EDUCATION_TYPE             STRING,
    NAME_FAMILY_STATUS              STRING,
    NAME_HOUSING_TYPE               STRING,
    REGION_POPULATION_RELATIVE      DOUBLE,
    DAYS_BIRTH                      INT,
    DAYS_EMPLOYED                   INT,
    DAYS_REGISTRATION               DOUBLE,
    DAYS_ID_PUBLISH                 INT,
    OWN_CAR_AGE                     DOUBLE,
    FLAG_MOBIL                      INT,
    FLAG_EMP_PHONE                  INT,
    FLAG_WORK_PHONE                 INT,
    FLAG_CONT_MOBILE                INT,
    FLAG_PHONE                      INT,
    FLAG_EMAIL                      INT,
    OCCUPATION_TYPE                 STRING,
    CNT_FAM_MEMBERS                 DOUBLE,
    REGION_RATING_CLIENT            INT,
    REGION_RATING_CLIENT_W_CITY     INT,
    WEEKDAY_APPR_PROCESS_START      STRING,
    HOUR_APPR_PROCESS_START         INT,
    REG_REGION_NOT_LIVE_REGION      INT,
    REG_REGION_NOT_WORK_REGION      INT,
    LIVE_REGION_NOT_WORK_REGION     INT,
    REG_CITY_NOT_LIVE_CITY          INT,
    REG_CITY_NOT_WORK_CITY          INT,
    LIVE_CITY_NOT_WORK_CITY         INT,
    ORGANIZATION_TYPE               STRING,
    EXT_SOURCE_1                    DOUBLE,
    EXT_SOURCE_2                    DOUBLE,
    EXT_SOURCE_3                    DOUBLE,
    APARTMENTS_AVG                  DOUBLE,
    BASEMENTAREA_AVG                DOUBLE,
    YEARS_BEGINEXPLUATATION_AVG     DOUBLE,
    YEARS_BUILD_AVG                 DOUBLE,
    COMMONAREA_AVG                  DOUBLE,
    ELEVATORS_AVG                   DOUBLE,
    ENTRANCES_AVG                   DOUBLE,
    FLOORSMAX_AVG                   DOUBLE,
    FLOORSMIN_AVG                   DOUBLE,
    LANDAREA_AVG                    DOUBLE,
    LIVINGAPARTMENTS_AVG            DOUBLE,
    LIVINGAREA_AVG                  DOUBLE,
    NONLIVINGAPARTMENTS_AVG         DOUBLE,
    NONLIVINGAREA_AVG               DOUBLE,
    APARTMENTS_MODE                 DOUBLE,
    BASEMENTAREA_MODE               DOUBLE,
    YEARS_BEGINEXPLUATATION_MODE    DOUBLE,
    YEARS_BUILD_MODE                DOUBLE,
    COMMONAREA_MODE                 DOUBLE,
    ELEVATORS_MODE                  DOUBLE,
    ENTRANCES_MODE                  DOUBLE,
    FLOORSMAX_MODE                  DOUBLE,
    FLOORSMIN_MODE                  DOUBLE,
    LANDAREA_MODE                   DOUBLE,
    LIVINGAPARTMENTS_MODE           DOUBLE,
    LIVINGAREA_MODE                 DOUBLE,
    NONLIVINGAPARTMENTS_MODE        DOUBLE,
    NONLIVINGAREA_MODE              DOUBLE,
    APARTMENTS_MEDI                 DOUBLE,
    BASEMENTAREA_MEDI               DOUBLE,
    YEARS_BEGINEXPLUATATION_MEDI    DOUBLE,
    YEARS_BUILD_MEDI                DOUBLE,
    COMMONAREA_MEDI                 DOUBLE,
    ELEVATORS_MEDI                  DOUBLE,
    ENTRANCES_MEDI                  DOUBLE,
    FLOORSMAX_MEDI                  DOUBLE,
    FLOORSMIN_MEDI                  DOUBLE,
    LANDAREA_MEDI                   DOUBLE,
    LIVINGAPARTMENTS_MEDI           DOUBLE,
    LIVINGAREA_MEDI                 DOUBLE,
    NONLIVINGAPARTMENTS_MEDI        DOUBLE,
    NONLIVINGAREA_MEDI              DOUBLE,
    FONDKAPREMONT_MODE              STRING,
    HOUSETYPE_MODE                  STRING,
    TOTALAREA_MODE                  DOUBLE,
    WALLSMATERIAL_MODE              STRING,
    EMERGENCYSTATE_MODE             STRING,
    OBS_30_CNT_SOCIAL_CIRCLE        DOUBLE,
    DEF_30_CNT_SOCIAL_CIRCLE        DOUBLE,
    OBS_60_CNT_SOCIAL_CIRCLE        DOUBLE,
    DEF_60_CNT_SOCIAL_CIRCLE        DOUBLE,
    DAYS_LAST_PHONE_CHANGE          DOUBLE,
    FLAG_DOCUMENT_2                 INT,
    FLAG_DOCUMENT_3                 INT,
    FLAG_DOCUMENT_4                 INT,
    FLAG_DOCUMENT_5                 INT,
    FLAG_DOCUMENT_6                 INT,
    FLAG_DOCUMENT_7                 INT,
    FLAG_DOCUMENT_8                 INT,
    FLAG_DOCUMENT_9                 INT,
    FLAG_DOCUMENT_10                INT,
    FLAG_DOCUMENT_11                INT,
    FLAG_DOCUMENT_12                INT,
    FLAG_DOCUMENT_13                INT,
    FLAG_DOCUMENT_14                INT,
    FLAG_DOCUMENT_15                INT,
    FLAG_DOCUMENT_16                INT,
    FLAG_DOCUMENT_17                INT,
    FLAG_DOCUMENT_18                INT,
    FLAG_DOCUMENT_19                INT,
    FLAG_DOCUMENT_20                INT,
    FLAG_DOCUMENT_21                INT,
    AMT_REQ_CREDIT_BUREAU_HOUR      DOUBLE,
    AMT_REQ_CREDIT_BUREAU_DAY       DOUBLE,
    AMT_REQ_CREDIT_BUREAU_WEEK      DOUBLE,
    AMT_REQ_CREDIT_BUREAU_MON       DOUBLE,
    AMT_REQ_CREDIT_BUREAU_QRT       DOUBLE,
    AMT_REQ_CREDIT_BUREAU_YEAR      DOUBLE
)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe'
WITH SERDEPROPERTIES (
    'serialization.format' = ',',
    'field.delim'          = ',',
    'serialization.null.format' = ''
)
STORED AS INPUTFORMAT  'org.apache.hadoop.mapred.TextInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
LOCATION 's3://hcdr/raw/application_train/'
TBLPROPERTIES ('skip.header.line.count'='1');

CREATE EXTERNAL TABLE raw_db.bureau (
    SK_ID_CURR              INT,
    SK_ID_BUREAU            BIGINT,
    CREDIT_ACTIVE           STRING,
    CREDIT_CURRENCY         STRING,
    DAYS_CREDIT             INT,
    CREDIT_DAY_OVERDUE      INT,
    DAYS_CREDIT_ENDDATE     DOUBLE,
    DAYS_ENDDATE_FACT       DOUBLE,
    AMT_CREDIT_MAX_OVERDUE  DOUBLE,
    CNT_CREDIT_PROLONG      INT,
    AMT_CREDIT_SUM          DOUBLE,
    AMT_CREDIT_SUM_DEBT     DOUBLE,
    AMT_CREDIT_SUM_LIMIT    DOUBLE,
    AMT_CREDIT_SUM_OVERDUE  DOUBLE,
    CREDIT_TYPE             STRING,
    DAYS_CREDIT_UPDATE      INT,
    AMT_ANNUITY             DOUBLE
)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe'
WITH SERDEPROPERTIES (
    'serialization.format' = ',',
    'field.delim'          = ',',
    'serialization.null.format' = ''
)
STORED AS INPUTFORMAT  'org.apache.hadoop.mapred.TextInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
LOCATION 's3://hcdr/raw/bureau/'
TBLPROPERTIES ('skip.header.line.count'='1');

CREATE EXTERNAL TABLE raw_db.bureau_balance (
    SK_ID_BUREAU    BIGINT,
    MONTHS_BALANCE  INT,
    STATUS          STRING
)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe'
WITH SERDEPROPERTIES (
    'serialization.format' = ',',
    'field.delim'          = ',',
    'serialization.null.format' = ''
)
STORED AS INPUTFORMAT  'org.apache.hadoop.mapred.TextInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
LOCATION 's3://hcdr/raw/bureau_balance/'
TBLPROPERTIES ('skip.header.line.count'='1');

CREATE EXTERNAL TABLE raw_db.credit_card_balance (
    SK_ID_PREV                  BIGINT,
    SK_ID_CURR                  INT,
    MONTHS_BALANCE              INT,
    AMT_BALANCE                 DOUBLE,
    AMT_CREDIT_LIMIT_ACTUAL     INT,
    AMT_DRAWINGS_ATM_CURRENT    DOUBLE,
    AMT_DRAWINGS_CURRENT        DOUBLE,
    AMT_DRAWINGS_OTHER_CURRENT  DOUBLE,
    AMT_DRAWINGS_POS_CURRENT    DOUBLE,
    AMT_INST_MIN_REGULARITY     DOUBLE,
    AMT_PAYMENT_CURRENT         DOUBLE,
    AMT_PAYMENT_TOTAL_CURRENT   DOUBLE,
    AMT_RECEIVABLE_PRINCIPAL    DOUBLE,
    AMT_RECIVABLE               DOUBLE,
    AMT_TOTAL_RECEIVABLE        DOUBLE,
    CNT_DRAWINGS_ATM_CURRENT    DOUBLE,
    CNT_DRAWINGS_CURRENT        INT,
    CNT_DRAWINGS_OTHER_CURRENT  DOUBLE,
    CNT_DRAWINGS_POS_CURRENT    DOUBLE,
    CNT_INSTALMENT_MATURE_CUM   DOUBLE,
    NAME_CONTRACT_STATUS        STRING,
    SK_DPD                      INT,
    SK_DPD_DEF                  INT
)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe'
WITH SERDEPROPERTIES (
    'serialization.format' = ',',
    'field.delim'          = ',',
    'serialization.null.format' = ''
)
STORED AS INPUTFORMAT  'org.apache.hadoop.mapred.TextInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
LOCATION 's3://hcdr/raw/credit_card_balance/'
TBLPROPERTIES ('skip.header.line.count'='1');

CREATE EXTERNAL TABLE raw_db.installments_payments (
    SK_ID_PREV              BIGINT,
    SK_ID_CURR              INT,
    NUM_INSTALMENT_VERSION  DOUBLE,
    NUM_INSTALMENT_NUMBER   INT,
    DAYS_INSTALMENT         DOUBLE,
    DAYS_ENTRY_PAYMENT      DOUBLE,
    AMT_INSTALMENT          DOUBLE,
    AMT_PAYMENT             DOUBLE
)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe'
WITH SERDEPROPERTIES (
    'serialization.format' = ',',
    'field.delim'          = ',',
    'serialization.null.format' = ''
)
STORED AS INPUTFORMAT  'org.apache.hadoop.mapred.TextInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
LOCATION 's3://hcdr/raw/installments_payments/'
TBLPROPERTIES ('skip.header.line.count'='1');

CREATE EXTERNAL TABLE raw_db.pos_cash_balance (
    SK_ID_PREV              BIGINT,
    SK_ID_CURR              INT,
    MONTHS_BALANCE          INT,
    CNT_INSTALMENT          DOUBLE,
    CNT_INSTALMENT_FUTURE   DOUBLE,
    NAME_CONTRACT_STATUS    STRING,
    SK_DPD                  INT,
    SK_DPD_DEF              INT
)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe'
WITH SERDEPROPERTIES (
    'serialization.format' = ',',
    'field.delim'          = ',',
    'serialization.null.format' = ''
)
STORED AS INPUTFORMAT  'org.apache.hadoop.mapred.TextInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
LOCATION 's3://hcdr/raw/POS_CASH_balance/'
TBLPROPERTIES ('skip.header.line.count'='1');

CREATE EXTERNAL TABLE raw_db.previous_application (
    SK_ID_PREV                  BIGINT,
    SK_ID_CURR                  INT,
    NAME_CONTRACT_TYPE          STRING,
    AMT_ANNUITY                 DOUBLE,
    AMT_APPLICATION             DOUBLE,
    AMT_CREDIT                  DOUBLE,
    AMT_DOWN_PAYMENT            DOUBLE,
    AMT_GOODS_PRICE             DOUBLE,
    WEEKDAY_APPR_PROCESS_START  STRING,
    HOUR_APPR_PROCESS_START     INT,
    FLAG_LAST_APPL_PER_CONTRACT STRING,
    NFLAG_LAST_APPL_IN_DAY      INT,
    RATE_DOWN_PAYMENT           DOUBLE,
    RATE_INTEREST_PRIMARY       DOUBLE,
    RATE_INTEREST_PRIVILEGED    DOUBLE,
    NAME_CASH_LOAN_PURPOSE      STRING,
    NAME_CONTRACT_STATUS        STRING,
    DAYS_DECISION               INT,
    NAME_PAYMENT_TYPE           STRING,
    CODE_REJECT_REASON          STRING,
    NAME_TYPE_SUITE             STRING,
    NAME_CLIENT_TYPE            STRING,
    NAME_GOODS_CATEGORY         STRING,
    NAME_PORTFOLIO              STRING,
    NAME_PRODUCT_TYPE           STRING,
    CHANNEL_TYPE                STRING,
    SELLERPLACE_AREA            INT,
    NAME_SELLER_INDUSTRY        STRING,
    CNT_PAYMENT                 DOUBLE,
    NAME_YIELD_GROUP            STRING,
    PRODUCT_COMBINATION         STRING,
    DAYS_FIRST_DRAWING          DOUBLE,
    DAYS_FIRST_DUE              DOUBLE,
    DAYS_LAST_DUE_1ST_VERSION   DOUBLE,
    DAYS_LAST_DUE               DOUBLE,
    DAYS_TERMINATION            DOUBLE,
    NFLAG_INSURED_ON_APPROVAL   DOUBLE
)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe'
WITH SERDEPROPERTIES (
    'serialization.format' = ',',
    'field.delim'          = ',',
    'serialization.null.format' = ''
)
STORED AS INPUTFORMAT  'org.apache.hadoop.mapred.TextInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
LOCATION 's3://hcdr/raw/previous_application/'
TBLPROPERTIES ('skip.header.line.count'='1');