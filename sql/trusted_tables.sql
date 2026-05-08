DROP TABLE IF EXISTS trusted_db.application_train;
DROP TABLE IF EXISTS trusted_db.bureau;
DROP TABLE IF EXISTS trusted_db.bureau_balance;
DROP TABLE IF EXISTS trusted_db.credit_card_balance;
DROP TABLE IF EXISTS trusted_db.installments_payments;
DROP TABLE IF EXISTS trusted_db.pos_cash_balance;
DROP TABLE IF EXISTS trusted_db.previous_application;

CREATE EXTERNAL TABLE trusted_db.application_train (
    sk_id_curr                      INT,
    target                          INT,
    name_contract_type              STRING,
    code_gender                     STRING,
    flag_own_car                    STRING,
    flag_own_realty                 STRING,
    cnt_children                    INT,
    amt_income_total                DOUBLE,
    amt_credit                      DOUBLE,
    amt_annuity                     DOUBLE,
    amt_goods_price                 DOUBLE,
    name_type_suite                 STRING,
    name_income_type                STRING,
    name_education_type             STRING,
    name_family_status              STRING,
    name_housing_type               STRING,
    region_population_relative      DOUBLE,
    days_birth                      INT,
    days_employed                   INT,
    days_registration               DOUBLE,
    days_id_publish                 INT,
    flag_mobil                      INT,
    flag_emp_phone                  INT,
    flag_work_phone                 INT,
    flag_cont_mobile                INT,
    flag_phone                      INT,
    flag_email                      INT,
    occupation_type                 STRING,
    cnt_fam_members                 DOUBLE,
    region_rating_client            INT,
    region_rating_client_w_city     INT,
    weekday_appr_process_start      STRING,
    hour_appr_process_start         INT,
    reg_region_not_live_region      INT,
    reg_region_not_work_region      INT,
    live_region_not_work_region     INT,
    reg_city_not_live_city          INT,
    reg_city_not_work_city          INT,
    live_city_not_work_city         INT,
    organization_type               STRING,
    ext_source_2                    DOUBLE,
    ext_source_3                    DOUBLE,
    obs_30_cnt_social_circle        DOUBLE,
    def_30_cnt_social_circle        DOUBLE,
    obs_60_cnt_social_circle        DOUBLE,
    def_60_cnt_social_circle        DOUBLE,
    days_last_phone_change          DOUBLE,
    flag_document_2                 INT,
    flag_document_3                 INT,
    flag_document_4                 INT,
    flag_document_5                 INT,
    flag_document_6                 INT,
    flag_document_7                 INT,
    flag_document_8                 INT,
    flag_document_9                 INT,
    flag_document_10                INT,
    flag_document_11                INT,
    flag_document_12                INT,
    flag_document_13                INT,
    flag_document_14                INT,
    flag_document_15                INT,
    flag_document_16                INT,
    flag_document_17                INT,
    flag_document_18                INT,
    flag_document_19                INT,
    flag_document_20                INT,
    flag_document_21                INT,
    amt_req_credit_bureau_hour      DOUBLE,
    amt_req_credit_bureau_day       DOUBLE,
    amt_req_credit_bureau_week      DOUBLE,
    amt_req_credit_bureau_mon       DOUBLE,
    amt_req_credit_bureau_qrt       DOUBLE,
    amt_req_credit_bureau_year      DOUBLE,
    flag_days_employed_anomalo      INT
)
STORED AS PARQUET
LOCATION 's3://hcdr/trusted/application_train/'
TBLPROPERTIES ('parquet.compress' = 'SNAPPY');

CREATE EXTERNAL TABLE trusted_db.bureau (
    sk_id_curr              INT,
    sk_id_bureau            BIGINT,
    credit_active           STRING,
    credit_currency         STRING,
    days_credit             INT,
    credit_day_overdue      INT,
    days_credit_enddate     DOUBLE,
    days_enddate_fact       DOUBLE,
    cnt_credit_prolong      INT,
    amt_credit_sum          DOUBLE,
    amt_credit_sum_debt     DOUBLE,
    amt_credit_sum_limit    DOUBLE,
    amt_credit_sum_overdue  DOUBLE,
    credit_type             STRING,
    days_credit_update      INT
)
STORED AS PARQUET
LOCATION 's3://hcdr/trusted/bureau/'
TBLPROPERTIES ('parquet.compress' = 'SNAPPY');

CREATE EXTERNAL TABLE trusted_db.bureau_balance (
    sk_id_bureau    BIGINT,
    months_balance  INT,
    status          STRING
)
STORED AS PARQUET
LOCATION 's3://hcdr/trusted/bureau_balance/'
TBLPROPERTIES ('parquet.compress' = 'SNAPPY');

CREATE EXTERNAL TABLE trusted_db.previous_application (
    sk_id_prev                  BIGINT,
    sk_id_curr                  INT,
    name_contract_type          STRING,
    amt_annuity                 DOUBLE,
    amt_application             DOUBLE,
    amt_credit                  DOUBLE,
    amt_goods_price             DOUBLE,
    weekday_appr_process_start  STRING,
    hour_appr_process_start     INT,
    flag_last_appl_per_contract STRING,
    nflag_last_appl_in_day      INT,
    name_cash_loan_purpose      STRING,
    name_contract_status        STRING,
    days_decision               INT,
    name_payment_type           STRING,
    code_reject_reason          STRING,
    name_client_type            STRING,
    name_goods_category         STRING,
    name_portfolio              STRING,
    name_product_type           STRING,
    channel_type                STRING,
    sellerplace_area            INT,
    name_seller_industry        STRING,
    cnt_payment                 DOUBLE,
    name_yield_group            STRING,
    product_combination         STRING
)
STORED AS PARQUET
LOCATION 's3://hcdr/trusted/previous_application/'
TBLPROPERTIES ('parquet.compress' = 'SNAPPY');

CREATE EXTERNAL TABLE trusted_db.pos_cash_balance (
    sk_id_prev              BIGINT,
    sk_id_curr              INT,
    months_balance          INT,
    cnt_instalment          DOUBLE,
    cnt_instalment_future   DOUBLE,
    name_contract_status    STRING,
    sk_dpd                  INT,
    sk_dpd_def              INT
)
STORED AS PARQUET
LOCATION 's3://hcdr/trusted/pos_cash_balance/'
TBLPROPERTIES ('parquet.compress' = 'SNAPPY');

CREATE EXTERNAL TABLE trusted_db.credit_card_balance (
    sk_id_prev                  BIGINT,
    sk_id_curr                  INT,
    months_balance              INT,
    amt_balance                 DOUBLE,
    amt_credit_limit_actual     INT,
    amt_drawings_atm_current    DOUBLE,
    amt_drawings_current        DOUBLE,
    amt_drawings_other_current  DOUBLE,
    amt_drawings_pos_current    DOUBLE,
    amt_inst_min_regularity     DOUBLE,
    amt_payment_current         DOUBLE,
    amt_payment_total_current   DOUBLE,
    amt_receivable_principal    DOUBLE,
    amt_recivable               DOUBLE,
    amt_total_receivable        DOUBLE,
    cnt_drawings_atm_current    DOUBLE,
    cnt_drawings_current        INT,
    cnt_drawings_other_current  DOUBLE,
    cnt_drawings_pos_current    DOUBLE,
    cnt_instalment_mature_cum   DOUBLE,
    name_contract_status        STRING,
    sk_dpd                      INT,
    sk_dpd_def                  INT
)
STORED AS PARQUET
LOCATION 's3://hcdr/trusted/credit_card_balance/'
TBLPROPERTIES ('parquet.compress' = 'SNAPPY');

CREATE EXTERNAL TABLE trusted_db.installments_payments (
    sk_id_prev              BIGINT,
    sk_id_curr              INT,
    num_instalment_version  DOUBLE,
    num_instalment_number   INT,
    days_instalment         DOUBLE,
    days_entry_payment      DOUBLE,
    amt_instalment          DOUBLE,
    amt_payment             DOUBLE
)
STORED AS PARQUET
LOCATION 's3://hcdr/trusted/installments_payments/'
TBLPROPERTIES ('parquet.compress' = 'SNAPPY');