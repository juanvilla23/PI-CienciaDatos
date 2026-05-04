ALTER TABLE raw_db.application_train
SET TBLPROPERTIES ('use.null.for.invalid.data'='true');

SELECT * FROM raw_db.application_train LIMIT 10;

ALTER TABLE raw_db.bureau_balance
SET TBLPROPERTIES ('use.null.for.invalid.data'='true');

SELECT * FROM raw_db.bureau_balance LIMIT 10;

ALTER TABLE raw_db.bureau
SET TBLPROPERTIES ('use.null.for.invalid.data'='true');

SELECT * FROM raw_db.bureau LIMIT 10;

ALTER TABLE raw_db.previous_application
SET TBLPROPERTIES ('use.null.for.invalid.data'='true');

SELECT * FROM raw_db.previous_application LIMIT 10;

ALTER TABLE raw_db.credit_card_balance
SET TBLPROPERTIES ('use.null.for.invalid.data'='true');

SELECT * FROM raw_db.credit_card_balance LIMIT 10;

ALTER TABLE raw_db.installments_payments
SET TBLPROPERTIES ('use.null.for.invalid.data'='true');

SELECT * FROM raw_db.installments_payments LIMIT 10;

ALTER TABLE raw_db.pos_cash_balance
SET TBLPROPERTIES ('use.null.for.invalid.data'='true');

SELECT * FROM raw_db.pos_cash_balance LIMIT 10;