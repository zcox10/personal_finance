WITH
  get_accounts AS (
  SELECT 
    PARSE_DATE("%Y%m%d", _TABLE_SUFFIX) AS partition_date,
    item_id,
    account_id,
    account_mask,
    account_name,
    account_official_name,
    account_type,
    account_subtype,
    institution_name,
    PARSE_DATE("%Y%m%d", _TABLE_SUFFIX) AS transaction_date,
    IF(account_type = "credit", balance.current * -1, balance.current) AS actual_amount,
    balance.currency_code
  FROM `zsc-personal.personal_finance.financial_accounts_*`
  )
  , accounts_distinct AS (
  SELECT
    item_id,
    account_id,
    account_mask,
    account_name,
    account_official_name,
    account_type,
    account_subtype,
    institution_name,
  FROM get_accounts
  QUALIFY ROW_NUMBER() OVER(PARTITION BY item_id, account_id ORDER BY transaction_date DESC) = 1
  )
  , get_investments AS (
  SELECT
    PARSE_DATE("%Y%m%d", _TABLE_SUFFIX) AS partition_date,
    item_id,
    account_id,
    cost_basis,
    institution_price,
    quantity,
    institution_price_date,
    institution_value,
    currency_code,
    security.type AS security_type,
    security.ticker_symbol,
  FROM `zsc-personal.personal_finance.plaid_investment_holdings_*`
  )
  , join_investments AS (
  SELECT 
    i.*,
    acct.* EXCEPT(item_id, account_id)
  FROM get_investments i
  LEFT JOIN accounts_distinct acct
  USING (item_id, account_id)
  )
  , removed_transactions AS (
  SELECT *
  FROM `zsc-personal.personal_finance.plaid_removed_transactions_*`
  )
  , get_transactions AS (
  SELECT 
    PARSE_DATE("%Y%m%d", _TABLE_SUFFIX) AS partition_date,
    item_id,
    account_id,
    transaction_id,
    transaction_date,
    FORMAT_DATE("%Y-%m", DATE_TRUNC(transaction_date, MONTH)) AS transaction_month,
    IF(amount > 0, "DEBIT", "CREDIT") AS transaction_type,
    amount * -1 AS actual_amount,
    currency_code,

    personal_finance_category.primary AS category_raw,
    personal_finance_category.detailed AS subcategory_raw,

    CASE 
      -- exclude
      WHEN REGEXP_CONTAINS(merchant.name, r"Online Banking transfer") AND institution_name = "Bank of America" THEN "EXCLUDE_CATEGORY"
      WHEN personal_finance_category.detailed = "LOAN_PAYMENTS_CREDIT_CARD_PAYMENT" THEN "EXCLUDE_CATEGORY"

      -- income/deposits
      WHEN merchant.merchant_name = "Spotify" AND amount < 0 THEN "INCOME"

      -- investments
      WHEN
        ( -- if CREDIT amount (amount < 0), then income deposit (TRANSFER_IN); else TRANSFER_OUT
          merchant.merchant_name IN ("Coinbase", "Fundrise Real Estate", "Binance.us", "Gemini") 
          OR counterparties[SAFE_OFFSET(0)].name = "Charles Schwab"
          OR REGEXP_CONTAINS(merchant.name, r"MSPBNA DES")
        ) THEN IF(amount < 0, "TRANSFER_IN", "TRANSFER_OUT")

      -- transportation
      WHEN merchant.merchant_name = "Downtown Tempe Authority" THEN "TRANSPORTATION"

      -- rent
      WHEN 
        REGEXP_CONTAINS(LOWER(merchant.name), r"the palisades in") 
        OR merchant.merchant_name = "Pay Ready Parent" 
      THEN "RENT_AND_UTILITIES"

      -- food/drink
      WHEN merchant.merchant_name = "Ryze" THEN "FOOD_AND_DRINK"
      
      -- education
      WHEN merchant.name = "SEAS GRAD ADM OFF DEPO" THEN "GENERAL_SERVICES"
      
      -- end
      ELSE personal_finance_category.primary
    END AS category_updated,

    CASE 
      -- exclude
      WHEN REGEXP_CONTAINS(merchant.name, r"Online Banking transfer") AND institution_name = "Bank of America" THEN "EXCLUDE_CATEGORY"
      WHEN personal_finance_category.detailed = "LOAN_PAYMENTS_CREDIT_CARD_PAYMENT" THEN "EXCLUDE_CATEGORY"

      -- income/deposits
      WHEN merchant.merchant_name = "Spotify" AND amount < 0 THEN "INCOME_WAGES"

      -- investments / IN
      WHEN
        ( -- if CREDIT amount (amount > 0), then income deposit (TRANSFER_IN); else TRANSFER_OUT
          merchant.merchant_name IN ("Coinbase", "Fundrise Real Estate", "Binance.us", "Gemini") 
          OR counterparties[SAFE_OFFSET(0)].name = "Charles Schwab"
          OR REGEXP_CONTAINS(merchant.name, r"MSPBNA DES")
        ) 
        AND amount < 0 
      THEN "TRANSFER_IN_INVESTMENT_AND_RETIREMENT_FUNDS"

      -- investments / OUT
      WHEN merchant.merchant_name IN ("Coinbase", "Binance.us", "Gemini") AND amount > 0 THEN "TRANSFER_OUT_INVESTMENT_AND_RETIREMENT_FUNDS_CRYPTO"
      WHEN merchant.merchant_name IN ("Fundrise Real Estate") AND amount > 0 THEN "TRANSFER_OUT_INVESTMENT_AND_RETIREMENT_FUNDS_REAL_ESTATE"
      WHEN counterparties[SAFE_OFFSET(0)].name = "Charles Schwab" AND amount > 0 THEN "TRANSFER_OUT_INVESTMENT_AND_RETIREMENT_FUNDS_STOCKS"

      -- transportation
      WHEN merchant.merchant_name = "Downtown Tempe Authority" THEN "TRANSPORTATION_PARKING"

      -- rent
      WHEN 
        REGEXP_CONTAINS(LOWER(merchant.name), r"the palisades in") 
        OR merchant.merchant_name = "Pay Ready Parent" 
      THEN "RENT_AND_UTILITIES_RENT"

      -- personal payment transfers
      WHEN personal_finance_category.detailed = "TRANSFER_OUT_ACCOUNT_TRANSFER" AND merchant.merchant_name NOT IN ("Venmo", "Zelle", "Bank of America", "Chase Bank") THEN "TRANSFER_OUT_OTHER_TRANSFER_OUT"

      -- food/drink
      WHEN merchant.merchant_name = "Ryze" THEN "FOOD_AND_DRINK_OTHER_FOOD_AND_DRINK"
      
      -- education
      WHEN merchant.name = "SEAS GRAD ADM OFF DEPO" THEN "GENERAL_SERVICES_EDUCATION"

      -- end
      ELSE personal_finance_category.detailed
    END AS subcategory_updated,

    payment_channel,

    COALESCE(
      merchant.merchant_name,
      counterparties[SAFE_OFFSET(0)].name,
      CASE
        WHEN REGEXP_CONTAINS(merchant.name, r"DES:") THEN REGEXP_EXTRACT(merchant.name, r"^(.*?)(?: DES:|$)")
        WHEN REGEXP_CONTAINS(merchant.name, r"WITHDRWL") THEN REGEXP_EXTRACT(merchant.name, r'WITHDRWL\s*(.*?)\s*-')
        WHEN REGEXP_CONTAINS(merchant.name, r"Online Banking transfer") AND institution_name = "Bank of America" THEN "Bank of America"
        WHEN REGEXP_CONTAINS(merchant.name, r"Payment Thank You") AND institution_name = "Chase" THEN "Chase Bank"
        ELSE merchant.name
      END
    ) AS merchant_name,

    merchant.merchant_name AS merchant_name_raw,
    counterparties[SAFE_OFFSET(0)].name AS counterparty_name_raw,
    merchant.name AS name_raw,

    counterparties[SAFE_OFFSET(0)].type AS merchant_type,
    acct.* EXCEPT(item_id, account_id)
  FROM `zsc-personal.personal_finance.plaid_transactions_*`
  LEFT JOIN removed_transactions r
  USING (item_id, account_id, transaction_id)
  LEFT JOIN accounts_distinct acct
  USING (item_id, account_id)
  WHERE
    -- if removed transaction is present and removed date >= transaction_date, remove the transaciton
    -- else, even if removed transaction is present and date_removed < transaction_date, keep transaction
    IF(r.transaction_id IS NOT NULL, r.date_removed < transaction_date, TRUE)

    -- remove pending transactions
    AND NOT is_pending
  
  -- remove duplicates
  QUALIFY ROW_NUMBER() OVER(PARTITION BY transaction_id ORDER BY COALESCE(TIMESTAMP(transaction_datetime), TIMESTAMP(transaction_date)) DESC ) = 1
  )
  , budget_values AS (
  SELECT 
    FORMAT_DATE("%Y-%m", PARSE_DATE("%Y%m", _TABLE_SUFFIX)) AS transaction_month,
    DATE(FORMAT_DATE("%Y-%m-%d", PARSE_DATE("%Y%m", _TABLE_SUFFIX))) AS transaction_date,
    *
  FROM `zsc-personal.budget_values.budget_values_*`
  )
  , add_transaction_categories AS (
  SELECT 
    g.*,
    b.category,
    
    CASE
      WHEN b.category = "Personal Investments" AND merchant_name IN ("Coinbase", "Gemini", "Binance.us") THEN "Crypto"
      WHEN b.category = "Personal Investments" AND merchant_name IN ("Fundrise Real Estate") THEN "Real Estate"
      WHEN b.category = "Personal Investments" AND merchant_name IN ("Charles Schwab") THEN "Stocks"
      WHEN b.category = "Personal Investments" THEN "Other"
      ELSE b.subcategory
    END AS subcategory,

    b.detail_category,
  FROM get_transactions g
  LEFT JOIN budget_values b
  ON 
    g.category_updated = b.category_raw
    AND g.subcategory_updated = b.subcategory_raw
    AND g.transaction_month = b.transaction_month
  WHERE g.category_updated != "EXCLUDE_CATEGORY"
  )
  , transactions_agg AS (
  SELECT 
    transaction_month,
    category,
    subcategory,

    -- cast to string to join on budget_values
    IFNULL(detail_category, "null") AS detail_category,

    SUM(actual_amount) AS actual_amount,
    COUNT(DISTINCT transaction_id) AS transactions_count
  FROM add_transaction_categories
  GROUP BY 1,2,3,4
  )
  , join_transactions_agg AS (
  SELECT 
    transaction_date,
    transaction_month,
    category,
    subcategory,
    
    -- cast back to string
    IF(detail_category = "null", CAST(NULL AS STRING), detail_category) AS detail_category,

    budget_amount,
    IFNULL(actual_amount, 0) AS actual_amount,
    IFNULL(transactions_count, 0) AS transactions_count
  FROM ( -- detail_category can be null, and need to join on this field
    SELECT 
      * EXCEPT(detail_category),
      IFNULL(detail_category, "null") AS detail_category
    FROM budget_values
    )
  LEFT JOIN transactions_agg
  USING (transaction_month, category, subcategory, detail_category)
  WHERE 
    -- only include categories that have budget or actual spending
    (budget_amount + actual_amount) != 0
  )
  , union_data AS (
  SELECT 
    CURRENT_DATE() AS partition_date,
    "TRANSACTIONS_AGG" AS metric_category,
    CAST(NULL AS STRING) AS item_id,
    CAST(NULL AS STRING) AS account_id,
    CAST(NULL AS STRING) AS account_mask,
    CAST(NULL AS STRING) AS account_name,
    CAST(NULL AS STRING) AS account_official_name,
    CAST(NULL AS STRING) AS account_type,
    CAST(NULL AS STRING) AS account_subtype,
    CAST(NULL AS STRING) AS institution_name,
    CAST(NULL AS STRING) AS transaction_id,
    transaction_date,
    transaction_month,
    CAST(NULL AS STRING) AS transaction_type,
    budget_amount,
    transactions_count,
    actual_amount,
    CAST(NULL AS STRING) AS currency_code,
    CAST(NULL AS STRING) AS category_raw,
    CAST(NULL AS STRING) AS subcategory_raw,
    CAST(NULL AS STRING) AS category_updated,
    CAST(NULL AS STRING) AS subcategory_updated,
    category,
    subcategory,
    detail_category,
    CONCAT(category, " - ", subcategory) AS final_category,
    CONCAT(category, " - ", subcategory, IF(detail_category IS NULL, "", CONCAT(" - ", detail_category))) AS full_category,
    CAST(NULL AS STRING) AS payment_channel,
    CAST(NULL AS STRING) AS merchant_name,
    CAST(NULL AS STRING) AS merchant_name_raw,
    CAST(NULL AS STRING) AS counterparty_name_raw,
    CAST(NULL AS STRING) AS name_raw,
    CAST(NULL AS STRING) AS merchant_type,
    CAST(NULL AS FLOAT64) AS institution_price,
    CAST(NULL AS FLOAT64) AS quantity,
    CAST(NULL AS FLOAT64) cost_basis, 
    CAST(NULL AS STRING) security_type,
    CAST(NULL AS STRING) ticker_symbol
  FROM join_transactions_agg

  UNION ALL

  SELECT 
    partition_date,
    "TRANSACTIONS" AS metric_category,
    item_id,
    account_id,
    account_mask,
    account_name,
    account_official_name,
    account_type,
    account_subtype,
    institution_name,
    transaction_id,
    transaction_date,
    transaction_month,
    transaction_type,

    CAST(NULL AS FLOAT64) AS budget_amount,
    CAST(NULL AS INT64) AS transactions_count,
    actual_amount,
    currency_code,
    category_raw,
    subcategory_raw,
    category_updated,
    subcategory_updated,
    category,
    subcategory,
    detail_category,
    CONCAT(category, " - ", subcategory) AS final_category,
    CONCAT(category, " - ", subcategory, IF(detail_category IS NULL, "", CONCAT(" - ", detail_category))) AS full_category,
    payment_channel,
    merchant_name,
    merchant_name_raw,
    counterparty_name_raw,
    name_raw,
    merchant_type,
    CAST(NULL AS FLOAT64) AS institution_price,
    CAST(NULL AS FLOAT64) AS quantity,
    CAST(NULL AS FLOAT64) cost_basis, 
    CAST(NULL AS STRING) security_type,
    CAST(NULL AS STRING) ticker_symbol
  FROM add_transaction_categories

  UNION ALL

  SELECT 
    partition_date,
    "ACCOUNTS" AS metric_category,
    item_id,
    account_id,
    account_mask,
    account_name,
    account_official_name,
    account_type,
    account_subtype,
    institution_name,
    CAST(NULL AS STRING) AS transaction_id,
    transaction_date,
    FORMAT_DATE("%Y-%m", DATE_TRUNC(transaction_date, MONTH)) AS transaction_month,
    CAST(NULL AS STRING) AS transaction_type,
    CAST(NULL AS FLOAT64) AS budget_amount,
    CAST(NULL AS INT64) AS transactions_count,
    actual_amount,
    currency_code,
    CAST(NULL AS STRING) AS category_raw,
    CAST(NULL AS STRING) AS subcategory_raw,
    CAST(NULL AS STRING) AS category_updated,
    CAST(NULL AS STRING) AS subcategory_updated,
    CAST(NULL AS STRING) AS category,
    CAST(NULL AS STRING) AS subcategory,
    CAST(NULL AS STRING) AS detail_category,
    CAST(NULL AS STRING) AS final_category,
    CAST(NULL AS STRING) AS full_category,
    CAST(NULL AS STRING) AS payment_channel,
    CAST(NULL AS STRING) AS merchant_name,
    CAST(NULL AS STRING) AS merchant_name_raw,
    CAST(NULL AS STRING) AS counterparty_name_raw,
    CAST(NULL AS STRING) AS name_raw,
    CAST(NULL AS STRING) AS merchant_type,
    CAST(NULL AS FLOAT64) AS institution_price,
    CAST(NULL AS FLOAT64) AS quantity,
    CAST(NULL AS FLOAT64) cost_basis, 
    CAST(NULL AS STRING) security_type,
    CAST(NULL AS STRING) ticker_symbol
  FROM get_accounts

  UNION ALL

  SELECT
    partition_date,
    "INVESTMENTS" AS metric_category,
    item_id,
    account_id,
    account_mask,
    account_name,
    account_official_name,
    account_type,
    account_subtype,
    institution_name,
    CAST(NULL AS STRING) AS transaction_id,
    institution_price_date AS transaction_date,
    FORMAT_DATE("%Y-%m", DATE_TRUNC(institution_price_date, MONTH)) AS transaction_month,
    CAST(NULL AS STRING) AS transaction_type,
    CAST(NULL AS FLOAT64) AS budget_amount,
    CAST(NULL AS INT64) AS transactions_count,
    institution_value AS actual_amount,
    currency_code,
    CAST(NULL AS STRING) AS category_raw,
    CAST(NULL AS STRING) AS subcategory_raw,
    CAST(NULL AS STRING) AS category_updated,
    CAST(NULL AS STRING) AS subcategory_updated,
    CAST(NULL AS STRING) AS category,
    CAST(NULL AS STRING) AS subcategory,
    CAST(NULL AS STRING) AS detail_category,
    CAST(NULL AS STRING) AS final_category,
    CAST(NULL AS STRING) AS full_category,
    CAST(NULL AS STRING) AS payment_channel,
    CAST(NULL AS STRING) AS merchant_name,
    CAST(NULL AS STRING) AS merchant_name_raw,
    CAST(NULL AS STRING) AS counterparty_name_raw,
    CAST(NULL AS STRING) AS name_raw,
    CAST(NULL AS STRING) AS merchant_type,
    institution_price,
    quantity,
    cost_basis, 
    security_type,
    ticker_symbol
  FROM join_investments
  )
SELECT *
FROM union_data