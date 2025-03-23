CREATE TEMPORARY FUNCTION TITLE (input STRING) RETURNS STRING LANGUAGE js AS """
function titleCase(input) {
  const stopWords = ['and', 'or', 'but', 'the', 'a', 'an', 'in', 'on', 'at', 'to'];
  return input.split(' ').map(word => {
    if (stopWords.includes(word.toLowerCase())) {
      return word.toLowerCase();
    } else {
      return word.charAt(0).toUpperCase() + word.slice(1).toLowerCase();
    }
  }).join(' ');
}
return titleCase(input);
""";


WITH
  get_accounts AS (
    SELECT
      PARSE_DATE("%Y%m%d", _TABLE_SUFFIX) AS partition_date,
      item_id,
      account_id,
      institution_name,
      CASE
        WHEN institution_name = "Charles Schwab" THEN "Schwab"
        WHEN institution_name = "E*TRADE Financial" THEN "E-Trade"
        WHEN institution_name = "Bank of America" THEN "BoA"
        WHEN account_subtype = "cryptocurrency" THEN "Crypto"
        ELSE institution_name
      END AS account_name,
      CASE
        WHEN institution_name = "Charles Schwab" THEN IF(account_name = "Schwab 529 Plan", "529", account_name)
        WHEN institution_name IN ("Bank of America", "Vanguard", "Chase", "E*TRADE Financial", "Fundrise") THEN TITLE (account_subtype)
        WHEN account_subtype = "cryptocurrency" THEN account_name
        ELSE institution_name
      END AS account_subname,
      TITLE (account_type) AS account_type,
      PARSE_DATE("%Y%m%d", _TABLE_SUFFIX) AS transaction_date,
      IF(account_type = "credit", balance.current * -1, balance.current) AS actual_amount,
      balance.currency_code
    FROM
      `zsc-personal.personal_finance.financial_accounts_*`
  ),
  accounts_distinct AS (
    SELECT
      item_id,
      account_id,
      institution_name,
      account_name,
      account_subname,
      account_type
    FROM
      get_accounts
    QUALIFY
      ROW_NUMBER() OVER (
        PARTITION BY
          item_id,
          account_id
        ORDER BY
          transaction_date DESC
      ) = 1
  ),
  get_investments AS (
    SELECT
      PARSE_DATE("%Y%m%d", _TABLE_SUFFIX) AS partition_date,
      item_id,
      account_id,
      security.security_id,
      cost_basis,
      institution_value,
      quantity,
      institution_price,
      institution_price_date,
      currency_code,
      security.type AS security_type,
      IFNULL(security.ticker_symbol, security.name) AS ticker_symbol,
      security.name AS security_name,
    FROM
      `zsc-personal.personal_finance.plaid_investment_holdings_*`
    QUALIFY
      ROW_NUMBER() OVER (
        PARTITION BY
          security_id,
          partition_date
        ORDER BY
          institution_price_date DESC,
          institution_value DESC
      ) = 1
  ),
  join_investments AS (
    SELECT
      i.*,
      acct.* EXCEPT (item_id, account_id, institution_name)
    FROM
      get_investments i
      LEFT JOIN accounts_distinct acct USING (item_id, account_id)
  ),
  removed_transactions AS (
    SELECT
      *
    FROM
      `zsc-personal.personal_finance.plaid_removed_transactions_*`
  ),
  get_transactions AS (
    SELECT
      PARSE_DATE("%Y%m%d", _TABLE_SUFFIX) AS partition_date,
      item_id,
      account_id,
      transaction_id,
      transaction_date,
      DATE_TRUNC(transaction_date, MONTH) AS transaction_month,
      IF(amount > 0, "DEBIT", "CREDIT") AS transaction_type,
      amount * -1 AS actual_amount,
      currency_code,
      personal_finance_category.primary AS category_raw,
      personal_finance_category.detailed AS subcategory_raw,
      CASE
      -- exclude
        WHEN REGEXP_CONTAINS(merchant.name, r"Online Banking transfer")
        AND institution_name = "Bank of America" THEN "EXCLUDE_CATEGORY"
        WHEN personal_finance_category.detailed = "LOAN_PAYMENTS_CREDIT_CARD_PAYMENT" THEN "EXCLUDE_CATEGORY"
        -- income/deposits
        WHEN merchant.merchant_name = "Spotify"
        AND amount < 0 THEN "INCOME"
        -- atm
        WHEN merchant.name LIKE "%WITHDRWL KAPIL GROCER%" THEN "TRANSFER_OUT"
        -- investments
        WHEN ( -- if CREDIT amount (amount < 0), then income deposit (TRANSFER_IN); else TRANSFER_OUT
          merchant.merchant_name IN ("Coinbase", "Fundrise Real Estate", "Binance.us", "Gemini")
          OR counterparties[SAFE_OFFSET(0)].name = "Charles Schwab"
          OR REGEXP_CONTAINS(merchant.name, r"MSPBNA DES")
        ) THEN IF(amount < 0, "TRANSFER_IN", "TRANSFER_OUT")
        -- transportation
        WHEN merchant.merchant_name = "Downtown Tempe Authority" THEN "TRANSPORTATION"
        -- rent
        WHEN REGEXP_CONTAINS(LOWER(merchant.name), r"the palisades in")
        OR merchant.merchant_name = "Pay Ready Parent"
        OR REGEXP_CONTAINS(merchant.name, r"Irina Tesis") THEN "RENT_AND_UTILITIES"
        -- laundry
        WHEN merchant.merchant_name = "Hercules Corp" THEN "PERSONAL_CARE"
        -- food/drink
        WHEN merchant.merchant_name = "Cb & Cb Pima Crossing" THEN "FOOD_AND_DRINK"
        WHEN merchant.merchant_name = "Ryze" THEN "FOOD_AND_DRINK"
        WHEN merchant.merchant_name = "Ollieseats" THEN "FOOD_AND_DRINK"
        WHEN merchant.merchant_name = "Blanco" THEN "FOOD_AND_DRINK"
        -- education
        WHEN merchant.name = "EB *10TH ANNUAL BLOOMB" THEN "GENERAL_SERVICES"
        WHEN STARTS_WITH(merchant.name, "SEAS GRAD") THEN "GENERAL_SERVICES"
        WHEN merchant.merchant_name = "Cu Pawprint" THEN "GENERAL_SERVICES"
        WHEN REGEXP_EXTRACT(merchant.name, r"^(.*?)(?: DES:|$)") = "COLUMBIA" THEN "GENERAL_SERVICES"
        -- shopping
        WHEN merchant.merchant_name IN ("Amazon Prime", "Drscholls", "Nike") THEN "GENERAL_MERCHANDISE"
        -- phone
        WHEN merchant.merchant_name = "Apple" THEN "RENT_AND_UTILITIES"
        -- tech
        WHEN merchant.merchant_name IN ("Plaid Technologies Inc", "OpenAI", "1Password", "Colab", "Medium Annual")
        OR STARTS_WITH(merchant.name, "CLOUD") THEN "GENERAL_SERVICES"
        -- entertainment
        WHEN merchant.merchant_name IN ("Tru Med", "IGN") THEN "ENTERTAINMENT"
        WHEN LOWER(merchant.name) LIKE "%footprint center%" THEN "ENTERTAINMENT"
        WHEN merchant.name LIKE "%FLOWERY UPPER WS%" THEN "ENTERTAINMENT"
        -- utilities
        WHEN (
          REGEXP_CONTAINS(merchant.name, r"Hamza Bencheikh")
          AND REGEXP_CONTAINS(LOWER(merchant.name), r"utilities")
        ) THEN "RENT_AND_UTILITIES"
        -- end
        ELSE personal_finance_category.primary
      END AS category_updated,
      CASE
      -- exclude
        WHEN REGEXP_CONTAINS(merchant.name, r"Online Banking transfer")
        AND institution_name = "Bank of America" THEN "EXCLUDE_CATEGORY"
        WHEN personal_finance_category.detailed = "LOAN_PAYMENTS_CREDIT_CARD_PAYMENT" THEN "EXCLUDE_CATEGORY"
        -- income/deposits
        WHEN merchant.merchant_name = "Spotify"
        AND amount < 0 THEN "INCOME_WAGES"
        -- atm
        WHEN merchant.name LIKE "%WITHDRWL KAPIL GROCER%" THEN "TRANSFER_OUT_WITHDRAWAL"
        -- investments / IN
        WHEN ( -- if CREDIT amount (amount > 0), then income deposit (TRANSFER_IN); else TRANSFER_OUT
          merchant.merchant_name IN ("Coinbase", "Fundrise Real Estate", "Binance.us", "Gemini")
          OR counterparties[SAFE_OFFSET(0)].name = "Charles Schwab"
          OR REGEXP_CONTAINS(merchant.name, r"MSPBNA DES")
        )
        AND amount < 0 THEN "TRANSFER_IN_INVESTMENT_AND_RETIREMENT_FUNDS"
        -- investments / OUT
        WHEN merchant.merchant_name IN ("Coinbase", "Binance.us", "Gemini")
        AND amount > 0 THEN "TRANSFER_OUT_INVESTMENT_AND_RETIREMENT_FUNDS_CRYPTO"
        WHEN merchant.merchant_name IN ("Fundrise Real Estate")
        AND amount > 0 THEN "TRANSFER_OUT_INVESTMENT_AND_RETIREMENT_FUNDS_REAL_ESTATE"
        WHEN counterparties[SAFE_OFFSET(0)].name IN ("Charles Schwab", "Az Management & Inves")
        AND amount > 0 THEN "TRANSFER_OUT_INVESTMENT_AND_RETIREMENT_FUNDS_STOCKS"
        -- transportation
        WHEN merchant.merchant_name = "Downtown Tempe Authority" THEN "TRANSPORTATION_PARKING"
        -- rent
        WHEN REGEXP_CONTAINS(LOWER(merchant.name), r"the palisades in")
        OR merchant.merchant_name = "Pay Ready Parent"
        OR REGEXP_CONTAINS(merchant.name, r"Irina Tesis") THEN "RENT_AND_UTILITIES_RENT"
        -- personal payment transfers
        WHEN personal_finance_category.detailed = "TRANSFER_OUT_ACCOUNT_TRANSFER"
        AND merchant.merchant_name NOT IN ("Venmo", "Zelle", "Bank of America", "Chase Bank") THEN "TRANSFER_OUT_OTHER_TRANSFER_OUT"
        -- laundry
        WHEN merchant.merchant_name = "Hercules Corp" THEN "PERSONAL_CARE_LAUNDRY_AND_DRY_CLEANING"
        -- food/drink
        WHEN merchant.merchant_name = "Cb & Cb Pima Crossing" THEN "FOOD_AND_DRINK_RESTAURANT"
        WHEN merchant.merchant_name = "Ryze" THEN "FOOD_AND_DRINK_OTHER_FOOD_AND_DRINK"
        WHEN merchant.merchant_name = "Ollieseats" THEN "FOOD_AND_DRINK_RESTAURANT"
        WHEN merchant.merchant_name = "Blanco" THEN "FOOD_AND_DRINK_RESTAURANT"
        -- education
        WHEN merchant.name = "EB *10TH ANNUAL BLOOMB" THEN "GENERAL_SERVICES_EDUCATION"
        WHEN STARTS_WITH(merchant.name, "SEAS GRAD") THEN "GENERAL_SERVICES_EDUCATION"
        WHEN merchant.merchant_name = "Cu Pawprint" THEN "GENERAL_SERVICES_EDUCATION"
        WHEN REGEXP_EXTRACT(merchant.name, r"^(.*?)(?: DES:|$)") = "COLUMBIA" THEN "GENERAL_SERVICES_EDUCATION"
        -- shopping
        WHEN merchant.merchant_name = "Amazon Prime" THEN "GENERAL_MERCHANDISE_ONLINE_MARKETPLACES"
        WHEN merchant.merchant_name IN ("Drscholls", "Nike") THEN "GENERAL_MERCHANDISE_CLOTHING_AND_ACCESSORIES"
        -- phone
        WHEN merchant.merchant_name = "Apple" THEN "RENT_AND_UTILITIES_TELEPHONE"
        -- tech
        WHEN merchant.merchant_name IN ("Plaid Technologies Inc", "OpenAI", "1Password", "Colab", "Medium Annual")
        OR STARTS_WITH(merchant.name, "CLOUD") THEN "GENERAL_SERVICES_TECH"
        -- cannabis
        WHEN merchant.merchant_name IN ("Tru Med") THEN "ENTERTAINMENT_OTHER_ENTERTAINMENT"
        WHEN merchant.name LIKE "%FLOWERY UPPER WS%" THEN "ENTERTAINMENT_OTHER_ENTERTAINMENT"
        WHEN LOWER(merchant.name) LIKE "%footprint center%" THEN "ENTERTAINMENT_SPORTING_EVENTS_AMUSEMENT_PARKS_AND_MUSEUMS"
        WHEN merchant.merchant_name IN ("IGN") THEN "ENTERTAINMENT_VIDEO_GAMES"
        -- utilities
        WHEN (
          REGEXP_CONTAINS(merchant.name, r"Hamza Bencheikh")
          AND REGEXP_CONTAINS(LOWER(merchant.name), r"utilities")
        ) THEN "RENT_AND_UTILITIES_GAS_AND_ELECTRICITY"
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
          WHEN REGEXP_CONTAINS(merchant.name, r"Online Banking transfer")
          AND institution_name = "Bank of America" THEN "Bank of America"
          WHEN REGEXP_CONTAINS(merchant.name, r"Payment Thank You")
          AND institution_name = "Chase" THEN "Chase Bank"
          ELSE merchant.name
        END
      ) AS merchant_name,
      merchant.merchant_name AS merchant_name_raw,
      counterparties[SAFE_OFFSET(0)].name AS counterparty_name_raw,
      merchant.name AS name_raw,
      counterparties[SAFE_OFFSET(0)].type AS merchant_type,
      acct.* EXCEPT (item_id, account_id, institution_name)
    FROM
      `zsc-personal.personal_finance.plaid_transactions_*` t
      LEFT JOIN removed_transactions r USING (item_id, account_id, transaction_id)
      LEFT JOIN accounts_distinct acct USING (item_id, account_id)
    WHERE
      -- only include partitions >= previous year i.e. 2024-07-23 would return all partitions >= 2023-01-01
      t.transaction_date >= DATE_TRUNC(DATE_ADD(CURRENT_DATE(), INTERVAL -2 YEAR), YEAR)
      -- if removed transaction is present and removed date >= transaction_date, remove the transaciton
      -- else, even if removed transaction is present and date_removed < transaction_date, keep transaction
      AND IF(r.transaction_id IS NOT NULL, r.date_removed < transaction_date, TRUE)
      -- remove pending transactions
      AND NOT is_pending
      -- remove duplicates
    QUALIFY
      ROW_NUMBER() OVER (
        PARTITION BY
          transaction_id
        ORDER BY
          COALESCE(TIMESTAMP(transaction_datetime), TIMESTAMP(transaction_date)) DESC
      ) = 1
  ),
  budget_values AS (
    SELECT
      DATE_TRUNC(PARSE_DATE("%Y%m", _TABLE_SUFFIX), MONTH) AS transaction_month,
      DATE(FORMAT_DATE("%Y-%m-%d", PARSE_DATE("%Y%m", _TABLE_SUFFIX))) AS transaction_date,
      *
    FROM
      `zsc-personal.budget_values.budget_values_*`
  ),
  add_transaction_categories AS (
    SELECT
      g.*,
      b.category,
      CASE
        WHEN b.category = "Personal Investments"
        AND merchant_name IN ("Coinbase", "Gemini", "Binance.us") THEN "Crypto"
        WHEN b.category = "Personal Investments"
        AND merchant_name IN ("Fundrise Real Estate") THEN "Real Estate"
        WHEN b.category = "Personal Investments"
        AND merchant_name IN ("Charles Schwab") THEN "Stocks"
        WHEN b.category = "Personal Investments" THEN "Other"
        ELSE b.subcategory
      END AS subcategory,
      b.detail_category,
    FROM
      get_transactions g
      LEFT JOIN budget_values b ON g.category_updated = b.category_raw
      AND g.subcategory_updated = b.subcategory_raw
      AND g.transaction_month = b.transaction_month
    WHERE
      g.category_updated != "EXCLUDE_CATEGORY"
  ),
  transactions_agg AS (
    SELECT
      transaction_month,
      category,
      subcategory,
      -- cast to string to join on budget_values
      IFNULL(detail_category, "null") AS detail_category,
      SUM(actual_amount) AS actual_amount,
      COUNT(DISTINCT transaction_id) AS transactions_count
    FROM
      add_transaction_categories
    GROUP BY
      1,
      2,
      3,
      4
  ),
  budget_values_agg AS (
    SELECT
      transaction_month,
      transaction_date,
      category,
      subcategory,
      IFNULL(detail_category, "null") AS detail_category, -- detail_category can be null, and need to join on this field
      SUM(budget_amount) AS budget_amount
    FROM
      budget_values
    GROUP BY
      1,
      2,
      3,
      4,
      5
  ),
  join_transactions_agg AS (
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
    FROM
      budget_values_agg
      LEFT JOIN transactions_agg USING (transaction_month, category, subcategory, detail_category)
    WHERE
      category NOT IN ("Transfer", "Exclude") -- non-important categories
      -- only include categories that have budget or actual spending
      -- commenting out so figures show up as $0 instead of blank in dashboard
      -- AND (budget_amount + IFNULL(actual_amount, 0)) != 0
  ),
  create_balance_category AS (
    SELECT
      transaction_date,
      transaction_month,
      "Balance" AS category,
      CAST(NULL AS STRING) AS subcategory,
      CAST(NULL AS STRING) AS detail_category,
      SUM(budget_amount) AS budget_amount,
      SUM(actual_amount) AS actual_amount,
      0 AS transactions_count
    FROM
      join_transactions_agg
    GROUP BY
      1,
      2
  ),
  final_transactions_agg AS (
    SELECT
      *
    FROM
      join_transactions_agg
    UNION ALL
    SELECT
      *
    FROM
      create_balance_category
  ),
  union_data AS (
    SELECT
      CURRENT_DATE() AS partition_date,
      "TRANSACTIONS_AGG" AS metric_category,
      CAST(NULL AS STRING) AS item_id,
      CAST(NULL AS STRING) AS account_id,
      CAST(NULL AS STRING) AS account_name,
      CAST(NULL AS STRING) AS account_subname,
      CAST(NULL AS STRING) AS account_type,
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
      CONCAT(category, IF(subcategory IS NULL, "", CONCAT(" - ", subcategory))) AS final_category,
      CONCAT(
        category,
        IF(subcategory IS NULL, "", CONCAT(" - ", subcategory)),
        IF(detail_category IS NULL, "", CONCAT(" - ", detail_category))
      ) AS full_category,
      CAST(NULL AS STRING) AS payment_channel,
      CAST(NULL AS STRING) AS merchant_name,
      CAST(NULL AS STRING) AS merchant_name_raw,
      CAST(NULL AS STRING) AS counterparty_name_raw,
      CAST(NULL AS STRING) AS name_raw,
      CAST(NULL AS STRING) AS merchant_type,
      CAST(NULL AS FLOAT64) AS institution_price,
      CAST(NULL AS FLOAT64) AS quantity,
      CAST(NULL AS FLOAT64) cost_basis,
      CAST(NULL AS STRING) AS security_type,
      CAST(NULL AS STRING) AS security_name,
      CAST(NULL AS STRING) AS ticker_symbol,
    FROM
      final_transactions_agg
    UNION ALL
    SELECT
      partition_date,
      "TRANSACTIONS" AS metric_category,
      item_id,
      account_id,
      account_name,
      account_subname,
      account_type,
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
      CONCAT(category, IF(subcategory IS NULL, "", CONCAT(" - ", subcategory))) AS final_category,
      CONCAT(
        category,
        IF(subcategory IS NULL, "", CONCAT(" - ", subcategory)),
        IF(detail_category IS NULL, "", CONCAT(" - ", detail_category))
      ) AS full_category,
      payment_channel,
      merchant_name,
      merchant_name_raw,
      counterparty_name_raw,
      name_raw,
      merchant_type,
      CAST(NULL AS FLOAT64) AS institution_price,
      CAST(NULL AS FLOAT64) AS quantity,
      CAST(NULL AS FLOAT64) cost_basis,
      CAST(NULL AS STRING) AS security_type,
      CAST(NULL AS STRING) AS security_name,
      CAST(NULL AS STRING) AS ticker_symbol,
    FROM
      add_transaction_categories
    UNION ALL
    SELECT
      partition_date,
      "ACCOUNTS" AS metric_category,
      item_id,
      account_id,
      account_name,
      account_subname,
      account_type,
      CAST(NULL AS STRING) AS transaction_id,
      transaction_date,
      DATE_TRUNC(transaction_date, MONTH) AS transaction_month,
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
      CAST(NULL AS STRING) AS security_type,
      CAST(NULL AS STRING) AS security_name,
      CAST(NULL AS STRING) AS ticker_symbol,
    FROM
      get_accounts
    UNION ALL
    SELECT
      partition_date,
      "INVESTMENTS" AS metric_category,
      item_id,
      account_id,
      account_name,
      account_subname,
      account_type,
      security_id AS transaction_id,
      partition_date AS transaction_date,
      DATE_TRUNC(partition_date, MONTH) AS transaction_month,
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
      security_name,
      ticker_symbol,
    FROM
      join_investments
  )
SELECT
  *
FROM
  union_data