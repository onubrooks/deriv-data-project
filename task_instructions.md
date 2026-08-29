# BUILD

Design and document a production-grade data engineering solution for a financial trading platform.

This assessment evaluates engineering judgment, edge-case awareness, and structural design quality — not syntax recall or tool memorisation. You are expected to use AI assistants, web search, and any scripting or query tool you choose. A live technical walkthrough is required after submission; you will be asked to defend your design choices and reasoning in your own words.

Your submission is a **GitHub repository**. Organise it clearly — an evaluator who cannot navigate your repository cannot score it.

---

## INPUT FILES

All files are in the `/data` folder. Your solution must be grounded in these specific files — generic answers not traceable to the provided data will not score.

### Core trading warehouse tables (four JSON files)

These represent the *target* tables your pipeline loads into.

| File | Description | Key relations |
|------|-------------|---------------|
| `client_signup.json` | One row per client: `client_id`, `signup_date`, `country`, `email`, `kyc_status`, `account_type`, `referral_source`, `signup_platform`, `promo_code`, `assigned_manager` | Root entity |
| `client_profile.json` | One row per client: `client_id`, `full_name`, `date_of_birth`, `nationality`, `risk_category`, `account_balance_usd`, `account_status`, `currency`, `last_login_date`, `preferred_language` | `client_id` → `client_signup.client_id` (1:1) |
| `client_deposit.json` | One row per deposit: `deposit_id`, `client_id`, `deposit_date`, `amount_usd`, `payment_method`, `currency_original`, `exchange_rate`, `status`, `processing_days`, `fee_usd` | `client_id` → `client_signup.client_id` (many:1) |
| `client_trades.json` | One row per trade: `trade_id`, `client_id`, `trade_date`, `instrument`, `direction`, `volume_lots`, `open_price`, `close_price`, `pnl_usd`, `trade_status` | `client_id` → `client_signup.client_id` (many:1) |

### New: vendor deposit feed (three CSV files)

A third-party payment processor delivers daily deposit extracts. These must be reconciled against the warehouse's `client_deposit` table.

```
deposits_vendor_20240301.csv  — columns: deposit_id, client_id, deposit_date, amount_usd,
                                          payment_method, currency_original, exchange_rate,
                                          status, processing_days, fee_usd
deposits_vendor_20240302.csv  — delivered next day; inspect columns carefully
deposits_vendor_20240303.csv  — delivered late; inspect dates carefully
```

### New: CDC change-log (one JSONL file)

A Change Data Capture stream from the operational database, one JSON object per line.

```
client_profile_changes.jsonl  — fields: lsn (log sequence number — a monotonically
                                         increasing integer that records the exact order
                                         of operations in the source database transaction
                                         log), commit_ts, op (insert/update/delete),
                                         client_id, before (null on insert), after (null on delete)
```

The file is delivered in **arrival order**, which is not guaranteed to match `lsn` order.

---

## DATA FILES

The complete contents of each input file are embedded below. Use these directly — no external `/data` folder is required.

### `client_signup.json`

```json
[
  {"client_id": "CL001", "signup_date": "2024-01-05", "country": "Malaysia", "email": "aisha.tan@email.com", "referral_source": "organic", "account_type": "standard", "kyc_status": "approved", "signup_platform": "web", "promo_code": null, "assigned_manager": "MGR01"},
  {"client_id": "CL002", "signup_date": "2024-01-08", "country": "Singapore", "email": "james.lim@email.com", "referral_source": "paid_search", "account_type": "professional", "kyc_status": "approved", "signup_platform": "web", "promo_code": "PROMO10", "assigned_manager": "MGR02"},
  {"client_id": "CL003", "signup_date": "2024-01-12", "country": "Thailand", "email": "somchai.k@email.com", "referral_source": "affiliate", "account_type": "standard", "kyc_status": "approved", "signup_platform": "mobile", "promo_code": null, "assigned_manager": "MGR01"},
  {"client_id": "CL004", "signup_date": "2024-01-15", "country": "Indonesia", "email": "budi.s@email.com", "referral_source": "social_media", "account_type": "standard", "kyc_status": "approved", "signup_platform": "mobile", "promo_code": "WELCOME", "assigned_manager": "MGR03"},
  {"client_id": "CL005", "signup_date": "2024-01-20", "country": "UAE", "email": "khalid.m@email.com", "referral_source": "referral", "account_type": "vip", "kyc_status": "approved", "signup_platform": "web", "promo_code": null, "assigned_manager": "MGR02"},
  {"client_id": "CL006", "signup_date": "2024-01-22", "country": "UK", "email": "sophie.w@email.com", "referral_source": "organic", "account_type": "professional", "kyc_status": "approved", "signup_platform": "web", "promo_code": null, "assigned_manager": "MGR04"},
  {"client_id": "CL007", "signup_date": "2024-03-15", "country": "Nigeria", "email": "emeka.o@email.com", "referral_source": "affiliate", "account_type": "standard", "kyc_status": "approved", "signup_platform": "mobile", "promo_code": null, "assigned_manager": "MGR01"},
  {"client_id": "CL008", "signup_date": "2024-02-01", "country": "Malaysia", "email": "raj.n@email.com", "referral_source": "paid_search", "account_type": "standard", "kyc_status": "approved", "signup_platform": "web", "promo_code": "PROMO10", "assigned_manager": "MGR03"},
  {"client_id": "CL009", "signup_date": "2024-02-05", "country": "Germany", "email": "anna.s@email.com", "referral_source": "organic", "account_type": "professional", "kyc_status": "approved", "signup_platform": "web", "promo_code": null, "assigned_manager": "MGR04"},
  {"client_id": "CL010", "signup_date": "2024-02-10", "country": "Brazil", "email": "lucas.f@email.com", "referral_source": "social_media", "account_type": "standard", "kyc_status": "approved", "signup_platform": "mobile", "promo_code": "WELCOME", "assigned_manager": "MGR01"},
  {"client_id": "CL011", "signup_date": "2024-02-14", "country": "India", "email": "priya.r@email.com", "referral_source": "referral", "account_type": "standard", "kyc_status": "approved", "signup_platform": "web", "promo_code": null, "assigned_manager": "MGR02"},
  {"client_id": "CL012", "signup_date": "2024-02-18", "country": "Singapore", "email": "david.t@email.com", "referral_source": "paid_search", "account_type": "standard", "kyc_status": "rejected", "signup_platform": "web", "promo_code": "PROMO10", "assigned_manager": "MGR03"},
  {"client_id": "CL013", "signup_date": "2024-02-22", "country": "Thailand", "email": "nara.p@email.com", "referral_source": "affiliate", "account_type": "standard", "kyc_status": "approved", "signup_platform": "mobile", "promo_code": null, "assigned_manager": "MGR01"},
  {"client_id": "CL014", "signup_date": "2024-03-01", "country": "Malaysia", "email": "farah.a@email.com", "referral_source": "organic", "account_type": "vip", "kyc_status": "approved", "signup_platform": "web", "promo_code": null, "assigned_manager": "MGR02"},
  {"client_id": "CL015", "signup_date": "2024-03-05", "country": "UAE", "email": "omar.h@email.com", "referral_source": "referral", "account_type": "professional", "kyc_status": "approved", "signup_platform": "web", "promo_code": null, "assigned_manager": "MGR04"},
  {"client_id": "CL016", "signup_date": "2024-03-10", "country": "Indonesia", "email": "dewi.k@email.com", "referral_source": "social_media", "account_type": "standard", "kyc_status": "approved", "signup_platform": "mobile", "promo_code": "WELCOME", "assigned_manager": "MGR03"},
  {"client_id": "CL017", "signup_date": "2024-03-18", "country": "UK", "email": "thomas.b@email.com", "referral_source": "paid_search", "account_type": "standard", "kyc_status": "approved", "signup_platform": "web", "promo_code": null, "assigned_manager": "MGR01"},
  {"client_id": "CL018", "signup_date": "2024-03-25", "country": "India", "email": "rahul.g@email.com", "referral_source": "affiliate", "account_type": "standard", "kyc_status": "approved", "signup_platform": "mobile", "promo_code": "PROMO10", "assigned_manager": "MGR02"},
  {"client_id": "CL019", "signup_date": "2024-04-02", "country": "Singapore", "email": "michelle.l@email.com", "referral_source": "referral", "account_type": "vip", "kyc_status": "approved", "signup_platform": "web", "promo_code": null, "assigned_manager": "MGR04"},
  {"client_id": "CL020", "signup_date": "2024-04-08", "country": "Germany", "email": "hans.m@email.com", "referral_source": "organic", "account_type": "professional", "kyc_status": "approved", "signup_platform": "web", "promo_code": null, "assigned_manager": "MGR03"},
  {"client_id": "CL021", "signup_date": "2024-04-15", "country": "Nigeria", "email": "amaka.e@email.com", "referral_source": "social_media", "account_type": "standard", "kyc_status": "approved", "signup_platform": "mobile", "promo_code": null, "assigned_manager": "MGR01"},
  {"client_id": "CL022", "signup_date": "2024-04-20", "country": "Malaysia", "email": "wei.c@email.com", "referral_source": "paid_search", "account_type": "standard", "kyc_status": "approved", "signup_platform": "web", "promo_code": "WELCOME", "assigned_manager": "MGR02"},
  {"client_id": "CL023", "signup_date": "2024-04-28", "country": "Brazil", "email": "ana.s@email.com", "referral_source": "affiliate", "account_type": "standard", "kyc_status": "approved", "signup_platform": "mobile", "promo_code": null, "assigned_manager": "MGR04"},
  {"client_id": "CL024", "signup_date": "2024-05-03", "country": "Thailand", "email": "krit.w@email.com", "referral_source": "organic", "account_type": "standard", "kyc_status": "approved", "signup_platform": "web", "promo_code": null, "assigned_manager": "MGR03"},
  {"client_id": "CL025", "signup_date": "2024-05-10", "country": "UAE", "email": "sara.k@email.com", "referral_source": "referral", "account_type": "vip", "kyc_status": "approved", "signup_platform": "web", "promo_code": null, "assigned_manager": "MGR02"},
  {"client_id": "CL026", "signup_date": "2024-05-15", "country": "Indonesia", "email": "andi.p@email.com", "referral_source": "social_media", "account_type": "standard", "kyc_status": "pending", "signup_platform": "mobile", "promo_code": null, "assigned_manager": "MGR01"},
  {"client_id": "CL027", "signup_date": "2024-05-20", "country": "UK", "email": "emily.j@email.com", "referral_source": "paid_search", "account_type": "professional", "kyc_status": "approved", "signup_platform": "web", "promo_code": "PROMO10", "assigned_manager": "MGR04"},
  {"client_id": "CL028", "signup_date": "2024-05-28", "country": "India", "email": "vikram.s@email.com", "referral_source": "affiliate", "account_type": "standard", "kyc_status": "approved", "signup_platform": "mobile", "promo_code": null, "assigned_manager": "MGR03"},
  {"client_id": "CL029", "signup_date": "2024-06-05", "country": "Singapore", "email": "jason.k@email.com", "referral_source": "organic", "account_type": "standard", "kyc_status": "approved", "signup_platform": "web", "promo_code": null, "assigned_manager": "MGR02"},
  {"client_id": "CL030", "signup_date": "2024-06-12", "country": "Malaysia", "email": "nurul.h@email.com", "referral_source": "referral", "account_type": "standard", "kyc_status": "approved", "signup_platform": "web", "promo_code": "WELCOME", "assigned_manager": "MGR01"}
]
```

### `client_profile.json`

```json
[
  {"client_id": "CL001", "full_name": "Aisha Tan", "date_of_birth": "1990-04-12", "nationality": "Malaysian", "risk_category": "medium", "account_balance_usd": 1250.00, "account_status": "active", "currency": "USD", "last_login_date": "2024-11-20", "preferred_language": "English"},
  {"client_id": "CL002", "full_name": "James Lim", "date_of_birth": "1985-07-23", "nationality": "Singaporean", "risk_category": "high", "account_balance_usd": 8500.00, "account_status": "active", "currency": "USD", "last_login_date": "2024-11-18", "preferred_language": "English"},
  {"client_id": "CL003", "full_name": "Somchai Krung", "date_of_birth": "1992-11-05", "nationality": "Thai", "risk_category": "low", "account_balance_usd": 320.00, "account_status": "active", "currency": "THB", "last_login_date": "2024-11-15", "preferred_language": "Thai"},
  {"client_id": "CL004", "full_name": "Budi Santoso", "date_of_birth": "1988-03-30", "nationality": "Indonesian", "risk_category": "low", "account_balance_usd": 450.00, "account_status": "active", "currency": "IDR", "last_login_date": "2024-11-10", "preferred_language": "Indonesian"},
  {"client_id": "CL005", "full_name": "Khalid Mansour", "date_of_birth": "1979-08-17", "nationality": "Emirati", "risk_category": "high", "account_balance_usd": 12000.00, "account_status": "active", "currency": "USD", "last_login_date": "2024-11-22", "preferred_language": "Arabic"},
  {"client_id": "CL006", "full_name": "Sophie Wright", "date_of_birth": "1994-02-14", "nationality": "British", "risk_category": "medium", "account_balance_usd": 3200.00, "account_status": "active", "currency": "GBP", "last_login_date": "2024-11-19", "preferred_language": "English"},
  {"client_id": "CL007", "full_name": "Emeka Okafor", "date_of_birth": "1991-06-28", "nationality": "Nigerian", "risk_category": "low", "account_balance_usd": 180.00, "account_status": "active", "currency": "USD", "last_login_date": "2024-11-05", "preferred_language": "English"},
  {"client_id": "CL008", "full_name": "Raj Nair", "date_of_birth": "1987-09-11", "nationality": "Malaysian", "risk_category": "medium", "account_balance_usd": 950.00, "account_status": "inactive", "currency": "USD", "last_login_date": "2024-03-01", "preferred_language": "English"},
  {"client_id": "CL009", "full_name": "Anna Schmidt", "date_of_birth": "1993-12-03", "nationality": "German", "risk_category": "high", "account_balance_usd": 6700.00, "account_status": "active", "currency": "EUR", "last_login_date": "2024-11-21", "preferred_language": "German"},
  {"client_id": "CL010", "full_name": "Lucas Ferreira", "date_of_birth": "1996-05-19", "nationality": "Brazilian", "risk_category": "low", "account_balance_usd": 280.00, "account_status": "active", "currency": "BRL", "last_login_date": "2024-11-08", "preferred_language": "Portuguese"},
  {"client_id": "CL011", "full_name": "Priya Rao", "date_of_birth": "1990-01-25", "nationality": "Indian", "risk_category": "medium", "account_balance_usd": 1100.00, "account_status": "active", "currency": "USD", "last_login_date": "2024-11-17", "preferred_language": "English"},
  {"client_id": "CL012", "full_name": "David Tan", "date_of_birth": "1984-10-07", "nationality": "Singaporean", "risk_category": "low", "account_balance_usd": 0.00, "account_status": "suspended", "currency": "USD", "last_login_date": "2024-11-01", "preferred_language": "English"},
  {"client_id": "CL013", "full_name": "Nara Patel", "date_of_birth": "1995-07-14", "nationality": "Thai", "risk_category": "low", "account_balance_usd": 390.00, "account_status": "active", "currency": "THB", "last_login_date": "2024-11-12", "preferred_language": "Thai"},
  {"client_id": "CL014", "full_name": "Farah Ahmad", "date_of_birth": "1989-04-22", "nationality": "Malaysian", "risk_category": "high", "account_balance_usd": 9800.00, "account_status": "active", "currency": "USD", "last_login_date": "2024-11-23", "preferred_language": "Malay"},
  {"client_id": "CL015", "full_name": "Omar Hassan", "date_of_birth": "1982-11-30", "nationality": "Emirati", "risk_category": "high", "account_balance_usd": 15500.00, "account_status": "active", "currency": "USD", "last_login_date": "2024-11-20", "preferred_language": "Arabic"},
  {"client_id": "CL016", "full_name": "Dewi Kartika", "date_of_birth": "1997-03-08", "nationality": "Indonesian", "risk_category": "low", "account_balance_usd": 210.00, "account_status": "active", "currency": "IDR", "last_login_date": "2024-11-09", "preferred_language": "Indonesian"},
  {"client_id": "CL017", "full_name": "Thomas Brown", "date_of_birth": "1991-08-16", "nationality": "British", "risk_category": "medium", "account_balance_usd": 2100.00, "account_status": "active", "currency": "GBP", "last_login_date": "2024-11-14", "preferred_language": "English"},
  {"client_id": "CL018", "full_name": "Rahul Gupta", "date_of_birth": "1994-01-27", "nationality": "Indian", "risk_category": "medium", "account_balance_usd": 870.00, "account_status": "active", "currency": "USD", "last_login_date": "2024-11-11", "preferred_language": "English"},
  {"client_id": "CL019", "full_name": "Michelle Lee", "date_of_birth": "1986-05-09", "nationality": "Singaporean", "risk_category": "high", "account_balance_usd": 78500.00, "account_status": "active", "currency": "USD", "last_login_date": "2024-11-22", "preferred_language": "English"},
  {"client_id": "CL020", "full_name": "Hans Mueller", "date_of_birth": "1980-09-21", "nationality": "German", "risk_category": "high", "account_balance_usd": 11200.00, "account_status": "active", "currency": "EUR", "last_login_date": "2024-11-18", "preferred_language": "German"},
  {"client_id": "CL021", "full_name": "Amaka Eze", "date_of_birth": "1998-02-13", "nationality": "Nigerian", "risk_category": "low", "account_balance_usd": 150.00, "account_status": "active", "currency": "USD", "last_login_date": "2024-11-07", "preferred_language": "English"},
  {"client_id": "CL022", "full_name": "Wei Chen", "date_of_birth": "1993-06-04", "nationality": "Malaysian", "risk_category": "medium", "account_balance_usd": 1650.00, "account_status": "active", "currency": "USD", "last_login_date": "2024-11-16", "preferred_language": "Chinese"},
  {"client_id": "CL023", "full_name": "Ana Silva", "date_of_birth": "1996-11-28", "nationality": "Brazilian", "risk_category": "low", "account_balance_usd": 310.00, "account_status": "active", "currency": "BRL", "last_login_date": "2024-11-06", "preferred_language": "Portuguese"},
  {"client_id": "CL024", "full_name": "Krit Wongsa", "date_of_birth": "1990-08-15", "nationality": "Thai", "risk_category": "low", "account_balance_usd": 420.00, "account_status": "active", "currency": "THB", "last_login_date": "2024-11-13", "preferred_language": "Thai"},
  {"client_id": "CL025", "full_name": "Sara Khalid", "date_of_birth": "1888-12-19", "nationality": "Emirati", "risk_category": "high", "account_balance_usd": 18200.00, "account_status": "active", "currency": "USD", "last_login_date": "2024-05-08", "preferred_language": "Arabic"},
  {"client_id": "CL026", "full_name": "Andi Purnama", "date_of_birth": "1999-04-02", "nationality": "Indonesian", "risk_category": "low", "account_balance_usd": 0.00, "account_status": "pending", "currency": "IDR", "last_login_date": null, "preferred_language": "Indonesian"},
  {"client_id": "CL027", "full_name": "Emily Jones", "date_of_birth": "1992-07-11", "nationality": "British", "risk_category": "medium", "account_balance_usd": 4300.00, "account_status": "active", "currency": "GBP", "last_login_date": "2024-11-20", "preferred_language": "English"},
  {"client_id": "CL028", "full_name": "Vikram Singh", "date_of_birth": "1985-10-24", "nationality": "Indian", "risk_category": "medium", "account_balance_usd": 760.00, "account_status": "active", "currency": "USD", "last_login_date": "2024-11-15", "preferred_language": "Hindi"},
  {"client_id": "CL029", "full_name": "Jason Koh", "date_of_birth": "1994-03-17", "nationality": "Singaporean", "risk_category": "low", "account_balance_usd": 580.00, "account_status": "active", "currency": "USD", "last_login_date": "2024-11-10", "preferred_language": "English"},
  {"client_id": "CL030", "full_name": "Nurul Huda", "date_of_birth": "1991-09-06", "nationality": "Malaysian", "risk_category": "medium", "account_balance_usd": 1420.00, "account_status": "active", "currency": "USD", "last_login_date": "2024-11-19", "preferred_language": "Malay"}
]
```

### `client_deposit.json`

```json
[
  {"deposit_id": "DEP001", "client_id": "CL001", "deposit_date": "2024-01-10", "amount_usd": 500.00, "payment_method": "bank_transfer", "currency_original": "MYR", "exchange_rate": 4.71, "status": "completed", "processing_days": 2, "fee_usd": 5.00},
  {"deposit_id": "DEP002", "client_id": "CL002", "deposit_date": "2024-01-15", "amount_usd": 2000.00, "payment_method": "credit_card", "currency_original": "SGD", "exchange_rate": 1.34, "status": "completed", "processing_days": 1, "fee_usd": 20.00},
  {"deposit_id": "DEP003", "client_id": "CL005", "deposit_date": "2024-01-25", "amount_usd": 5000.00, "payment_method": "bank_transfer", "currency_original": "USD", "exchange_rate": 1.00, "status": "completed", "processing_days": 3, "fee_usd": 0.00},
  {"deposit_id": "DEP004", "client_id": "CL006", "deposit_date": "2024-02-03", "amount_usd": 1500.00, "payment_method": "credit_card", "currency_original": "GBP", "exchange_rate": 0.79, "status": "completed", "processing_days": 1, "fee_usd": 15.00},
  {"deposit_id": "DEP005", "client_id": "CL008", "deposit_date": "2024-02-08", "amount_usd": 800.00, "payment_method": "bank_transfer", "currency_original": "MYR", "exchange_rate": 4.71, "status": "completed", "processing_days": 2, "fee_usd": 8.00},
  {"deposit_id": "DEP006", "client_id": "CL009", "deposit_date": "2024-02-12", "amount_usd": 3000.00, "payment_method": "bank_transfer", "currency_original": "EUR", "exchange_rate": 0.92, "status": "completed", "processing_days": 3, "fee_usd": 0.00},
  {"deposit_id": "DEP007", "client_id": "CL011", "deposit_date": "2024-02-20", "amount_usd": 600.00, "payment_method": "e_wallet", "currency_original": "USD", "exchange_rate": 1.00, "status": "completed", "processing_days": 1, "fee_usd": 6.00},
  {"deposit_id": "DEP008", "client_id": "CL012", "deposit_date": "2024-02-25", "amount_usd": 350.00, "payment_method": "credit_card", "currency_original": "SGD", "exchange_rate": 1.34, "status": "completed", "processing_days": 1, "fee_usd": 3.50},
  {"deposit_id": "DEP009", "client_id": "CL013", "deposit_date": "2024-03-05", "amount_usd": 250.00, "payment_method": "e_wallet", "currency_original": "THB", "exchange_rate": 35.20, "status": "completed", "processing_days": 1, "fee_usd": 2.50},
  {"deposit_id": "DEP010", "client_id": "CL014", "deposit_date": "2024-03-10", "amount_usd": 4500.00, "payment_method": "bank_transfer", "currency_original": "MYR", "exchange_rate": 4.71, "status": "completed", "processing_days": 2, "fee_usd": 0.00},
  {"deposit_id": "DEP011", "client_id": "CL015", "deposit_date": "2024-03-15", "amount_usd": 8000.00, "payment_method": "bank_transfer", "currency_original": "USD", "exchange_rate": 1.00, "status": "completed", "processing_days": 3, "fee_usd": 0.00},
  {"deposit_id": "DEP012", "client_id": "CL017", "deposit_date": "2024-03-28", "amount_usd": 900.00, "credit_card": "credit_card", "currency_original": "GBP", "exchange_rate": 0.79, "status": "completed", "processing_days": 1, "fee_usd": 9.00},
  {"deposit_id": "DEP013", "client_id": "CL019", "deposit_date": "2024-04-05", "amount_usd": 75000.00, "payment_method": "bank_transfer", "currency_original": "USD", "exchange_rate": 1.00, "status": "completed", "processing_days": 5, "fee_usd": 0.00},
  {"deposit_id": "DEP014", "client_id": "CL020", "deposit_date": "2024-04-12", "amount_usd": 5500.00, "payment_method": "bank_transfer", "currency_original": "EUR", "exchange_rate": 0.92, "status": "completed", "processing_days": 3, "fee_usd": 0.00},
  {"deposit_id": "DEP015", "client_id": "CL022", "deposit_date": "2024-04-25", "amount_usd": 700.00, "payment_method": "e_wallet", "currency_original": "MYR", "exchange_rate": 4.71, "status": "completed", "processing_days": 1, "fee_usd": 7.00},
  {"deposit_id": "DEP016", "client_id": "CL024", "deposit_date": "2024-05-08", "amount_usd": 300.00, "payment_method": "e_wallet", "currency_original": "THB", "exchange_rate": 35.20, "status": "completed", "processing_days": 1, "fee_usd": 3.00},
  {"deposit_id": "DEP017", "client_id": "CL025", "deposit_date": "2024-05-12", "amount_usd": 10000.00, "payment_method": "bank_transfer", "currency_original": "USD", "exchange_rate": 1.00, "status": "completed", "processing_days": 2, "fee_usd": 0.00},
  {"deposit_id": "DEP018", "client_id": "CL027", "deposit_date": "2024-05-25", "amount_usd": 2500.00, "payment_method": "credit_card", "currency_original": "GBP", "exchange_rate": 0.79, "status": "completed", "processing_days": 1, "fee_usd": 25.00},
  {"deposit_id": "DEP019", "client_id": "CL029", "deposit_date": "2024-06-15", "amount_usd": 400.00, "payment_method": "e_wallet", "currency_original": "SGD", "exchange_rate": 1.34, "status": "completed", "processing_days": 1, "fee_usd": 4.00},
  {"deposit_id": "DEP020", "client_id": "CL031", "deposit_date": "2024-07-01", "amount_usd": 1200.00, "payment_method": "bank_transfer", "currency_original": "USD", "exchange_rate": 1.00, "status": "completed", "processing_days": 2, "fee_usd": 12.00}
]
```

### `client_trades.json`

```json
[
  {"trade_id": "TRD001", "client_id": "CL001", "trade_date": "2024-01-15", "instrument": "EUR/USD", "direction": "buy", "volume_lots": 0.5, "open_price": 1.0920, "close_price": 1.0985, "pnl_usd": 32.50, "trade_status": "closed"},
  {"trade_id": "TRD002", "client_id": "CL002", "trade_date": "2024-01-20", "instrument": "Gold", "direction": "sell", "volume_lots": 1.0, "open_price": 2025.50, "close_price": 2018.30, "pnl_usd": 72.00, "trade_status": "closed"},
  {"trade_id": "TRD003", "client_id": "CL005", "trade_date": "2024-02-01", "instrument": "BTC/USD", "direction": "buy", "volume_lots": 0.2, "open_price": 42500.00, "close_price": 44200.00, "pnl_usd": 340.00, "trade_status": "closed"},
  {"trade_id": "TRD004", "client_id": "CL006", "trade_date": "2024-02-08", "instrument": "EUR/USD", "direction": "sell", "volume_lots": 0.5, "open_price": 1.0850, "close_price": 1.0810, "pnl_usd": 20.00, "trade_status": "closed"},
  {"trade_id": "TRD005", "client_id": "CL007", "trade_date": "2024-02-20", "instrument": "USD/JPY", "direction": "buy", "volume_lots": 1.0, "open_price": 149.50, "close_price": 150.20, "pnl_usd": 47.00, "trade_status": "closed"},
  {"trade_id": "TRD006", "client_id": "CL008", "trade_date": "2024-11-05", "instrument": "Gold", "direction": "buy", "volume_lots": 0.5, "open_price": 2680.00, "close_price": 2695.00, "pnl_usd": 75.00, "trade_status": "closed"},
  {"trade_id": "TRD007", "client_id": "CL009", "trade_date": "2024-02-18", "instrument": "S&P500", "direction": "buy", "volume_lots": 2.0, "open_price": 5050.00, "close_price": 5120.00, "pnl_usd": 140.00, "trade_status": "closed"},
  {"trade_id": "TRD008", "client_id": "CL011", "trade_date": "2024-03-01", "instrument": "EUR/USD", "direction": "buy", "volume_lots": 1.0, "open_price": 1.0780, "close_price": 1.0820, "pnl_usd": 40.00, "trade_status": "closed"},
  {"trade_id": "TRD009", "client_id": "CL014", "trade_date": "2024-03-15", "instrument": "Gold", "direction": "sell", "volume_lots": 1.5, "open_price": 2180.00, "close_price": 2165.00, "pnl_usd": 225.00, "trade_status": "closed"},
  {"trade_id": "TRD010", "client_id": "CL015", "trade_date": "2024-03-20", "instrument": "BTC/USD", "direction": "buy", "volume_lots": 0.5, "open_price": 68000.00, "close_price": 71500.00, "pnl_usd": 1750.00, "trade_status": "closed"},
  {"trade_id": "TRD011", "client_id": "CL017", "trade_date": "2024-04-02", "instrument": "USD/JPY", "direction": "sell", "volume_lots": 1.0, "open_price": 151.80, "close_price": 151.20, "pnl_usd": 40.00, "trade_status": "closed"},
  {"trade_id": "TRD012", "client_id": "CL019", "trade_date": "2024-04-10", "instrument": "Gold", "direction": "buy", "volume_lots": 5.0, "open_price": 2320.00, "close_price": 2320.00, "pnl_usd": 245.00, "trade_status": "closed"},
  {"trade_id": "TRD013", "client_id": "CL020", "trade_date": "2024-04-18", "instrument": "S&P500", "direction": "sell", "volume_lots": 2.0, "open_price": 5200.00, "close_price": 5150.00, "pnl_usd": 100.00, "trade_status": "closed"},
  {"trade_id": "TRD014", "client_id": "CL022", "trade_date": "2024-05-02", "instrument": "EUR/USD", "direction": "buy", "volume_lots": 0.5, "open_price": 1.0720, "close_price": 1.0760, "pnl_usd": 20.00, "trade_status": "closed"},
  {"trade_id": "TRD015", "client_id": "CL024", "trade_date": "2024-05-12", "instrument": "USD/JPY", "direction": "sell", "volume_lots": 1.0, "open_price": 155.30, "close_price": 154.80, "pnl_usd": 33.50, "trade_status": "closed"},
  {"trade_id": "TRD016", "client_id": "CL025", "trade_date": "2024-05-15", "instrument": "Gold", "direction": "buy", "volume_lots": 2.0, "open_price": 2340.00, "close_price": 2380.00, "pnl_usd": 800.00, "trade_status": "closed"},
  {"trade_id": "TRD017", "client_id": "CL027", "trade_date": "2024-06-01", "instrument": "BTC/USD", "direction": "buy", "volume_lots": 0.3, "open_price": 67500.00, "close_price": 69200.00, "pnl_usd": 510.00, "trade_status": "closed"},
  {"trade_id": "TRD018", "client_id": "CL029", "trade_date": "2024-06-20", "instrument": "EUR/USD", "direction": "sell", "volume_lots": 1.0, "open_price": 1.0850, "close_price": 1.0815, "pnl_usd": 35.00, "trade_status": "closed"},
  {"trade_id": "TRD019", "client_id": "CL002", "trade_date": "2024-07-05", "instrument": "Gold", "direction": "buy", "volume_lots": 2.0, "open_price": 2380.00, "close_price": 2410.00, "pnl_usd": 600.00, "trade_status": "closed"},
  {"trade_id": "TRD020", "client_id": "CL009", "trade_date": "2024-08-10", "instrument": "S&P500", "direction": "buy", "volume_lots": 3.0, "open_price": 5450.00, "close_price": 5520.00, "pnl_usd": 210.00, "trade_status": "closed"}
]
```

### `deposits_vendor_20240301.csv`

```csv
deposit_id,client_id,deposit_date,amount_usd,payment_method,currency_original,exchange_rate,status,processing_days,fee_usd
VDEP001,CL003,2024-03-01,-250.00,bank_transfer,THB,35.20,completed,2,0.00
VDEP002,CL001,2024-03-01,1500.00,credit_card,USD,1.00,completed,1,15.00
VDEP003,CL007,2024-03-01,320.00,e_wallet,MYR,4.72,completed,1,3.20
VDEP004,CL012,2024-03-01,2200.00,bank_transfer,SGD,1.34,completed,2,0.00
VDEP005,CL005,2024-03-01,875.00,credit_card,GBP,0.79,completed,1,8.75
VDEP006,CL018,2024-03-01,500.00,e_wallet,IDR,15750.00,pending,3,5.00
VDEP007,CL009,2024-03-01,4200.00,bank_transfer,EUR,0.92,completed,2,0.00
VDEP008,CL022,2024-03-01,150.00,credit_card,MYR,4.72,completed,1,2.50
VDEP009,CL026,2024-03-01,3000.00,bank_transfer,SGD,1.34,pending,2,0.00
```

### `deposits_vendor_20240302.csv`

> **Note:** column `payment_method` is renamed `method` in this file — a schema drift to handle.

```csv
deposit_id,client_id,deposit_date,amount_usd,method,currency_original,exchange_rate,status,processing_days,fee_usd
VDEP002,CL001,2024-03-01,1500.00,credit_card,USD,1.00,completed,1,15.00
VDEP005,CL005,2024-03-01,875.00,credit_card,GBP,0.79,completed,1,8.75
VDEP010,CL010,2024-03-02,600.00,e_wallet,MYR,4.72,completed,1,6.00
VDEP011,CL013,2024-03-02,1800.00,bank_transfer,SGD,1.34,completed,2,0.00
VDEP012,CL015,2024-03-02,250.00,credit_card,USD,1.00,completed,1,4.00
VDEP013,CL017,2024-03-02,3500.00,bank_transfer,EUR,0.92,completed,2,0.00
VDEP014,CL020,2024-03-02,420.00,e_wallet,IDR,15750.00,pending,3,4.20
VDEP015,CL023,2024-03-02,990.00,credit_card,GBP,0.79,completed,1,9.90
VDEP016,CL025,2024-03-02,2750.00,bank_transfer,THB,35.20,completed,2,0.00
```

### `deposits_vendor_20240303.csv`

> **Note:** delivered late; all deposit dates predate the filename date — a late-delivery scenario to handle.

```csv
deposit_id,client_id,deposit_date,amount_usd,payment_method,currency_original,exchange_rate,status,processing_days,fee_usd
VDEP017,CL015,2024-02-26,1100.00,bank_transfer,MYR,4.72,completed,2,0.00
VDEP018,CL019,2024-02-28,670.00,e_wallet,SGD,1.34,completed,1,6.70
VDEP019,CL022,2024-02-25,2400.00,bank_transfer,EUR,0.92,completed,2,0.00
VDEP020,CL099,2024-02-27,800.00,bank_transfer,USD,1.00,completed,3,0.00
VDEP021,CL028,2024-02-27,350.00,credit_card,GBP,0.79,completed,1,5.00
VDEP022,CL016,2024-02-24,950.00,bank_transfer,THB,35.20,pending,2,0.00
```

### `client_profile_changes.jsonl`

> **Note:** records are in arrival order, **not** LSN order — see `lsn` values.

```jsonl
{"lsn": 1005, "commit_ts": "2024-11-15T11:00:00Z", "op": "update", "client_id": "CL001", "before": {"risk_category": "high", "account_balance_usd": 1250.00, "account_status": "active"}, "after": {"risk_category": "high", "account_balance_usd": 1850.00, "account_status": "active"}}
{"lsn": 1009, "commit_ts": "2024-11-21T10:00:00Z", "op": "update", "client_id": "CL014", "before": {"risk_category": "high", "account_balance_usd": 12300.00, "account_status": "active"}, "after": {"risk_category": "medium", "account_balance_usd": 12300.00, "account_status": "active"}}
{"lsn": 1001, "commit_ts": "2024-10-01T08:00:00Z", "op": "insert", "client_id": "CL030", "before": null, "after": {"full_name": "Nurul Huda", "date_of_birth": "1991-09-06", "nationality": "Malaysian", "risk_category": "medium", "account_balance_usd": 1420.00, "account_status": "active", "currency": "USD", "preferred_language": "Malay"}}
{"lsn": 1004, "commit_ts": "2024-11-15T10:30:00Z", "op": "update", "client_id": "CL001", "before": {"risk_category": "medium", "account_balance_usd": 1250.00, "account_status": "active"}, "after": {"risk_category": "high", "account_balance_usd": 1250.00, "account_status": "active"}}
{"lsn": 1010, "commit_ts": "2024-11-21T14:00:00Z", "op": "delete", "client_id": "CL012", "before": {"full_name": "David Tan", "risk_category": "low", "account_balance_usd": 0.00, "account_status": "suspended"}, "after": null}
{"lsn": 1012, "commit_ts": "2024-11-22T08:00:00Z", "op": "update", "client_id": "CL002", "before": {"risk_category": "high", "account_balance_usd": 8500.00, "account_status": "active"}, "after": {"risk_category": "high", "account_balance_usd": 9200.00, "account_status": "active"}}
{"lsn": 1003, "commit_ts": "2024-11-14T09:00:00Z", "op": "update", "client_id": "CL009", "before": {"risk_category": "high", "account_balance_usd": 6700.00, "account_status": "active"}, "after": {"risk_category": "medium", "account_balance_usd": 6700.00, "account_status": "active"}}
{"lsn": 1015, "commit_ts": "2024-11-22T15:00:00Z", "op": "update", "client_id": "CL019", "before": {"risk_category": "high", "account_balance_usd": 78500.00, "account_status": "active"}, "after": {"risk_category": "high", "account_balance_usd": 80000.00, "account_status": "active"}}
{"lsn": 1008, "commit_ts": "2024-11-20T09:00:00Z", "op": "update", "client_id": "CL014", "before": {"risk_category": "high", "account_balance_usd": 9800.00, "account_status": "active"}, "after": {"risk_category": "high", "account_balance_usd": 12300.00, "account_status": "active"}}
{"lsn": 1018, "commit_ts": "2024-11-23T09:00:00Z", "op": "update", "client_id": "CL022", "before": {"risk_category": "medium", "account_balance_usd": 1650.00, "account_status": "active"}, "after": {"risk_category": "medium", "account_balance_usd": 2100.00, "account_status": "active"}}
{"lsn": 1006, "commit_ts": "2024-11-15T14:00:00Z", "op": "update", "client_id": "CL001", "before": {"risk_category": "high", "account_balance_usd": 1850.00, "account_status": "active"}, "after": {"risk_category": "high", "account_balance_usd": 1850.00, "account_status": "under_review"}}
{"lsn": 1020, "commit_ts": "2024-11-23T16:00:00Z", "op": "update", "client_id": "CL025", "before": {"risk_category": "high", "account_balance_usd": 18200.00, "account_status": "active"}, "after": {"risk_category": "medium", "account_balance_usd": 18200.00, "account_status": "active"}}
```

---

## MUST COMPLETE

---

### Part 1 — Pipeline Design & Reconciliation

*Ingest the vendor CSV feed and the CDC change-log into the trading warehouse.*

**1a. Pipeline design document**

Write a concise design document covering:

1. **Architecture overview** — source-to-target flow for both the vendor CSV feed and the CDC log. Name the layers (e.g. landing, staging, target) and what happens at each.
2. **Idempotency strategy** — how does the pipeline ensure that re-running it does not create duplicate records? Be specific about the mechanism (e.g. file manifest, hash, watermark, merge key).
3. **Late and missing data** — the vendor files do not always arrive on schedule and may contain records dated earlier than you expect. How does the pipeline detect and self-reconcile missing or late data without manual intervention?
4. **Source-delete handling** — the CDC log includes delete events. How do you represent a deleted source record in the target warehouse? What are the trade-offs of your approach?
5. **Edge cases** — list 2–5 specific edge cases your design explicitly handles (name the edge case and your handling strategy for each). You may include data quality safeguards as part of your design where relevant — this is not a required deliverable, but a well-reasoned design that anticipates data quality risk is a positive signal.

---

### Part 2 — Data Model & Historization

*Model the trading warehouse; support point-in-time history and analytics.*

**2a. Dimensional model / ERD**

Design a dimensional model for the trading warehouse. Your design should address:

- The **facts and dimensions** you would create, with grain stated for each fact table.
- Whether you would use a **Kimball star schema, Data Vault, or another approach** — and why that choice fits this dataset.
- How the model handles **late-arriving dimension records** (e.g. a client's first deposit or trade record arrives before their dimension row has been loaded into the warehouse).

Represent the model as an ERD diagram or as a clearly labelled text schema (table name, key columns, relationships). You do not need to include all 30 columns from the raw files.

**2b. Historization (SCD)**

The `client_profile_changes.jsonl` CDC feed delivers changes to `risk_category`, `account_balance_usd`, and `account_status`.

Answer the following, with justification for every choice:

1. Which SCD type would you apply to these client attributes, and why? What are the trade-offs?
2. How does your pipeline handle **update and delete events**? Walk through the merge/upsert logic and what happens in the warehouse when a delete arrives.
3. You need to **reload data for a specific historical date range** (e.g. re-process November 2024). How do you do this without corrupting the existing history?

---

### Part 3 — PII Handling

The input files contain PII fields: `email`, `date_of_birth`, and `full_name`. In two or three sentences, describe how you would handle these fields in your pipeline: at what layer would you apply masking or transformation, and what access control would you put on the unmasked data? Submit your response in `part3_pii.md`.

> Example of the expected shape (not a model answer to copy): *"Apply column-level encryption to email/DOB/full_name at the staging-to-curated transform; grant unmasked access only to Compliance via row-level security, with Analytics and BI limited to the masked view."*

---

## VALIDATION REQUIREMENTS

Before submitting, verify your document satisfies the following:

- Part 1a names 2–5 specific edge cases with a handling strategy for each.
- Part 3 describes a masking or transformation layer and names the access control on unmasked data.
- A `PROMPTS.md` file is present listing the AI prompts used for each part. Each entry must include the part it was used for, the actual prompt text (or a close paraphrase), and what you then changed or decided based on the output. A one-line entry such as "I used Claude for Part 1" does not meet this requirement. Where it happened, note it if you disagreed with, corrected, or rejected something the AI suggested. Example of an acceptable entry:

  ```
  ## Part 2b — Historization / SCD
  Prompt: "How do I write a MERGE statement in BigQuery to implement SCD Type 2 for a
  slowly-changing dimension table"
  ```

---

## DELIVERABLE FORMAT

Submit a **GitHub repository** with the following structure:

```
README.md                      — brief overview and how to navigate the repo
part1_pipeline.md              — Pipeline Design & Reconciliation
part2_data_model.md            — Data Model & Historization
part3_pii.md                   — PII Handling
sql/                           — any SQL files referenced from part2
code/                          — any prototype scripts (optional; a runnable, idempotent prototype is a strong positive signal)
PROMPTS.md                     — all AI prompts used, grouped by part
```

Each document should be self-contained: include your diagrams (Mermaid, ASCII, or image links), SQL, and explanations inline. Code or SQL submitted without accompanying explanation will not score.

---

## TOOLS

You may use any software, programming language, or platform, including:

- Large language models (ChatGPT, Claude, Gemini, etc.)
- SQL environments (DuckDB, PostgreSQL, BigQuery, SQLite, etc.)
- Scripting languages (Python, SQL, R, etc.)
- Diagramming tools (draw.io, Mermaid, dbdiagram.io, etc.)

---

## TECHNICAL CONSTRAINTS

- **Part 1a — Data quality is optional but rewarded:** If you include data quality safeguards in your pipeline design, differentiate severity and be specific about the on-failure action — a check with no severity or a vague action ("log and continue" for everything) does not add credit.
- **Part 2b — Delete handling must not destroy history:** A delete event must be represented as a soft-delete or end-dated SCD row with an audit trail — a hard delete from the warehouse is not an acceptable answer regardless of the rest of the design.
