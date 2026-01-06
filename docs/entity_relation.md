erDiagram
    Chain {
        int chain_id PK
        varchar name UK
    }

    Token {
        bigint id PK
        int chain_id FK "UK1"
        bytea address "UK1"
        varchar symbol
        smallint decimals
        bigint created_at_block
        timestamptz updated_at
        jsonb metadata "protocol-specific fields"
    }

    Protocol {
        bigint id PK
        int chain_id FK "UK1"
        bytea address "UK1"
        varchar name
        varchar protocol_type "lending,rwa"
        bigint created_at_block
        timestamptz updated_at
        jsonb metadata "protocol-specific config"
    }

    ReceiptTokens {
        bigint id PK
        bigint protocol_id FK "UK1"
        bigint underlying_token_id FK "UK1"
        bytea receipt_token_address
        varchar symbol "aWETH, spWETH, cWETH"
        bigint created_at_block
        timestamptz updated_at
        jsonb metadata "protocol-specific fields"
    }

    DebtTokens {
        bigint id PK
        bigint protocol_id FK "UK1"
        bigint underlying_token_id FK "UK1"
        bytea variable_debt_address
        bytea stable_debt_address "nullable, not all protocols"
        varchar variable_symbol "variableDebtWETH"
        varchar stable_symbol "stableDebtWETH, nullable"
        bigint created_at_block
        timestamptz updated_at
        jsonb metadata "protocol-specific fields"
    }

    Users {
        bigint id PK
        int chain_id FK "UK1"
        bytea address "UK1"
        bigint first_seen_block
        timestamptz created_at
        timestamptz updated_at
        jsonb metadata "protocol-specific user data"
    }

    ReserveData {
        bigint id PK
        bigint protocol_id FK "UK1"
        bigint token_id FK "UK1"
        bigint block_number "UK1"
        numeric liquidity_index "ray 10^27"
        numeric variable_borrow_index "ray 10^27"
        numeric ltv
        numeric liquidation_threshold
        numeric liquidation_bonus
        numeric current_liquidity_rate
        numeric current_variable_rate
        numeric current_stable_rate
        boolean is_active
        boolean is_frozen
        boolean is_paused
    }

    Borrowers {
        bigint id PK
        bigint user_id FK "UK1"
        bigint protocol_id FK "UK1"
        bigint token_id FK "UK1"
        bigint block_number "UK1"
        int block_version "UK1"
        numeric amount
        numeric change
        timestamptz created_at
    }

    BorrowerCollateral {
        bigint id PK
        bigint user_id FK "UK1"
        bigint protocol_id FK "UK1"
        bigint token_id FK "UK1"
        bigint block_number "UK1"
        int block_version "UK1"
        numeric amount
        numeric change
        boolean is_enabled
        timestamptz created_at
    }

    UserProtocolMetadata {
        bigint id PK
        bigint user_id FK "UK1"
        bigint protocol_id FK "UK1"
        timestamptz created_at
        timestamptz updated_at
        jsonb metadata "user-protocol specific data"
    }

    Chain ||--o{ Token : ""
    Chain ||--o{ Protocol : ""
    Chain ||--o{ Users : ""
    Protocol ||--o{ ReceiptTokens : ""
    Protocol ||--o{ DebtTokens : ""
    Protocol ||--o{ ReserveData : ""
    Token ||--o{ ReceiptTokens : ""
    Token ||--o{ DebtTokens : ""
    Token ||--o{ ReserveData : ""
    Users ||--o{ Borrowers : ""
    Users ||--o{ BorrowerCollateral : ""
    Protocol ||--o{ Borrowers : ""
    Protocol ||--o{ BorrowerCollateral : ""
    Token ||--o{ Borrowers : ""
    Token ||--o{ BorrowerCollateral : ""
    Users ||--o{ UserProtocolMetadata : ""
    Protocol ||--o{ UserProtocolMetadata : ""