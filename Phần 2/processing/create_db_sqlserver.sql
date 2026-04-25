/* ============================================================
   Data warehouse DDL for SQL Server
   Aligned with final Transform.py output (24 core tables)
   Schema : datamart
   Notes  :
   - This script creates only the warehouse objects.
   - The target database should already exist, or be created by load_to_db.py.
   - fact_sales_item uses a surrogate key because (order_id, product_id)
     is not guaranteed unique in the source data.
   ============================================================ */

IF NOT EXISTS (SELECT 1 FROM sys.schemas WHERE name = 'datamart')
BEGIN
    EXEC('CREATE SCHEMA datamart');
END;
GO

/* ============================
   DIMENSIONS (7)
   ============================ */

IF OBJECT_ID('datamart.dim_date', 'U') IS NULL
BEGIN
    CREATE TABLE datamart.dim_date (
        date_key        INT           NOT NULL PRIMARY KEY,
        full_date       DATE          NULL,
        [day]           INT           NULL,
        [month]         INT           NULL,
        quarter         INT           NULL,
        [year]          INT           NULL,
        week_of_year    INT           NULL,
        day_of_week     INT           NULL,
        is_weekend      BIT           NULL
    );
END;
GO

IF OBJECT_ID('datamart.dim_geography', 'U') IS NULL
BEGIN
    CREATE TABLE datamart.dim_geography (
        zip             INT            NOT NULL PRIMARY KEY,
        city            NVARCHAR(255)  NULL,
        region          NVARCHAR(255)  NULL,
        district        NVARCHAR(255)  NULL
    );
END;
GO

IF OBJECT_ID('datamart.dim_product', 'U') IS NULL
BEGIN
    CREATE TABLE datamart.dim_product (
        product_id      INT            NOT NULL PRIMARY KEY,
        product_name    NVARCHAR(255)  NULL,
        category        NVARCHAR(255)  NULL,
        segment         NVARCHAR(255)  NULL,
        size            NVARCHAR(100)  NULL,
        color           NVARCHAR(100)  NULL,
        price           FLOAT          NULL,
        cogs            FLOAT          NULL
    );
END;
GO

IF OBJECT_ID('datamart.dim_customer', 'U') IS NULL
BEGIN
    CREATE TABLE datamart.dim_customer (
        customer_id         INT            NOT NULL PRIMARY KEY,
        zip                 INT            NULL,
        city                NVARCHAR(255)  NULL,
        signup_date         DATE           NULL,
        gender              NVARCHAR(100)  NULL,
        age_group           NVARCHAR(100)  NULL,
        acquisition_channel NVARCHAR(255)  NULL
    );
END;
GO

IF OBJECT_ID('datamart.dim_promotion', 'U') IS NULL
BEGIN
    CREATE TABLE datamart.dim_promotion (
        promo_id             NVARCHAR(255) NOT NULL PRIMARY KEY,
        promo_name           NVARCHAR(255) NULL,
        promo_type           NVARCHAR(255) NULL,
        discount_value       FLOAT         NULL,
        start_date           DATE          NULL,
        end_date             DATE          NULL,
        applicable_category  NVARCHAR(255) NULL,
        promo_channel        NVARCHAR(255) NULL,
        stackable_flag       INT           NULL,
        min_order_value      FLOAT         NULL
    );
END;
GO

IF OBJECT_ID('datamart.dim_return_reason', 'U') IS NULL
BEGIN
    CREATE TABLE datamart.dim_return_reason (
        return_reason_key INT            NOT NULL PRIMARY KEY,
        return_reason     NVARCHAR(255)  NULL
    );
END;
GO

IF OBJECT_ID('datamart.dim_traffic_source', 'U') IS NULL
BEGIN
    CREATE TABLE datamart.dim_traffic_source (
        traffic_source_key INT            NOT NULL PRIMARY KEY,
        traffic_source     NVARCHAR(255)  NULL
    );
END;
GO

/* ============================
   FACTS (8)
   ============================ */

IF OBJECT_ID('datamart.fact_sales_item', 'U') IS NULL
BEGIN
    CREATE TABLE datamart.fact_sales_item (
        sales_item_key   BIGINT IDENTITY(1,1) NOT NULL PRIMARY KEY,
        order_id         INT            NULL,
        product_id       INT            NULL,
        customer_id      INT            NULL,
        zip              INT            NULL,
        date_key         INT            NULL,
        promo_id         NVARCHAR(255)  NULL,
        promo_id_2       NVARCHAR(255)  NULL,
        order_date       DATE           NULL,
        month_key        INT            NULL,
        [year]           INT            NULL,
        [month]          INT            NULL,
        quarter          INT            NULL,
        order_status     NVARCHAR(255)  NULL,
        payment_method   NVARCHAR(255)  NULL,
        device_type      NVARCHAR(255)  NULL,
        order_source     NVARCHAR(255)  NULL,
        quantity         INT            NULL,
        unit_price       FLOAT          NULL,
        discount_amount  FLOAT          NULL,
        product_name     NVARCHAR(255)  NULL,
        category         NVARCHAR(255)  NULL,
        segment          NVARCHAR(255)  NULL,
        size             NVARCHAR(100)  NULL,
        color            NVARCHAR(100)  NULL,
        list_price       FLOAT          NULL,
        cogs             FLOAT          NULL,
        payment_value    FLOAT          NULL,
        gross_sales      FLOAT          NULL,
        net_sales        FLOAT          NULL,
        line_cogs        FLOAT          NULL,
        line_profit      FLOAT          NULL,
        region           NVARCHAR(255)  NULL,
        city             NVARCHAR(255)  NULL,
        district         NVARCHAR(255)  NULL
    );
END;
GO

IF OBJECT_ID('datamart.fact_orders', 'U') IS NULL
BEGIN
    CREATE TABLE datamart.fact_orders (
        order_id           INT            NOT NULL PRIMARY KEY,
        customer_id        INT            NULL,
        zip                INT            NULL,
        date_key           INT            NULL,
        order_date         DATE           NULL,
        month_key          INT            NULL,
        [year]             INT            NULL,
        [month]            INT            NULL,
        quarter            INT            NULL,
        order_status       NVARCHAR(255)  NULL,
        payment_method     NVARCHAR(255)  NULL,
        device_type        NVARCHAR(255)  NULL,
        order_source       NVARCHAR(255)  NULL,
        region             NVARCHAR(255)  NULL,
        city               NVARCHAR(255)  NULL,
        district           NVARCHAR(255)  NULL,
        distinct_products  INT            NULL,
        total_quantity     INT            NULL,
        gross_sales        FLOAT          NULL,
        discount_amount    FLOAT          NULL,
        net_sales          FLOAT          NULL,
        order_cogs         FLOAT          NULL,
        order_profit       FLOAT          NULL,
        payment_value      FLOAT          NULL
    );
END;
GO

IF OBJECT_ID('datamart.fact_returns', 'U') IS NULL
BEGIN
    CREATE TABLE datamart.fact_returns (
        return_id          NVARCHAR(255)  NOT NULL PRIMARY KEY,
        order_id           INT            NULL,
        product_id         INT            NULL,
        return_date_key    INT            NULL,
        return_reason_key  INT            NULL,
        return_date        DATE           NULL,
        return_month_key   INT            NULL,
        return_year        INT            NULL,
        return_month       INT            NULL,
        return_quarter     INT            NULL,
        return_reason      NVARCHAR(255)  NULL,
        return_quantity    INT            NULL,
        refund_amount      FLOAT          NULL,
        category           NVARCHAR(255)  NULL,
        segment            NVARCHAR(255)  NULL,
        product_name       NVARCHAR(255)  NULL,
        region             NVARCHAR(255)  NULL,
        city               NVARCHAR(255)  NULL
    );
END;
GO

IF OBJECT_ID('datamart.fact_shipments', 'U') IS NULL
BEGIN
    CREATE TABLE datamart.fact_shipments (
        order_id            INT            NOT NULL PRIMARY KEY,
        ship_date_key       INT            NULL,
        delivery_date_key   INT            NULL,
        ship_date           DATE           NULL,
        delivery_date       DATE           NULL,
        ship_month_key      INT            NULL,
        delivery_month_key  INT            NULL,
        shipping_fee        FLOAT          NULL,
        delivery_days       INT            NULL,
        region              NVARCHAR(255)  NULL,
        city                NVARCHAR(255)  NULL,
        order_source        NVARCHAR(255)  NULL,
        device_type         NVARCHAR(255)  NULL,
        order_status        NVARCHAR(255)  NULL
    );
END;
GO

IF OBJECT_ID('datamart.fact_reviews', 'U') IS NULL
BEGIN
    CREATE TABLE datamart.fact_reviews (
        review_id         NVARCHAR(255)  NOT NULL PRIMARY KEY,
        order_id          INT            NULL,
        product_id        INT            NULL,
        customer_id       INT            NULL,
        review_date_key   INT            NULL,
        review_date       DATE           NULL,
        review_month_key  INT            NULL,
        rating            INT            NULL,
        review_title      NVARCHAR(500)  NULL,
        category          NVARCHAR(255)  NULL,
        segment           NVARCHAR(255)  NULL,
        product_name      NVARCHAR(255)  NULL,
        region            NVARCHAR(255)  NULL,
        city              NVARCHAR(255)  NULL
    );
END;
GO

IF OBJECT_ID('datamart.fact_inventory_snapshot', 'U') IS NULL
BEGIN
    CREATE TABLE datamart.fact_inventory_snapshot (
        snapshot_date_key  INT            NOT NULL,
        product_id         INT            NOT NULL,
        snapshot_date      DATE           NULL,
        month_key          INT            NULL,
        [year]             INT            NULL,
        [month]            INT            NULL,
        quarter            INT            NULL,
        product_name       NVARCHAR(255)  NULL,
        category           NVARCHAR(255)  NULL,
        segment            NVARCHAR(255)  NULL,
        stock_on_hand      INT            NULL,
        units_received     INT            NULL,
        units_sold         INT            NULL,
        stockout_days      INT            NULL,
        days_of_supply     FLOAT          NULL,
        fill_rate          FLOAT          NULL,
        stockout_flag      INT            NULL,
        overstock_flag     INT            NULL,
        reorder_flag       INT            NULL,
        sell_through_rate  FLOAT          NULL,
        CONSTRAINT PK_fact_inventory_snapshot PRIMARY KEY (snapshot_date_key, product_id)
    );
END;
GO

IF OBJECT_ID('datamart.fact_sales_series', 'U') IS NULL
BEGIN
    CREATE TABLE datamart.fact_sales_series (
        date_key        INT     NOT NULL PRIMARY KEY,
        [date]          DATE    NULL,
        month_key       INT     NULL,
        [year]          INT     NULL,
        [month]         INT     NULL,
        quarter         INT     NULL,
        revenue         FLOAT   NULL,
        cogs            FLOAT   NULL,
        profit          FLOAT   NULL
    );
END;
GO

IF OBJECT_ID('datamart.fact_web_traffic', 'U') IS NULL
BEGIN
    CREATE TABLE datamart.fact_web_traffic (
        date_key                  INT            NOT NULL,
        traffic_source_key        INT            NOT NULL,
        [date]                    DATE           NULL,
        month_key                 INT            NULL,
        [year]                    INT            NULL,
        [month]                   INT            NULL,
        quarter                   INT            NULL,
        traffic_source            NVARCHAR(255)  NULL,
        sessions                  INT            NULL,
        unique_visitors           INT            NULL,
        page_views                INT            NULL,
        bounce_rate               FLOAT          NULL,
        avg_session_duration_sec  FLOAT          NULL,
        CONSTRAINT PK_fact_web_traffic PRIMARY KEY (date_key, traffic_source_key)
    );
END;
GO

/* ============================
   MARTS (9)
   ============================ */

IF OBJECT_ID('datamart.mart_sales_overview', 'U') IS NULL
BEGIN
    CREATE TABLE datamart.mart_sales_overview (
        month_key        INT            NULL,
        [year]           INT            NULL,
        [month]          INT            NULL,
        quarter          INT            NULL,
        region           NVARCHAR(255)  NULL,
        city             NVARCHAR(255)  NULL,
        category         NVARCHAR(255)  NULL,
        segment          NVARCHAR(255)  NULL,
        orders           INT            NULL,
        units_sold       INT            NULL,
        gross_sales      FLOAT          NULL,
        discount_amount  FLOAT          NULL,
        net_sales        FLOAT          NULL,
        cogs             FLOAT          NULL,
        profit           FLOAT          NULL,
        aov              FLOAT          NULL
    );
END;
GO

IF OBJECT_ID('datamart.mart_customer_insight', 'U') IS NULL
BEGIN
    CREATE TABLE datamart.mart_customer_insight (
        month_key             INT            NULL,
        [year]                INT            NULL,
        [month]               INT            NULL,
        quarter               INT            NULL,
        age_group             NVARCHAR(100)  NULL,
        gender                NVARCHAR(100)  NULL,
        region                NVARCHAR(255)  NULL,
        acquisition_channel   NVARCHAR(255)  NULL,
        active_customers      INT            NULL,
        new_customers         INT            NULL,
        orders                INT            NULL,
        revenue               FLOAT          NULL,
        revenue_per_customer  FLOAT          NULL,
        orders_per_customer   FLOAT          NULL,
        aov                   FLOAT          NULL
    );
END;
GO

IF OBJECT_ID('datamart.mart_product_performance', 'U') IS NULL
BEGIN
    CREATE TABLE datamart.mart_product_performance (
        month_key        INT            NULL,
        [year]           INT            NULL,
        [month]          INT            NULL,
        quarter          INT            NULL,
        product_id       INT            NULL,
        product_name     NVARCHAR(255)  NULL,
        category         NVARCHAR(255)  NULL,
        segment          NVARCHAR(255)  NULL,
        region           NVARCHAR(255)  NULL,
        orders           INT            NULL,
        units_sold       INT            NULL,
        revenue          FLOAT          NULL,
        profit           FLOAT          NULL,
        cogs             FLOAT          NULL,
        discount_amount  FLOAT          NULL,
        return_qty       INT            NULL,
        refund_amount    FLOAT          NULL,
        avg_rating       FLOAT          NULL,
        review_count     INT            NULL
    );
END;
GO

IF OBJECT_ID('datamart.mart_operations', 'U') IS NULL
BEGIN
    CREATE TABLE datamart.mart_operations (
        month_key          INT            NULL,
        [year]             INT            NULL,
        [month]            INT            NULL,
        quarter            INT            NULL,
        region             NVARCHAR(255)  NULL,
        orders             INT            NULL,
        revenue            FLOAT          NULL,
        avg_delivery_days  FLOAT          NULL,
        shipping_fee       FLOAT          NULL,
        returns            INT            NULL,
        refund_amount      FLOAT          NULL,
        avg_rating         FLOAT          NULL
    );
END;
GO

IF OBJECT_ID('datamart.mart_promotion_effectiveness', 'U') IS NULL
BEGIN
    CREATE TABLE datamart.mart_promotion_effectiveness (
        month_key        INT            NULL,
        [year]           INT            NULL,
        [month]          INT            NULL,
        quarter          INT            NULL,
        promo_id         NVARCHAR(255)  NULL,
        promo_name       NVARCHAR(255)  NULL,
        promo_type       NVARCHAR(255)  NULL,
        category         NVARCHAR(255)  NULL,
        segment          NVARCHAR(255)  NULL,
        orders           INT            NULL,
        units_sold       INT            NULL,
        discount_amount  FLOAT          NULL,
        revenue          FLOAT          NULL,
        profit           FLOAT          NULL
    );
END;
GO

IF OBJECT_ID('datamart.mart_return_diagnostics', 'U') IS NULL
BEGIN
    CREATE TABLE datamart.mart_return_diagnostics (
        month_key          INT            NULL,
        return_reason_key  INT            NULL,
        return_reason      NVARCHAR(255)  NULL,
        category           NVARCHAR(255)  NULL,
        segment            NVARCHAR(255)  NULL,
        region             NVARCHAR(255)  NULL,
        return_count       INT            NULL,
        return_qty         INT            NULL,
        refund_amount      FLOAT          NULL,
        avg_rating         FLOAT          NULL
    );
END;
GO

IF OBJECT_ID('datamart.mart_shipping_service', 'U') IS NULL
BEGIN
    CREATE TABLE datamart.mart_shipping_service (
        month_key          INT            NULL,
        region             NVARCHAR(255)  NULL,
        order_source       NVARCHAR(255)  NULL,
        device_type        NVARCHAR(255)  NULL,
        shipments          INT            NULL,
        shipping_fee       FLOAT          NULL,
        avg_delivery_days  FLOAT          NULL
    );
END;
GO

IF OBJECT_ID('datamart.mart_inventory_risk', 'U') IS NULL
BEGIN
    CREATE TABLE datamart.mart_inventory_risk (
        month_key          INT            NULL,
        [year]             INT            NULL,
        [month]            INT            NULL,
        quarter            INT            NULL,
        product_id         INT            NULL,
        product_name       NVARCHAR(255)  NULL,
        category           NVARCHAR(255)  NULL,
        segment            NVARCHAR(255)  NULL,
        stock_on_hand      INT            NULL,
        units_received     INT            NULL,
        units_sold         INT            NULL,
        stockout_days      INT            NULL,
        days_of_supply     FLOAT          NULL,
        fill_rate          FLOAT          NULL,
        stockout_flag      INT            NULL,
        overstock_flag     INT            NULL,
        reorder_flag       INT            NULL,
        sell_through_rate  FLOAT          NULL
    );
END;
GO

IF OBJECT_ID('datamart.mart_sales_forecast_ready', 'U') IS NULL
BEGIN
    CREATE TABLE datamart.mart_sales_forecast_ready (
        date_key           INT     NOT NULL PRIMARY KEY,
        [date]             DATE    NULL,
        month_key          INT     NULL,
        [year]             INT     NULL,
        [month]            INT     NULL,
        quarter            INT     NULL,
        revenue            FLOAT   NULL,
        cogs               FLOAT   NULL,
        profit             FLOAT   NULL,
        lag_1              FLOAT   NULL,
        lag_7              FLOAT   NULL,
        lag_30             FLOAT   NULL,
        rolling_mean_7     FLOAT   NULL,
        rolling_mean_30    FLOAT   NULL,
        rolling_std_30     FLOAT   NULL,
        revenue_growth_7d  FLOAT   NULL,
        profit_margin      FLOAT   NULL
    );
END;
GO

/* ============================
   FOREIGN KEYS
   ============================ */

IF OBJECT_ID('datamart.FK_dim_customer_geography', 'F') IS NULL
ALTER TABLE datamart.dim_customer
ADD CONSTRAINT FK_dim_customer_geography FOREIGN KEY (zip)
REFERENCES datamart.dim_geography(zip);
GO

IF OBJECT_ID('datamart.FK_fact_sales_item_product', 'F') IS NULL
ALTER TABLE datamart.fact_sales_item
ADD CONSTRAINT FK_fact_sales_item_product FOREIGN KEY (product_id)
REFERENCES datamart.dim_product(product_id);
GO

IF OBJECT_ID('datamart.FK_fact_sales_item_customer', 'F') IS NULL
ALTER TABLE datamart.fact_sales_item
ADD CONSTRAINT FK_fact_sales_item_customer FOREIGN KEY (customer_id)
REFERENCES datamart.dim_customer(customer_id);
GO

IF OBJECT_ID('datamart.FK_fact_sales_item_geography', 'F') IS NULL
ALTER TABLE datamart.fact_sales_item
ADD CONSTRAINT FK_fact_sales_item_geography FOREIGN KEY (zip)
REFERENCES datamart.dim_geography(zip);
GO

IF OBJECT_ID('datamart.FK_fact_sales_item_date', 'F') IS NULL
ALTER TABLE datamart.fact_sales_item
ADD CONSTRAINT FK_fact_sales_item_date FOREIGN KEY (date_key)
REFERENCES datamart.dim_date(date_key);
GO

IF OBJECT_ID('datamart.FK_fact_sales_item_promo_1', 'F') IS NULL
ALTER TABLE datamart.fact_sales_item
ADD CONSTRAINT FK_fact_sales_item_promo_1 FOREIGN KEY (promo_id)
REFERENCES datamart.dim_promotion(promo_id);
GO

IF OBJECT_ID('datamart.FK_fact_sales_item_promo_2', 'F') IS NULL
ALTER TABLE datamart.fact_sales_item
ADD CONSTRAINT FK_fact_sales_item_promo_2 FOREIGN KEY (promo_id_2)
REFERENCES datamart.dim_promotion(promo_id);
GO

IF OBJECT_ID('datamart.FK_fact_orders_customer', 'F') IS NULL
ALTER TABLE datamart.fact_orders
ADD CONSTRAINT FK_fact_orders_customer FOREIGN KEY (customer_id)
REFERENCES datamart.dim_customer(customer_id);
GO

IF OBJECT_ID('datamart.FK_fact_orders_geography', 'F') IS NULL
ALTER TABLE datamart.fact_orders
ADD CONSTRAINT FK_fact_orders_geography FOREIGN KEY (zip)
REFERENCES datamart.dim_geography(zip);
GO

IF OBJECT_ID('datamart.FK_fact_orders_date', 'F') IS NULL
ALTER TABLE datamart.fact_orders
ADD CONSTRAINT FK_fact_orders_date FOREIGN KEY (date_key)
REFERENCES datamart.dim_date(date_key);
GO

IF OBJECT_ID('datamart.FK_fact_returns_order', 'F') IS NULL
ALTER TABLE datamart.fact_returns
ADD CONSTRAINT FK_fact_returns_order FOREIGN KEY (order_id)
REFERENCES datamart.fact_orders(order_id);
GO

IF OBJECT_ID('datamart.FK_fact_returns_product', 'F') IS NULL
ALTER TABLE datamart.fact_returns
ADD CONSTRAINT FK_fact_returns_product FOREIGN KEY (product_id)
REFERENCES datamart.dim_product(product_id);
GO

IF OBJECT_ID('datamart.FK_fact_returns_date', 'F') IS NULL
ALTER TABLE datamart.fact_returns
ADD CONSTRAINT FK_fact_returns_date FOREIGN KEY (return_date_key)
REFERENCES datamart.dim_date(date_key);
GO

IF OBJECT_ID('datamart.FK_fact_returns_reason', 'F') IS NULL
ALTER TABLE datamart.fact_returns
ADD CONSTRAINT FK_fact_returns_reason FOREIGN KEY (return_reason_key)
REFERENCES datamart.dim_return_reason(return_reason_key);
GO

IF OBJECT_ID('datamart.FK_fact_shipments_order', 'F') IS NULL
ALTER TABLE datamart.fact_shipments
ADD CONSTRAINT FK_fact_shipments_order FOREIGN KEY (order_id)
REFERENCES datamart.fact_orders(order_id);
GO

IF OBJECT_ID('datamart.FK_fact_shipments_ship_date', 'F') IS NULL
ALTER TABLE datamart.fact_shipments
ADD CONSTRAINT FK_fact_shipments_ship_date FOREIGN KEY (ship_date_key)
REFERENCES datamart.dim_date(date_key);
GO

IF OBJECT_ID('datamart.FK_fact_shipments_delivery_date', 'F') IS NULL
ALTER TABLE datamart.fact_shipments
ADD CONSTRAINT FK_fact_shipments_delivery_date FOREIGN KEY (delivery_date_key)
REFERENCES datamart.dim_date(date_key);
GO

IF OBJECT_ID('datamart.FK_fact_reviews_order', 'F') IS NULL
ALTER TABLE datamart.fact_reviews
ADD CONSTRAINT FK_fact_reviews_order FOREIGN KEY (order_id)
REFERENCES datamart.fact_orders(order_id);
GO

IF OBJECT_ID('datamart.FK_fact_reviews_product', 'F') IS NULL
ALTER TABLE datamart.fact_reviews
ADD CONSTRAINT FK_fact_reviews_product FOREIGN KEY (product_id)
REFERENCES datamart.dim_product(product_id);
GO

IF OBJECT_ID('datamart.FK_fact_reviews_customer', 'F') IS NULL
ALTER TABLE datamart.fact_reviews
ADD CONSTRAINT FK_fact_reviews_customer FOREIGN KEY (customer_id)
REFERENCES datamart.dim_customer(customer_id);
GO

IF OBJECT_ID('datamart.FK_fact_reviews_date', 'F') IS NULL
ALTER TABLE datamart.fact_reviews
ADD CONSTRAINT FK_fact_reviews_date FOREIGN KEY (review_date_key)
REFERENCES datamart.dim_date(date_key);
GO

IF OBJECT_ID('datamart.FK_fact_inventory_snapshot_date', 'F') IS NULL
ALTER TABLE datamart.fact_inventory_snapshot
ADD CONSTRAINT FK_fact_inventory_snapshot_date FOREIGN KEY (snapshot_date_key)
REFERENCES datamart.dim_date(date_key);
GO

IF OBJECT_ID('datamart.FK_fact_inventory_snapshot_product', 'F') IS NULL
ALTER TABLE datamart.fact_inventory_snapshot
ADD CONSTRAINT FK_fact_inventory_snapshot_product FOREIGN KEY (product_id)
REFERENCES datamart.dim_product(product_id);
GO

IF OBJECT_ID('datamart.FK_fact_sales_series_date', 'F') IS NULL
ALTER TABLE datamart.fact_sales_series
ADD CONSTRAINT FK_fact_sales_series_date FOREIGN KEY (date_key)
REFERENCES datamart.dim_date(date_key);
GO

IF OBJECT_ID('datamart.FK_fact_web_traffic_date', 'F') IS NULL
ALTER TABLE datamart.fact_web_traffic
ADD CONSTRAINT FK_fact_web_traffic_date FOREIGN KEY (date_key)
REFERENCES datamart.dim_date(date_key);
GO

IF OBJECT_ID('datamart.FK_fact_web_traffic_source', 'F') IS NULL
ALTER TABLE datamart.fact_web_traffic
ADD CONSTRAINT FK_fact_web_traffic_source FOREIGN KEY (traffic_source_key)
REFERENCES datamart.dim_traffic_source(traffic_source_key);
GO

IF OBJECT_ID('datamart.FK_mart_sales_forecast_ready_date', 'F') IS NULL
ALTER TABLE datamart.mart_sales_forecast_ready
ADD CONSTRAINT FK_mart_sales_forecast_ready_date FOREIGN KEY (date_key)
REFERENCES datamart.dim_date(date_key);
GO

/* ============================
   INDEXES
   ============================ */

IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE name = 'IX_fact_sales_item_order_id' AND object_id = OBJECT_ID('datamart.fact_sales_item'))
    CREATE INDEX IX_fact_sales_item_order_id ON datamart.fact_sales_item(order_id);
GO
IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE name = 'IX_fact_sales_item_product_id' AND object_id = OBJECT_ID('datamart.fact_sales_item'))
    CREATE INDEX IX_fact_sales_item_product_id ON datamart.fact_sales_item(product_id);
GO
IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE name = 'IX_fact_sales_item_date_key' AND object_id = OBJECT_ID('datamart.fact_sales_item'))
    CREATE INDEX IX_fact_sales_item_date_key ON datamart.fact_sales_item(date_key);
GO
IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE name = 'IX_fact_orders_date_key' AND object_id = OBJECT_ID('datamart.fact_orders'))
    CREATE INDEX IX_fact_orders_date_key ON datamart.fact_orders(date_key);
GO
IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE name = 'IX_fact_returns_order_id' AND object_id = OBJECT_ID('datamart.fact_returns'))
    CREATE INDEX IX_fact_returns_order_id ON datamart.fact_returns(order_id);
GO
IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE name = 'IX_fact_reviews_order_id' AND object_id = OBJECT_ID('datamart.fact_reviews'))
    CREATE INDEX IX_fact_reviews_order_id ON datamart.fact_reviews(order_id);
GO
IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE name = 'IX_fact_inventory_snapshot_month_key' AND object_id = OBJECT_ID('datamart.fact_inventory_snapshot'))
    CREATE INDEX IX_fact_inventory_snapshot_month_key ON datamart.fact_inventory_snapshot(month_key);
GO
IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE name = 'IX_fact_sales_series_month_key' AND object_id = OBJECT_ID('datamart.fact_sales_series'))
    CREATE INDEX IX_fact_sales_series_month_key ON datamart.fact_sales_series(month_key);
GO
