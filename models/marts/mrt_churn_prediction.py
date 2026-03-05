def model(dbt, session):
    """
    Train a churn prediction model and score all customers.

    Input: mrt_customer_360 (wide feature table with churn labels)
    Output: DataFrame with customer_unique_id, churn_probability, churn_prediction, churn_risk_tier
    """
    dbt.config(
        materialized="table",
        tags=["marts", "ml", "python"],
        packages=["scikit-learn", "pandas"],
    )

    # 1. Load feature table
    customer_360 = dbt.ref("mrt_customer_360").to_pandas()

    import pandas as pd
    from sklearn.ensemble import GradientBoostingClassifier
    from sklearn.model_selection import train_test_split
    from sklearn.metrics import classification_report
    from sklearn.impute import SimpleImputer

    # 2. Define features and target
    feature_columns = [
        "TOTAL_ORDERS",
        "LIFETIME_REVENUE",
        "AVG_ORDER_VALUE",
        "TOTAL_ITEMS_PURCHASED",
        "AVG_DELIVERY_DAYS",
        "AVG_DELIVERY_DELTA_DAYS",
        "AVG_REVIEW_SCORE",
        "WORST_REVIEW_SCORE",
        "AVG_INSTALLMENTS",
        "LATE_DELIVERY_COUNT",
        "CUSTOMER_TENURE_DAYS",
        "RECENCY_DAYS",
        "FREQUENCY",
        "MONETARY",
        "RFM_COMBINED_SCORE",
        "UNIQUE_CATEGORIES_PURCHASED",
        "UNIQUE_PRODUCTS_PURCHASED",
        "UNIQUE_PAYMENT_METHODS",
        "CREDIT_CARD_USAGE_RATIO",
        "BOLETO_USAGE_RATIO",
    ]

    target = "CHURNED"

    X = customer_360[feature_columns].copy()
    y = customer_360[target].copy()

    # 3. Handle missing values
    imputer = SimpleImputer(strategy="median")
    X = pd.DataFrame(imputer.fit_transform(X), columns=feature_columns)

    # 4. Train/test split
    X_train, X_test, y_train, y_test = train_test_split(
        X, y, test_size=0.2, random_state=42, stratify=y
    )

    # 5. Train model
    clf = GradientBoostingClassifier(
        n_estimators=200,
        max_depth=5,
        learning_rate=0.1,
        subsample=0.8,
        random_state=42,
    )
    clf.fit(X_train, y_train)

    # 6. Evaluate (logged to Snowflake query history)
    y_pred = clf.predict(X_test)
    report = classification_report(y_test, y_pred)
    print("=== Classification Report ===")
    print(report)

    # 7. Feature importance (logged)
    importances = sorted(
        zip(feature_columns, clf.feature_importances_),
        key=lambda x: x[1],
        reverse=True,
    )
    print("\n=== Top 10 Feature Importances ===")
    for feat, imp in importances[:10]:
        print(f"  {feat}: {imp:.4f}")

    # 8. Score ALL customers
    X_all = pd.DataFrame(
        imputer.transform(customer_360[feature_columns]), columns=feature_columns
    )
    customer_360["CHURN_PROBABILITY"] = clf.predict_proba(X_all)[:, 1]
    customer_360["CHURN_PREDICTION"] = clf.predict(X_all)

    # 9. Assign risk tiers
    customer_360["CHURN_RISK_TIER"] = pd.cut(
        customer_360["CHURN_PROBABILITY"],
        bins=[0, 0.3, 0.6, 0.8, 1.0],
        labels=["Low", "Medium", "High", "Critical"],
        include_lowest=True,
    ).astype(str)

    # 10. Return scored output
    output = customer_360[
        [
            "CUSTOMER_UNIQUE_ID",
            "STATE",
            "TOTAL_ORDERS",
            "LIFETIME_REVENUE",
            "AVG_REVIEW_SCORE",
            "RECENCY_DAYS",
            "RFM_COMBINED_SCORE",
            "CHURN_PROBABILITY",
            "CHURN_PREDICTION",
            "CHURN_RISK_TIER",
            "CHURNED",
        ]
    ].copy()

    # Ensure all columns are Snowpark-compatible types
    output["CHURN_PREDICTION"] = output["CHURN_PREDICTION"].astype(int)
    output["CHURNED"] = output["CHURNED"].astype(int)

    # Set schema context to avoid write_pandas bug
    session.sql(f"USE SCHEMA {dbt.this.database}.{dbt.this.schema}").collect()

    return session.create_dataframe(output)
