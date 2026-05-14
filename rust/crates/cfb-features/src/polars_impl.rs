/// Feature computation using the Polars Rust API.
///
/// Apples-to-apples comparison with the Python Polars implementation.
use anyhow::Result;
use polars::prelude::*;

const WINDOW_1M: &str = "60000000000ns";
const WINDOW_5M: &str = "300000000000ns";
const WINDOW_15M: &str = "900000000000ns";

fn rolling_opts(window: &str) -> RollingOptionsDynamicWindow {
    RollingOptionsDynamicWindow {
        window_size: Duration::parse(window),
        min_periods: 1,
        closed_window: ClosedWindow::Right,
        fn_params: None,
    }
}

fn rolling_sum(expr: Expr, by: Expr, window: &str) -> Expr {
    expr.rolling_sum_by(by, rolling_opts(window))
}

pub fn compute_polars(df: DataFrame) -> Result<DataFrame> {
    let lf = df.lazy().sort(["ts"], Default::default());

    let signed_qty = when(col("side").eq(lit("buy")))
        .then(col("qty"))
        .otherwise(-col("qty"))
        .alias("signed_qty");

    let pv = (col("price") * col("qty")).alias("pv");
    // count column: 1 per row for rolling count via sum
    let one = lit(1i64).alias("_one");

    let lf = lf.with_columns([signed_qty, pv, one]);

    let by = col("ts");

    let lf = lf.with_columns([
        (rolling_sum(col("pv"), by.clone(), WINDOW_1M)
            / rolling_sum(col("qty"), by.clone(), WINDOW_1M))
        .alias("vwap_1m"),
        (rolling_sum(col("pv"), by.clone(), WINDOW_5M)
            / rolling_sum(col("qty"), by.clone(), WINDOW_5M))
        .alias("vwap_5m"),
        (rolling_sum(col("pv"), by.clone(), WINDOW_15M)
            / rolling_sum(col("qty"), by.clone(), WINDOW_15M))
        .alias("vwap_15m"),
        rolling_sum(col("signed_qty"), by.clone(), WINDOW_1M).alias("ofi_1m"),
        rolling_sum(col("_one"), by.clone(), WINDOW_1M).alias("trade_count_1m"),
    ]);

    // log return: ln(price / price.shift(1)) computed via a map on a struct column
    let log_ret = (col("price") / col("price").shift(lit(1)))
        .map(
            |s| {
                let ca = s.f64()?;
                let out: Float64Chunked =
                    ca.apply(|v| v.map(|x| if x > 0.0 { x.ln() } else { 0.0 }));
                Ok(Some(out.into_series()))
            },
            GetOutput::from_type(DataType::Float64),
        )
        .alias("log_ret");

    let lf = lf.with_columns([log_ret]);

    let sq_ret = (col("log_ret") * col("log_ret")).alias("sq_ret");
    let lf = lf.with_columns([sq_ret]);

    let lf = lf.with_columns([
        rolling_sum(col("sq_ret"), by.clone(), WINDOW_1M).alias("rv_1m"),
        rolling_sum(col("sq_ret"), by.clone(), WINDOW_5M).alias("rv_5m"),
    ]);

    let lf = lf.with_columns([lit(NULL).cast(DataType::Float64).alias("microprice")]);

    let result = lf
        .select([
            col("ts"),
            col("symbol"),
            col("vwap_1m"),
            col("vwap_5m"),
            col("vwap_15m"),
            col("rv_1m"),
            col("rv_5m"),
            col("ofi_1m"),
            col("microprice"),
            col("trade_count_1m"),
        ])
        .collect()?;

    Ok(result)
}
