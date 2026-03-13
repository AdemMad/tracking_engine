import polars as pl

from .config import DEFAULT_STORAGE, StorageConfig, ZONE_FLIP


def _pitch_expression(
    column_name: str,
    fallback: float,
) -> pl.Expr:
    return pl.col(column_name).fill_null(fallback)


def assign_zone(
    x_col: str = "player_x",
    y_col: str = "player_y",
    cfg: StorageConfig = DEFAULT_STORAGE,
) -> pl.Expr:
    """Assign one of the warehouse clustering zones from x/y coordinates."""
    x = pl.col(x_col)
    y = pl.col(y_col)
    length = _pitch_expression("pitch_length", cfg.length)
    width = _pitch_expression("pitch_width", cfg.width)
    x_min = -length / 2
    x_max = length / 2
    y_min = -width / 2
    y_max = width / 2
    x1 = x_min + cfg.zone.penalty_box_depth_m
    x2 = x_min + cfg.zone.wide_channel_depth_m
    x3 = x_max - cfg.zone.wide_channel_depth_m
    x4 = x_max - cfg.zone.penalty_box_depth_m
    y1 = y_min + cfg.zone.penalty_box_side_inset_m
    y2 = -width / cfg.zone.central_band_divisor
    y3 = width / cfg.zone.central_band_divisor
    y4 = y_max - cfg.zone.penalty_box_side_inset_m

    return (
        pl.when((x >= x4) & (y < y1)).then(pl.lit("att_box_left"))
        .when((x >= x4) & (y > y4)).then(pl.lit("att_box_right"))
        .when((x >= x4) & (y >= y1) & (y <= y4)).then(pl.lit("att_box_center"))
        .when((x >= x3) & (y < y2)).then(pl.lit("att_wide_left"))
        .when((x >= x3) & (y > y3)).then(pl.lit("att_wide_right"))
        .when((x >= x3) & (y >= y2) & (y <= y3)).then(pl.lit("att_halfspace_center"))
        .when((x >= x3) & (y >= y1) & (y < y2)).then(pl.lit("att_halfspace_left"))
        .when((x >= x3) & (y > y3) & (y <= y4)).then(pl.lit("att_halfspace_right"))
        .when((x >= x2) & (x <= x3) & (y < y2)).then(pl.lit("mid_wide_left"))
        .when((x >= x2) & (x <= x3) & (y > y3)).then(pl.lit("mid_wide_right"))
        .when((x >= x2) & (x <= x3) & (y >= y2) & (y <= y3)).then(pl.lit("mid_center"))
        .when((x >= x2) & (x <= x3) & (y >= y1) & (y < y2)).then(pl.lit("mid_halfspace_left"))
        .when((x >= x2) & (x <= x3) & (y > y3) & (y <= y4)).then(pl.lit("mid_halfspace_right"))
        .when((x <= x1) & (y < y1)).then(pl.lit("def_box_left"))
        .when((x <= x1) & (y > y4)).then(pl.lit("def_box_right"))
        .when((x <= x1) & (y >= y1) & (y <= y4)).then(pl.lit("def_box_center"))
        .when((x <= x2) & (y < y2)).then(pl.lit("def_wide_left"))
        .when((x <= x2) & (y > y3)).then(pl.lit("def_wide_right"))
        .otherwise(pl.lit("def_center"))
    )


def assign_ball_zone(cfg: StorageConfig = DEFAULT_STORAGE) -> pl.Expr:
    """Apply the same zone logic to ball coordinates."""
    return assign_zone("ball_x", "ball_y", cfg).alias(cfg.ball_zone_column)


def flip_zones_for_away(zone_col: str) -> pl.Expr:
    """Keep zones relative to each team's attacking direction."""
    expr = pl.col(zone_col)
    for original, flipped in ZONE_FLIP.items():
        expr = (
            pl.when((pl.col("team") == "away") & (pl.col(zone_col) == original))
            .then(pl.lit(flipped))
            .otherwise(expr)
        )
    return expr


def add_zone_columns(
    frame: pl.DataFrame | pl.LazyFrame,
    cfg: StorageConfig = DEFAULT_STORAGE,
) -> pl.DataFrame | pl.LazyFrame:
    """Add player and ball zones as clustering-friendly columns."""
    return (
        frame.with_columns([
            assign_zone("player_x", "player_y", cfg).alias(cfg.pitch_zone_column),
            assign_ball_zone(cfg),
        ])
        .with_columns([
            flip_zones_for_away(cfg.pitch_zone_column).alias(cfg.pitch_zone_column),
            flip_zones_for_away(cfg.ball_zone_column).alias(cfg.ball_zone_column),
        ])
    )
