from pyspark.sql import DataFrame
from pyspark.sql.functions import count, expr, when, col, dayofyear, to_date, hour
import matplotlib.pyplot as plt
import calendar


def prepare_logins_per_minut_graph(input_df: DataFrame, output_path: str):
    df = input_df.withColumn(
        "MinuteOfDay",
        when(col("Login Time").isNotNull(), expr("hour(`Login Time`) * 60 + minute(`Login Time`)"))
        .otherwise(None)
    )

    success_stats = (
        df.groupBy("MinuteOfDay", "Login Successful")
        .agg(count("*").alias("Count"))
    )

    success_stats.show()

    pivot_stats = success_stats.groupBy("MinuteOfDay").pivot("Login Successful", ["true", "false"]).sum("Count")
    pivot_stats.show()

    pivot_stats = pivot_stats.orderBy("MinuteOfDay")
    pivot_stats.show()

    pandas_stats = pivot_stats.toPandas()

    pandas_stats.to_csv(output_path + "/tables/logins_per_minute.csv", index=False)

    minutes_of_day = pandas_stats["MinuteOfDay"]
    successful = pandas_stats["true"]
    unsuccessful = pandas_stats["false"]

    row_count = len(minutes_of_day.index)

    plt.figure(figsize=(20, 7))
    plt.bar(minutes_of_day, unsuccessful, label="Unsuccessful Logins", color="salmon")
    plt.bar(minutes_of_day, successful, bottom=unsuccessful, label="Successful Logins", color="skyblue")

    plt.xlabel("Minute of Day")
    plt.ylabel("Number of Logins")
    plt.title("Logins per Minute of Day (Successful vs Unsuccessful)")
    plt.xticks(range(0, row_count, 60), labels=[f"{h}:00" for h in range(23)] + ["24:00"], rotation=45)
    plt.grid(axis="y", linestyle="--", alpha=0.7)
    plt.legend()

    plt.savefig(output_path + "/logins_per_minute_of_day.png", format="png", dpi=300)
    plt.close()


def prepare_logins_per_day_graph(input_df: DataFrame, output_path: str):
    df = input_df.withColumn("DayOfYear", dayofyear(to_date(col("Login Date"), "yyyy-MM-dd")))
    success_stats = (
        df.groupBy("DayOfYear", "Login Successful")
        .agg(count("*").alias("Count"))
    )
    success_stats.show()

    pivot_stats = success_stats.groupBy("DayOfYear").pivot("Login Successful", ["true", "false"]).sum("Count")
    pivot_stats = pivot_stats.fillna(0).orderBy("DayOfYear")
    pivot_stats.show()

    pandas_stats = pivot_stats.toPandas()

    pandas_stats.to_csv(output_path + "/tables/logins_per_day.csv", index=False)

    days_of_year = pandas_stats["DayOfYear"]
    successful = pandas_stats["true"]
    unsuccessful = pandas_stats["false"]

    print("days_of_year count: ", len(days_of_year.index))

    plt.figure(figsize=(20, 7))
    plt.bar(days_of_year, unsuccessful, label="Unsuccessful Logins", color="salmon")
    plt.bar(days_of_year, successful, bottom=unsuccessful, label="Successful Logins", color="skyblue")

    cumulative_days = [0]
    for month in range(1, 12):
        days_in_month = calendar.monthrange(2020, month)[1]
        cumulative_days.append(cumulative_days[-1] + days_in_month)
    month_labels = calendar.month_abbr[1:]

    print("cumulative_days:, ", cumulative_days)
    print("month_labels:, ", month_labels)

    plt.xticks(
        cumulative_days,
        labels=month_labels,
        rotation=45
    )

    plt.xlabel("Day of Year")
    plt.ylabel("Number of Logins")
    plt.title("Logins per Day of Year (Successful vs Unsuccessful)")
    plt.grid(axis="y", linestyle="--", alpha=0.7)
    plt.legend()

    plt.savefig(output_path + "/logins_per_day_of_year.png", format="png", dpi=300)
    plt.close()


def prepare_location_table(input_df: DataFrame, output_path: str):
    country_stats = (
        input_df.groupBy("Country")
        .agg(count("*").alias("Login Count"))
        .orderBy("Login Count", ascending=False)
    )

    country_stats.show()

    top_countries = country_stats.toPandas().head(10)
    top_countries["Percentage"] = (
            top_countries["Login Count"] / top_countries["Login Count"].sum() * 100
    )

    top_countries.to_csv(output_path + "/tables/locations.csv", index=False)

    plt.figure(figsize=(8, 8))
    plt.pie(
        top_countries["Login Count"],
        labels=top_countries["Country"],
        autopct="%1.1f%%",
    )
    plt.title("Percentage of Logins by Country")
    plt.savefig(output_path + "/logins_pie_chart.png", format="png", dpi=300)
    plt.close()


def prepare_device_usage_table(input_df: DataFrame, output_path: str):
    hourly_device_stats = (
        input_df.withColumn("HourOfDay", hour("Login Time"))
        .groupBy("HourOfDay", "Device Type")
        .agg(count("*").alias("Count"))
        .groupBy("HourOfDay")
        .pivot("Device Type", ["mobile", "desktop", "tablet"])  # Zakładamy te typy urządzeń
        .sum("Count")
        .fillna(0)
        .orderBy("HourOfDay")
    )

    hourly_device_stats.show()
    hourly_device_stats = hourly_device_stats.toPandas()

    hourly_device_stats.to_csv(output_path + "/tables/device_usage.csv", index=False)

    hours = hourly_device_stats["HourOfDay"]
    mobile = hourly_device_stats["mobile"]
    desktop = hourly_device_stats["desktop"]
    tablet = hourly_device_stats["tablet"]

    plt.figure(figsize=(12, 6))
    plt.bar(hours, mobile, label="Mobile", color="skyblue")
    plt.bar(hours, desktop, bottom=mobile, label="Desktop", color="salmon")
    plt.bar(hours, tablet, bottom=mobile + desktop, label="Tablet", color="lightgreen")

    plt.xlabel("Hour of Day")
    plt.ylabel("Number of Logins")
    plt.title("Device Type Usage by Hour of Day")
    plt.xticks(range(0, 24), labels=[f"{h}:00" for h in range(24)], rotation=45)
    plt.grid(axis="y", linestyle="--", alpha=0.7)
    plt.legend()

    plt.savefig(output_path + "/device_usage_by_hour.png", format="png", dpi=300)
    plt.close()
