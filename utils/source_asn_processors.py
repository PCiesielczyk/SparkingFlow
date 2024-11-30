from matplotlib import pyplot as plt
from pyspark.sql import DataFrame
from pyspark.sql.functions import count, desc, col, sum, when


def prepare_top_org_by_logs_graph(input_df: DataFrame, output_path: str):
    org_stats = (
        input_df.groupBy("name")
        .agg(count("*").alias("Logins"))
        .orderBy(desc("Logins"))
        .fillna({"name": "Unknown"})
    )
    org_stats = org_stats.limit(10)
    org_stats.show()

    org_stats_pandas = org_stats.toPandas()

    plt.figure(figsize=(12, 6))
    plt.bar(org_stats_pandas["name"], org_stats_pandas["Logins"], color="skyblue")
    plt.xticks(rotation=45, ha="right")
    plt.xlabel("Organization")
    plt.ylabel("Number of Logins")
    plt.title("Top Organizations by Number of Logins")
    plt.tight_layout()
    plt.savefig(output_path + "/top_organizations.png", dpi=300)
    plt.close()


def prepare_top_attacks_per_org(input_df: DataFrame, output_path: str):
    attack_stats = (
        input_df.withColumn("Attack Logins", when(col("Is Attack IP") == True, 1).otherwise(0))
        .groupBy("name")
        .agg(
            count("*").alias("Total Logins"),
            sum("Attack Logins").alias("Attack Logins")
        )
        .withColumn("Normal Logins", col("Total Logins") - col("Attack Logins"))
        .orderBy(col("Attack Logins").desc())
        .fillna({"name": "Unknown"})
        .limit(5)
    )

    pandas_stats = attack_stats.toPandas()

    asn_names = pandas_stats["name"]
    attack_logins = pandas_stats["Attack Logins"]
    normal_logins = pandas_stats["Normal Logins"]

    fig, ax = plt.subplots(figsize=(12, 6))

    ax.bar(asn_names, attack_logins, label="Attack Logins", color="salmon")

    ax.bar(asn_names, normal_logins, bottom=attack_logins, label="Normal Logins", color="skyblue")

    ax.set_xlabel("ASN Name")
    ax.set_ylabel("Logins")
    ax.set_title("Attack Logins vs Normal Logins per ASN")
    ax.set_xticklabels(asn_names, rotation=90)

    ax.legend()

    plt.tight_layout()
    plt.savefig(output_path + "/attack_logins_per_asn.png", dpi=300)

    plt.show()


def prepare_account_takeover_graph(input_df: DataFrame, output_path: str):
    takeover_stats = (
        input_df.filter(col("Is Account Takeover"))
        .groupBy("name")
        .agg(count("*").alias("Account Takeover Count"))
        .orderBy(col("Account Takeover Count").desc())
        .fillna({"name": "Unknown"})
        .limit(10)
    )

    pandas_stats = takeover_stats.toPandas()

    asn_names = pandas_stats["name"]
    takeover_counts = pandas_stats["Account Takeover Count"]

    plt.figure(figsize=(12, 6))
    plt.bar(asn_names, takeover_counts, label="Account Takeovers")

    plt.xlabel("ASN Name")
    plt.ylabel("Account Takeover Count")
    plt.title("Account Takeover Counts per ASN")
    plt.xticks(rotation=90)

    plt.legend()

    plt.tight_layout()
    plt.savefig(output_path + "/account_takeover_per_asn.png", dpi=300)

    plt.show()


def prepare_os_account_takeover_graph(input_df: DataFrame, output_path: str):
    takeover_stats = (
        input_df.filter(col("Is Account Takeover") == True)
        .groupBy("OS Name")
        .agg(count("*").alias("Account Takeover Count"))
        .orderBy(col("Account Takeover Count").desc())
    )

    pandas_stats = takeover_stats.toPandas()

    os_names = pandas_stats["OS Name"]
    takeover_counts = pandas_stats["Account Takeover Count"]

    plt.figure(figsize=(12, 6))
    plt.bar(os_names, takeover_counts, label="Account Takeovers")

    plt.xlabel("Operating System Name")
    plt.ylabel("Account Takeover Count")
    plt.title("Account Takeover Counts by OS")
    plt.xticks(rotation=45, ha="right")

    plt.legend()

    plt.tight_layout()
    plt.savefig(output_path + "/account_takeover_per_os.png", dpi=300)

    plt.show()


def prepare_os_attack_graph(input_df: DataFrame, output_path: str):
    attack_stats = (
        input_df.filter(col("Is Attack IP") == True)
        .groupBy("OS Name")
        .agg(count("*").alias("Attack IP Count"))
        .orderBy(col("Attack IP Count").desc())
    )

    pandas_stats = attack_stats.toPandas()

    os_names = pandas_stats["OS Name"]
    takeover_counts = pandas_stats["Attack IP Count"]

    plt.figure(figsize=(12, 6))
    plt.bar(os_names, takeover_counts, label="Attack IPs")

    plt.xlabel("Operating System Name")
    plt.ylabel("Attack IP Count")
    plt.title("Attack IP Counts by OS")
    plt.xticks(rotation=45, ha="right")

    plt.legend()

    plt.tight_layout()
    plt.savefig(output_path + "/attack_ips_per_os.png", dpi=300)

    plt.show()
