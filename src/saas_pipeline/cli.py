import argparse

from omegaconf import OmegaConf

from src.saas_pipeline.bronze import (
    create_spark_session,
    run_bronze,
)
from src.saas_pipeline.config import load_config
from src.saas_pipeline.silver import run_silver


def parse_args():
    parser = argparse.ArgumentParser(
        description="SAAS multi-tenant data pipeline"
    )

    parser.add_argument(
        "--env",
        required=True,
        choices=["dev", "qa", "main"],
        help="Execution environment",
    )

    parser.add_argument(
        "--tenant",
        required=True,
        help="Tenant code, for example: sv, pe, hn, or all",
    )

    parser.add_argument(
        "--start-date",
        required=False,
        help="Start date in YYYY-MM-DD format",
    )

    parser.add_argument(
        "--end-date",
        required=False,
        help="End date in YYYY-MM-DD format",
    )

    return parser.parse_args()


def main():
    args = parse_args()

    config = load_config(
        env=args.env,
        tenant=args.tenant,
    )

    cli_overrides = {
        "execution": {
            "tenant": args.tenant,
        }
    }

    if args.start_date:
        cli_overrides["execution"][
            "start_date"
        ] = args.start_date

    if args.end_date:
        cli_overrides["execution"][
            "end_date"
        ] = args.end_date

    cli_config = OmegaConf.create(
        cli_overrides
    )

    config = OmegaConf.merge(
        config,
        cli_config,
    )

    print(OmegaConf.to_yaml(config))

    spark = create_spark_session()

    try:
        print("\n=== BRONZE ===")
        run_bronze(
            spark,
            config,
        )

        print("\n=== SILVER ===")
        run_silver(
            spark,
            config,
        )

    finally:
        spark.stop()


if __name__ == "__main__":
    main()