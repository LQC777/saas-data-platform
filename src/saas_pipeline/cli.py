import argparse

from omegaconf import OmegaConf

from src.saas_pipeline.bronze import (
    create_spark_session,
    run_bronze,
)
from src.saas_pipeline.config import load_config
from src.saas_pipeline.gold import run_gold
from src.saas_pipeline.silver import run_silver


TENANTS = [
    "sv",
    "hn",
    "gt",
    "pe",
    "ec",
    "jm",
]


def parse_args():
    parser = argparse.ArgumentParser(
        description="SAAS multi-tenant data pipeline"
    )

    parser.add_argument(
        "--env",
        required=True,
        choices=[
            "dev",
            "qa",
            "main",
        ],
        help="Execution environment",
    )

    parser.add_argument(
        "--tenant",
        required=True,
        choices=[
            "sv",
            "hn",
            "gt",
            "pe",
            "ec",
            "jm",
            "all",
        ],
        help=(
            "Tenant code or 'all' "
            "to process every tenant"
        ),
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


def build_config(
    env,
    tenant,
    start_date=None,
    end_date=None,
):
    config = load_config(
        env=env,
        tenant=tenant,
    )

    cli_overrides = {
        "execution": {
            "tenant": tenant,
        }
    }

    if start_date:
        cli_overrides[
            "execution"
        ][
            "start_date"
        ] = start_date

    if end_date:
        cli_overrides[
            "execution"
        ][
            "end_date"
        ] = end_date

    cli_config = OmegaConf.create(
        cli_overrides
    )

    return OmegaConf.merge(
        config,
        cli_config,
    )


def run_tenant_pipeline(
    spark,
    config,
):
    tenant = (
        config.tenant.code
        .upper()
    )

    print(
        "\n"
        "========================================"
    )
    print(
        f"Processing tenant: {tenant}"
    )
    print(
        "========================================"
    )

    print("\n=== CONFIGURATION ===")
    print(
        OmegaConf.to_yaml(config)
    )

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

    # ---------------------------------------------------------
    # QUALITY will be executed here.
    #
    # It must run after Silver and before Gold.
    # ---------------------------------------------------------

    print("\n=== GOLD ===")

    run_gold(
        spark,
        config,
    )


def main():
    args = parse_args()

    tenants = (
        TENANTS
        if args.tenant == "all"
        else [args.tenant]
    )

    spark = create_spark_session()

    failures = []

    try:
        for tenant in tenants:
            try:
                config = build_config(
                    env=args.env,
                    tenant=tenant,
                    start_date=args.start_date,
                    end_date=args.end_date,
                )

                run_tenant_pipeline(
                    spark,
                    config,
                )

            except Exception as exc:
                print(
                    f"\nERROR processing tenant "
                    f"{tenant.upper()}: {exc}"
                )

                failures.append(
                    (
                        tenant,
                        str(exc),
                    )
                )

                # Load tenant configuration to determine
                # fail_fast behavior.
                try:
                    tenant_config = load_config(
                        env=args.env,
                        tenant=tenant,
                    )

                    fail_fast = bool(
                        tenant_config
                        .execution
                        .fail_fast
                    )

                except Exception:
                    fail_fast = False

                if fail_fast:
                    raise

    finally:
        spark.stop()

    # ---------------------------------------------------------
    # Report failures after processing all tenants
    # when fail_fast = false
    # ---------------------------------------------------------

    if failures:
        print(
            "\n========================================"
        )
        print(
            "PIPELINE COMPLETED WITH FAILURES"
        )
        print(
            "========================================"
        )

        for tenant, error in failures:
            print(
                f"{tenant.upper()}: "
                f"{error}"
            )

        raise RuntimeError(
            f"{len(failures)} tenant(s) failed"
        )

    print(
        "\n========================================"
    )
    print(
        "PIPELINE COMPLETED SUCCESSFULLY"
    )
    print(
        "========================================"
    )


if __name__ == "__main__":
    main()