import argparse

from omegaconf import OmegaConf

from src.saas_pipeline.config import load_config


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

    cli_config = OmegaConf.create(
        {
            "execution": {
                "tenant": args.tenant,
                "start_date": args.start_date,
                "end_date": args.end_date,
            }
        }
    )

    config = OmegaConf.merge(config, cli_config)

    print(OmegaConf.to_yaml(config))


if __name__ == "__main__":
    main()