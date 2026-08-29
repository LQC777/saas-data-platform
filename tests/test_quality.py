from saas_pipeline.config import load_config


def test_load_dev_sv_config():
    config = load_config(
        env="dev",
        tenant="sv",
    )

    assert config.environment == "dev"
    assert config.tenant.code == "sv"

    assert config.paths.bronze == "data/bronze"
    assert config.paths.silver == "data/silver"
    assert config.paths.gold == "data/gold"


def test_quality_configuration():
    config = load_config(
        env="dev",
        tenant="sv",
    )

    assert config.quality.fail_on_critical is False
    assert config.execution.fail_fast is False