from pathlib import Path

from omegaconf import DictConfig, OmegaConf

PROJECT_ROOT = Path(__file__).resolve().parents[2]
CONFIG_ROOT = PROJECT_ROOT / "config"


def load_config(env: str, tenant: str) -> DictConfig:
    """
    Load and merge base, environment and tenant configuration files.

    Precedence:
    base.yaml < env/<env>.yaml < tenants/<tenant>.yaml
    """

    base_path = CONFIG_ROOT / "base.yaml"
    env_path = CONFIG_ROOT / "env" / f"{env}.yaml"
    tenant_path = CONFIG_ROOT / "tenants" / f"{tenant}.yaml"

    if not base_path.exists():
        raise FileNotFoundError(f"Base config not found: {base_path}")

    if not env_path.exists():
        raise FileNotFoundError(f"Environment config not found: {env_path}")

    if not tenant_path.exists():
        raise FileNotFoundError(f"Tenant config not found: {tenant_path}")

    base_config = OmegaConf.load(base_path)
    env_config = OmegaConf.load(env_path)
    tenant_config = OmegaConf.load(tenant_path)

    return OmegaConf.merge(
        base_config,
        env_config,
        tenant_config,
    )