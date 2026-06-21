from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[1]


def read_asset(relative_path: str) -> str:
    return (REPO_ROOT / relative_path).read_text(encoding="utf-8")


def test_compose_defines_a_health_checked_gravitino_service():
    compose = read_asset("docker-compose.yml")

    assert "gravitino:" in compose
    assert "apache/gravitino:1.2.0" in compose
    assert '"8090:8090"' in compose
    assert "GRAVITINO_USE_WEB_V2: \"true\"" in compose
    assert "healthcheck:" in compose
    assert "http://localhost:8090/api/version" in compose
    assert "gravitino_data:/root/gravitino/data" in compose
    assert "paimon_warehouse:/workspace/data/paimon" in compose


def test_gravitino_initializer_is_strict_and_idempotent():
    script = read_asset("scripts/init_gravitino.sh")

    assert "set -euo pipefail" in script
    assert 'METALAKE="${GRAVITINO_METALAKE:-ai_observability}"' in script
    assert 'get_status "${GRAVITINO_URL}/api/metalakes/${METALAKE}"' in script
    assert 'create_resource "metalake"' in script
    assert 'get_status "${GRAVITINO_URL}/api/metalakes/${METALAKE}/catalogs/paimon_lake"' in script
    assert 'create_resource "Paimon catalog"' in script
    assert "--connect-timeout 3 --max-time 10" in script
    assert '|| echo "Metalake already exists"' not in script


def test_makefile_exposes_gravitino_lifecycle_targets():
    makefile = read_asset("Makefile")

    assert "gravitino-up:" in makefile
    assert "docker compose up -d --wait gravitino" in makefile
    assert "scripts/init_gravitino.sh" in makefile
    assert "gravitino-status:" in makefile
    assert "gravitino-catalogs:" in makefile
