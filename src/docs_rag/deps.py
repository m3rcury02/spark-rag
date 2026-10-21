"""pip requirements of the jobs that embed (D-014), in one place for scripts/run_job.py and the bundle.

The CPU build of the torch that Config.embedding.packages pins, per machine (serverless jobs have run on
aarch64), with the sha256 the index lists (https://download.pytorch.org/whl/cpu/torch/, read 2026-10-20).
From PyPI, pip took a CUDA build in M0 (torch 2.14.1+cu130, runs/m0_smoke_test.json): large, unused on CPU.
resources/pipeline.job.yml carries the same lines; tests/test_bundle.py checks that they match.
"""

from docs_rag.config import EmbeddingConfig

TORCH_INDEX = "https://download.pytorch.org/whl/cpu"
TORCH_CPU_WHEELS = {
    "x86_64": (
        "torch-2.14.1%2Bcpu-cp312-cp312-manylinux_2_28_x86_64.whl",
        "5a6363570c753812540a05eb82380e329469cbe668643e88111414c12627711f",
    ),
    "aarch64": (
        "torch-2.14.1%2Bcpu-cp312-cp312-manylinux_2_28_aarch64.whl",
        "2af649994b36dffc627175c3f306d9e513a309b58d4619ac7107577613336454",
    ),
}


def embedding_requirements(embedding: EmbeddingConfig) -> list[str]:
    """PEP 508 lines for the embedding packages: torch as a direct URL per machine, the rest pinned."""
    pins = dict(embedding.packages)
    torch = pins.pop("torch")
    lines = []
    for machine, (wheel, sha256) in TORCH_CPU_WHEELS.items():
        if not wheel.startswith(f"torch-{torch.replace('+', '%2B')}-"):
            raise ValueError(f"TORCH_CPU_WHEELS {wheel} is not the pinned torch {torch}")
        lines.append(f'torch @ {TORCH_INDEX}/{wheel}#sha256={sha256} ; platform_machine == "{machine}"')
    return lines + [f"{name}=={version}" for name, version in pins.items()]
