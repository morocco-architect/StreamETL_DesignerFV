# Evaluation-ready additions

This package keeps the StreamETL **1.2.0-rc1 application/core code** and adds reproducibility helpers for Windows evaluation and SoftwareX evidence collection.

Added only for evaluation convenience:

- `evaluation/run_l2_evaluation.py`
- `evaluation/plot_evaluation.py`
- `evaluation/requirements-eval.txt`
- `evaluation/README.md`
- `scripts/windows/01_setup_and_l2_evaluate.bat`
- `scripts/windows/02_start_docker_stack.bat`
- `scripts/windows/03_run_backend.bat`
- `scripts/windows/04_run_frontend.bat`
- `docs/SCREENSHOT_AND_L3_CHECKLIST.md`

No locally measured timing/coverage results are shipped as publication evidence. Run the scripts on the target machine and retain the generated raw CSV/XML/logs together with machine/software metadata.

The original RC code has already been shown to contain the 21 Intelligence tests, 8 deterministic compiler/API tests, and 3 example tests described in the manuscript. The evaluation-ready derivative should still be validated on the target Windows environment before any result is cited.
