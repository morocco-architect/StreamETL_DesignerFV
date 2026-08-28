from __future__ import annotations

from fastapi import APIRouter, HTTPException, status
from pydantic import ValidationError

from .error_diagnosis import ErrorDiagnoser
from .pipeline_assistant import PipelineAssistant
from .providers import ProviderDisabledError, ProviderError
from .repair import PipelineRepairer, RepairError
from .schemas import (
    ErrorDiagnosisRequest,
    ErrorDiagnosisResponse,
    PipelineBuildRequest,
    PipelineBuildResponse,
    RepairRequest,
    RepairResponse,
    StreamETLDAG,
    ValidationReport,
)
from .semantic_advisor import SemanticAdvisor

# No prefix here on purpose. main.py owns the public API prefix:
# app.include_router(router, prefix="/api/intelligence", tags=["intelligence"])
router = APIRouter()


@router.post(
    "/generate",
    response_model=PipelineBuildResponse,
    summary="AI-assisted natural language to native StreamETL DAG",
)
def generate_pipeline(request: PipelineBuildRequest) -> PipelineBuildResponse:
    try:
        pipeline, validation, provider_info = PipelineAssistant.generate_dag_from_prompt(request.prompt)
        return PipelineBuildResponse(
            pipeline=pipeline,
            validation=validation,
            provider=provider_info.name,
            model=provider_info.model,
        )
    except ProviderDisabledError as exc:
        raise HTTPException(status_code=status.HTTP_503_SERVICE_UNAVAILABLE, detail=str(exc)) from exc
    except ProviderError as exc:
        raise HTTPException(status_code=status.HTTP_502_BAD_GATEWAY, detail=str(exc)) from exc
    except ValidationError as exc:
        raise HTTPException(status_code=status.HTTP_422_UNPROCESSABLE_ENTITY, detail=exc.errors()) from exc


@router.post(
    "/analyze",
    response_model=ValidationReport,
    summary="Deterministic semantic validation of a native StreamETL DAG",
)
def analyze_pipeline(pipeline: StreamETLDAG) -> ValidationReport:
    return SemanticAdvisor.analyze(pipeline)


@router.post(
    "/repair",
    response_model=RepairResponse,
    summary="Apply explicit deterministic repairs and revalidate the DAG",
)
def repair_pipeline(request: RepairRequest) -> RepairResponse:
    try:
        return PipelineRepairer.repair(request)
    except RepairError as exc:
        raise HTTPException(status_code=status.HTTP_422_UNPROCESSABLE_ENTITY, detail=str(exc)) from exc

    
@router.post(
    "/diagnose",
    response_model=ErrorDiagnosisResponse,
    summary="Rule-first Spark error diagnosis with optional AI fallback",
)
def diagnose_error(request: ErrorDiagnosisRequest) -> ErrorDiagnosisResponse:
    return ErrorDiagnosisResponse(diagnosis=ErrorDiagnoser.diagnose(request))
