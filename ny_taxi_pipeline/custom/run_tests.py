from mage_ai.orchestration.triggers.api import trigger_pipeline

if 'custom' not in globals():
    from mage_ai.data_preparation.decorators import custom

@custom
def run_quality(*args, **kwargs):
    trigger_pipeline(
        'quality_checks',
        check_status=True,
        error_on_failure=True,
        verbose=True,
    )
    return "quality OK"