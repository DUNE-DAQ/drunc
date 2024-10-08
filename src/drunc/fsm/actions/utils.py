

def validate_run_type(run_type: str) -> str:
    """
    Validate the run type
    :param run_type: the run type
    :return: the validated run type
    """
    RUN_TYPES = ["PROD", "TEST"]
    if run_type not in RUN_TYPES:
        from drunc.exceptions import DruncException
        raise DruncException(f"Invalid run type: {run_type}. Must be one of {RUN_TYPES}")
    return run_type