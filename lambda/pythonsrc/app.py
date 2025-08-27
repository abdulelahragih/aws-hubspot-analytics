import os
import logging

from functions.activities import activities_handler
from functions.deals import deals_handler
from functions.owners import owners_handler
from functions.companies import companies_handler
from functions.pipelines import pipelines_handler
from functions.contacts import contacts_handler

LOG = logging.getLogger()
LOG.setLevel(logging.INFO)


def handler(event, context):
    """Dispatcher entrypoint. Selects a task based on TASK env var.

    This allows multiple Lambda functions to reuse the same image
    and choose behavior via environment variable.
    """
    task = os.environ.get("TASK", None)
    task = task.strip().lower() if task else None
    if task == "activities":
        return activities_handler(event, context)
    if task == "deals":
        return deals_handler(event, context)
    if task == "owners":
        return owners_handler(event, context)
    if task == "companies":
        return companies_handler(event, context)
    if task == "contacts":
        return contacts_handler(event, context)
    if task == "pipelines":
        return pipelines_handler(event, context)
    raise RuntimeError("Unknown TASK '%s'" % task)
